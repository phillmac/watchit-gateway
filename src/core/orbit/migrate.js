process.env.FORCE_COLOR = 1
const argv = require('minimist')(process.argv.slice(2));

const MAX_CHUNKS = 1000
const ORBIT_DB_NAME = 'wt.movies.db'
const MIGRATE_FROM_DB = argv.tmpdb || 'ipfs';

const IPFS_NODE = argv.node || 'ipfs'
const MONGO_HOST = argv.hdb || 'mongodb' // Temporary helper db

const SOURCE = argv.source || 'FULL' // Source to migrate from
const RECREATE = argv.r || true // Recreate database
const KEY = argv.key || 'watchit' // Local key used to IPNS publish
const REGEN = argv.g || false

const chalk = require('chalk')
const IpfsApi = require('ipfs-http-client');
const OrbitDB = require('orbit-db');
const {consume} = require('streaming-iterables')
const MongoClient = require('mongodb').MongoClient;
const ipfs = IpfsApi({host: IPFS_NODE, port: '5001', protocol: 'http'});
const msgpack = require("msgpack-lite");
const {v4: uuidv4} = require('uuid');
const logger = require('pino')({prettyPrint: true});
const logs = {
    success: (msg) => logger.info(chalk.green(msg)),
    info: (msg) => logger.info(chalk.cyan(msg)),
    warn: (msg) => logger.warn(chalk.yellow(msg)),
    err: (msg) => logger.err(chalk.red(msg)),
};

// List of default keys
(async () => {

        // FUNCTIONS
        const chunkGen = (_movies, l) => {
            // Split array in chunks
            return new Array(Math.ceil(_movies.length / l)).fill(0)
                .map((_, n) => _movies.slice(n * l, n * l + l));
        }, ifNotExistCreateKey = async (key) => {
            // Check if current used key exists
            const currentList = await ipfs.key.list()
            const existingKey = currentList.some((k) => Object.is(k.name, key))
            if (!existingKey) return await ipfs.key.gen(key)
            return false;
        }

        // Create OrbitDB instance
        const DB_NAME = MIGRATE_FROM_DB;
        const DB_OPTIONS = {overwrite: RECREATE, localOnly: false, replicate: true}
        const orbitdb = await OrbitDB.createInstance(ipfs, {
            directory: REGEN ? `./orbit${uuidv4()}` : './orbit'
        });

        // DB init
        const db = await orbitdb.log(ORBIT_DB_NAME, DB_OPTIONS);
        db.events.on('peer', (p) => logs.warn(`Peer Db: ${p}`));
        // END DB

        const definedType = SOURCE;
        const isMixedDB = Object.is(definedType, 'FULL')
        logs.info(`Starting ${definedType} db `);
        const dbAddress = db.address.toString()
        const dbAddressHash = dbAddress.split('/')[2]

        // Check if existing keys else create it
        if (await ifNotExistCreateKey(KEY))
            logs.warn(`"${KEY}" key created`)

        // Add provider to allow nodes connect to it
        await consume(ipfs.dht.provide(dbAddressHash))
        const ipns = await ipfs.name.publish(dbAddressHash, {key: KEY})

        // Start movies migration to orbit from mongo
        let index = 0; // Keep cursor for movies unique id
        const url = `mongodb://${MONGO_HOST}`;
        const client = new MongoClient(url, {
            useUnifiedTopology: true,
            keepAlive: true
        });

        try {
            logs.warn('Connecting to helper db..');
            await client.connect(async (err) => {

                // Generate cursor for all movies
                const tmp_db = client.db(DB_NAME)
                const cursor = tmp_db.collection('movies').find(
                    {...!isMixedDB && {group_name: definedType}}
                ).limit(0).sort({year: 1})

                // Using rawData.length in place or .count() approach because of unexpected behavior
                // On a sharded cluster, db.collection.count() without a query predicate can result in an inaccurate
                // count if orphaned documents exist or if a chunk migration is in progress.
                const rawData = await cursor.toArray()
                const size = rawData.length
                const data = chunkGen(rawData, MAX_CHUNKS);
                logs.warn(`Migrating ${size} movies..`)

                for (const chunk of data) {
                    // let before = +new Date();
                    let ch = chunk.map((v) => {
                        index++;
                        v['_id'] = `wt_loc_${index}`;
                        v['total'] = size;
                        return v
                    });

                    //Add movie
                    const {cid} = await ipfs.add(
                        msgpack.encode(ch),
                        {pin: true}
                    );

                    await db.add(cid.toString());
                    logs.info(`Processed: ${index}/${size}`);
                }

                logs.success(`CID for ${definedType}: ${dbAddressHash}`)
                logs.success(`IPNS for ${definedType}: ${ipns.name}`)
                await client.close();
                logs.warn('Closed db..\n');
            })


        } catch (err) {
            logs.error(err);
        }

    }
)()
