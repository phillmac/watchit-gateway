import asyncio
from os import environ

from src.core.http.QueueItem import QueueItem
from src.core import logger

from aiohttp import web
from aiohttp.web_response import json_response

class SleepMgr():
    def __init__(self, app, delay=3600):
        self.__sleep = None
        self.__delay = delay
        self.__shutdown = app['shutdown']

    async def sleep(self):
        self.__sleep = asyncio.create_task(asyncio.sleep(self.__delay))

        try:
            await self.__sleep
        except asyncio.CancelledError: # Catch external shutdown signal
            logger.notice('CancelledError')
            if not self.__shutdown.is_set():
                logger.notice('Set shutdown')
                self.__shutdown.set()
        except Exception as ex:
            logger.warning(ex) #Something unexpected

    def cancel_sleep(self):
        if self.__sleep:
            self.__sleep.cancel()

class WaitingCount():
    def __init__(self):
        self.__value = 0

    def __enter__(self):
        self.__value += 1

    def __exit__(self, type, value, tb):
        self.__value -= 1

    @property
    def value(self):
        return self.__value


async def get_item(request):
    app = request.app
    queue = app['queue']
    waiting_count = app['waiting_count']
    with waiting_count:
        try:
            item = await asyncio.wait_for(queue.get(), 30)
            queue.task_done()
            params = item.params
            logger.notice(f"Dequed item {params}")
            logger.info('Finished get_item request')
            return json_response(params)
        except asyncio.TimeoutError:
            logger.info('Timout waiting to dequeue work item')
            return json_response({'command': None})


async def add_item(request):
    params = await request.json()
    queue = request.app['queue']
    item = QueueItem(**params)

    logger.notice(f"Adding {params} to queue")
    await queue.put(item)
    logger.info('Finished adding item')

    return json_response('ok')


def shutdown_app(request):
    request.app['shutdown'].set()
    request.app['sleepmgr'].cancel_sleep()
    return json_response('ok')


async def start_background_tasks(app):
    pass


async def cleanup_background_tasks(app):
    for _name, task in app['tasks'].items():
        task.cancel()

    app['tasks'].clear()


async def cleanup_caches(app):
    pass

async def run_app():

    queue = asyncio.PriorityQueue()
    waiting_count = WaitingCount()

    app = web.Application()

    app['tasks'] = dict()
    app['queue'] = queue
    app['shutdown'] = asyncio.Event()
    app['sleepmgr'] = SleepMgr(app)
    app['waiting_count'] = waiting_count

    app.router.add_get('/ping', lambda request: json_response('pong'))
    app.router.add_get('/item', get_item)
    app.router.add_post('/item', add_item)
    app.router.add_get(
        '/count', lambda request: json_response({'count': queue.qsize()}))
    app.router.add_get(
        '/waiting', lambda request: json_response({'waiting': waiting_count.value}))
    #app.router.add_post('/shutdown', shutdown_app) #DON'T ENABLE IN PRODUCTION MODE

    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    app.on_cleanup.append(cleanup_caches)


    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port=environ.get(
        'LISTEN_PORT', 3005))
    await site.start()

    names = sorted(str(s.name) for s in runner.sites)
    print(
        "======== Running on {} ========\n"
        "(Press CTRL+C to quit)".format(", ".join(names))
    )

    while not app['shutdown'].is_set():
        await app['sleepmgr'].sleep()

    logger.info('Shutting down')

    await runner.cleanup()


if __name__ == '__main__':
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        print('KeyboardInterrupt')

