#!/bin/sh
# shellcheck disable=SC2034
peers=`cat /peering/peers.json`
[ ! -e "/ipfsdata/config" ] && ipfs init
ipfs config Addresses.Swarm '["/ip4/0.0.0.0/tcp/4001", "/ip4/0.0.0.0/tcp/4002/ws", "/ip6/::/tcp/4001"]' --json
ipfs config Addresses.API '/ip4/0.0.0.0/tcp/5001'
ipfs config Addresses.Gateway '/ip4/0.0.0.0/tcp/8080'
# shellcheck disable=SC2016
ipfs config Peering.Peers "$peers" --json
ipfs config --bool Swarm.EnableRelayHop false
ipfs config --bool Swarm.EnableAutoRelay true
ipfs config --bool Swarm.DisableBandwidthMetrics true
ipfs config Swarm.ConnMgr.HighWater 160 --json
ipfs config Swarm.ConnMgr.LowWater 80 --json
ipfs daemon --migrate=true --enable-gc --enable-namesys-pubsub --enable-pubsub-experiment --routing=dhtclient

