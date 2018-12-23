#!/usr/bin/env bash

export ZOOKEEPER_PEERS='localhost:2181'
export KAFKA_PEERS='localhost:9092'

nohup bash -c "cd ./bin/kafka && bin/zookeeper-server-start.sh config/zookeeper.properties &"
nohup bash -c "cd ./bin/kafka && bin/kafka-server-start.sh config/server.properties &"
sleep 5
