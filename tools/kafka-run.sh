#!/usr/bin/env bash

export ZOOKEEPER_PEERS='localhost:2181'
export KAFKA_PEERS='localhost:9092'

bash -c "cd /kafka && bash -c \"./bin/zookeeper-server-start.sh ./config/zookeeper.properties &\" && \
 ./bin/kafka-server-start.sh ./config/server.properties"
