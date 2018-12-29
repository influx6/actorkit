#!/usr/bin/env bash

export ZOOKEEPER_PEERS='localhost:2181'
export KAFKA_PEERS='localhost:9092'

docker run --rm -it -p 9092:9092 influx6/kafka-rdkafka-alpine-base:0.1 bash -c "cd /kafka && bash -c \"./bin/zookeeper-server-start.sh ./config/zookeeper.properties &\" && \
 ./bin/kafka-server-start.sh ./config/server.properties"
