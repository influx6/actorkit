#!/usr/bin/env bash

export ZOOKEEPER_PEERS='localhost:2181'
export KAFKA_PEERS='localhost:9092'

wget http://www.us.apache.org/dist/kafka/0.10.2.2/kafka_2.10-0.10.2.2.tgz  -O kafka.tgz
mkdir -p kafka && tar xzf kafka.tgz -C kafka --strip-components 1
nohup bash -c "cd kafka && bin/zookeeper-server-start.sh config/zookeeper.properties &"
nohup bash -c "cd kafka && bin/kafka-server-start.sh config/server.properties &"
sleep 5