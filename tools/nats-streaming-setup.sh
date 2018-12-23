#!/usr/bin/env bash

mkdir -p ./bin/nats-streaming
wget https://github.com/nats-io/nats-streaming-server/releases/download/v0.11.2/nats-streaming-server-v0.11.2-linux-amd64.zip -O nats-streaming.zip
unzip nats-streaming.zip -d ./bin/nats-streaming/
mv ./bin/nats-streaming/nats-streaming-server-v0.11.2-linux-amd64/* ./bin/nats-streaming

rm -rf nats-streaming-server-v0.11.2-linux-amd64
rm -rf nats-streaming.zip

chmod -R +x ./bin/nats-streaming

