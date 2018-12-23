#!/usr/bin/env bash

mkdir -p ./bin/gnatsd
wget https://github.com/nats-io/gnatsd/releases/download/v1.3.0/gnatsd-v1.3.0-linux-amd64.zip -O gnatsd.zip
unzip gnatsd.zip -d ./bin/gnatsd/
mv ./bin/gnatsd/gnatsd-v1.3.0-linux-amd64/* ./bin/gnatsd

rm -rf ./bin/gnatsd/gnatsd-v1.3.0-linux-amd64
rm -rf gnatsd.zip
chmod -R +x ./bin/gnatsd/gnatsd
