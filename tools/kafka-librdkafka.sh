#!/bin/bash
set -e

readonly version="$1"
readonly build_dir=./bin/librdkafka

if [ -z "$version" ]; then
    echo "Usage: $0 <version>"
    exit 1
fi

mkdir -p ./bin/librdkafka/bin
wget -qO- "https://github.com/edenhill/librdkafka/archive/v${version}.tar.gz" | tar xz --strip-components 1 -C ./bin/librdkafka

cd ./bin/librdkafka
./configure --prefix=/usr
make -j "$(getconf _NPROCESSORS_ONLN)"
make install