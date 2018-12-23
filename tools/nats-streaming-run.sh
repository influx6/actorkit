#!/usr/bin/env bash

nohup bash -c "./bin/nats-streaming/nats-streaming-server -m 8222 &"
sleep 5
