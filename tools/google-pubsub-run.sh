#!/usr/bin/env bash

export PUBSUB_IP=0.0.0.0
export PUBSUB_PORT=8538
export PUBSUB_EMULATOR_HOST="$(PUBSUB_IP):$(PUBSUB_PORT)"

nohup bash -c "gcloud beta emulators pubsub start --host=$PUBSUB_EMULATOR_HOST --data-dir=/data --log-http --verbosity=debug --user-output-enabled &";
sleep 5;