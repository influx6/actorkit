#!/usr/bin/env bash

export PUBSUB_IP=0.0.0.0
export PUBSUB_PORT=8538
export PUBSUB_EMULATOR_HOST="$(PUBSUB_IP):$(PUBSUB_PORT)"

cmd = "bash -c 'gcloud beta emulators pubsub start --host=$(PUBSUB_EMULATOR_HOST) --data-dir=/data --log-http --verbosity=debug --user-output-enabled'";
docker run influx6/google-pubsub-base:0.1 -it --rm $(cmd)
