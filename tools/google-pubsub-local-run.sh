#!/usr/bin/env bash

env PUBSUB_EMULATOR_HOST=localhost:8738 PUBSUB_PROJECT_ID="actorkit" \
gcloud beta emulators pubsub start --host-port=localhost:8738 --project="actorkit" --data-dir=./data --log-http --verbosity=debug --user-output-enabled
