FROM influx6/gcloud-golang-1.11.3-pubsub-emulator-debian-base:0.1

ENV GO111MODULE=on
COPY . gokit/actorkit
WORKDIR gokit/actorkit
RUN make gomod

ENTRYPOINT ["make", "google_pubsub_tests"]
