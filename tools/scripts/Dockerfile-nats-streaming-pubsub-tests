FROM influx6/nats-streaming-golang-1.11.3-debian-base:0.1

ENV GO111MODULE=on
COPY . gokit/actorkit
WORKDIR gokit/actorkit
RUN make gomod

ENTRYPOINT ["make", "nats_streaming_tests"]