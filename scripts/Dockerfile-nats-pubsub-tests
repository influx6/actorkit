FROM influx6/nats-golang-1.11.3-debian-base:0.1

ENV GO111MODULE=on
COPY . gokit/actorkit
WORKDIR gokit/actorkit
RUN make gomod
CMD "make nats_tests"

ENTRYPOINT ["make", "nats_tests"]
