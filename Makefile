VERSION ?= 0.1
PROJECT_DIR=$(realpath $(PWD)/)

gomod:
	@echo "Prerun: Verify go modules..."
	go mod verify
	@echo "Prerun: Tidying any missing or currupted modules..."
	go mod tidy

build: gomod build-golang-test build-golang-test build-redis-test build-kafka-test build-nats-test build-nats-streaming-test

tests: gomod run-golang-test run-golang-test run-kafka-test run-nats-test run-nats-streaming-test run-redis-test

build-golang-test:
	docker build -t actorkit-test -f ./scripts/Dockerfile-actorkit-tests $(PROJECT_DIR)

build-google-test:
	docker build -t actorkit-google-test -f ./scripts/Dockerfile-google-pubsub-tests $(PROJECT_DIR)
	
build-redis-test:
	docker build -t actorkit-redis-test -f ./scripts/Dockerfile-redis-pubsub-tests $(PROJECT_DIR)
	
build-kafka-test:
	docker build -t actorkit-kafka-test -f ./scripts/Dockerfile-kafka-pubsub-tests $(PROJECT_DIR)
	
build-nats-test:
	docker build -t actorkit-nats-test -f ./scripts/Dockerfile-nats-pubsub-tests $(PROJECT_DIR)
	
build-nats-streaming-test:
	docker build -t actorkit-nats-streaming-test -f ./scripts/Dockerfile-nats-streaming-pubsub-tests $(PROJECT_DIR)

run-golang-test:
	docker run -it --rm  actorkit-test

run-google-test:
	docker run -it --rm  actorkit-google-test
	
run-redis-test:
	docker run -it --rm  -t actorkit-redis-test
	
run-kafka-test:
	docker run -it --rm  -t actorkit-kafka-test
	
run-nats-test:
	docker run -it --rm  -t actorkit-nats-test
	
run-nats-streaming-test:
	docker run -it --rm  -t actorkit-nats-streaming-test


# These are only to be runned within individual docker image and ci

base_tests: platform_tests retries_tests
	go test -cover .
	go test -v -race .

platform_tests:
	go test -cover ./platform/...
	go test -v -race ./platform/...

retries_tests:
	go test -v -cover ./retries/...
	go test -v -race ./retries/...

nats_tests:
	export PATH=/scripts:$PATH
	sh /scripts/nats-run.sh
	go test -v -cover ./transit/nats/...
	go test -v -race ./transit/nats/...

kafka_tests:
	export PATH=/scripts:$PATH
	sh /scripts/kafka-run.sh
	go test -v -cover ./transit/kafka/...
	go test -v -race ./transit/kafka/...

redis_tests:
	export PATH=/scripts:$PATH
	sh /scripts/redis-run.sh
	go test -v -cover ./transit/redis/...
	go test -v -race ./transit/redis/...

nats_streaming_tests:
	export PATH=/scripts:$PATH
	sh /scripts/natstreaming-run.sh
	go test -v -cover ./transit/natstreaming/...
	go test -v -race ./transit/natstreaming/...

google_pubsub_tests:
	export PATH=/scripts:$PATH
	sh /scripts/google-run.sh
	go test -v -cover ./transit/google/...
	go test -v -race ./transit/google/...
