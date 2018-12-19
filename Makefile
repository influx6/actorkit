VERSION ?= 0.1
PROJECT_DIR=$(realpath $(PWD)/)

gomod:
	@echo "Prerun: Verify go modules..."
	go mod verify
	@echo "Prerun: Tidying any missing or currupted modules..."
	go mod tidy

build: gomod build-golang-test build-golang-pubsub-test build-redis-pubsub-test build-kafka-pubsub-test build-nats-pubsub-test build-nats-streaming-pubsub-test

tests: gomod run-golang-test run-golang-pubsub-test run-kafka-pubsub-test run-nats-pubsub-test run-nats-streaming-pubsub-test run-redis-pubsub-test

build-golang-test:
	docker build -t actorkit-test -f ./scripts/Dockerfile-actorkit-tests $(PROJECT_DIR)

build-google-pubsub-test:
	docker build -t actorkit-google-pubsub-test -f ./scripts/Dockerfile-google-pubsub-tests $(PROJECT_DIR)
	
build-redis-pubsub-test:
	docker build -t actorkit-redis-pubsub-test -f ./scripts/Dockerfile-redis-pubsub-tests $(PROJECT_DIR)
	
build-kafka-pubsub-test:
	docker build -t actorkit-kafka-pubsub-test -f ./scripts/Dockerfile-kafka-pubsub-tests $(PROJECT_DIR)
	
build-nats-pubsub-test:
	docker build -t actorkit-nats-pubsub-test -f ./scripts/Dockerfile-nats-pubsub-tests $(PROJECT_DIR)
	
build-nats-streaming-pubsub-test:
	docker build -t actorkit-nats-streaming-pubsub-test -f ./scripts/Dockerfile-nats-streaming-pubsub-tests $(PROJECT_DIR)

run-golang-test: build-golang-test
	docker run -it --rm  actorkit-test

run-google-pubsub-test: build-google-pubsub-test
	docker run -it --rm  actorkit-google-pubsub-test
	
run-redis-pubsub-test: build-redis-pubsub-test
	docker run -it --rm  -t actorkit-redis-pubsub-test
	
run-kafka-pubsub-test: build-kafka-pubsub-test
	docker run -it --rm  -t actorkit-kafka-pubsub-test
	
run-nats-pubsub-test: build-nats-pubsub-test
	docker run -it --rm  -t actorkit-nats-pubsub-test
	
run-nats-streaming-pubsub-test: build-nats-streaming-pubsub-test
	docker run -it --rm  -t actorkit-nats-streaming-pubsub-test


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
	/script/nats-run.sh
	go test -v -cover ./transit/nats/...
	go test -v -race ./transit/nats/...

kafka_tests:
	/script/kafka-run.sh
	go test -v -cover ./transit/kafka/...
	go test -v -race ./transit/kafka/...

redis_tests:
	/script/redis-run.sh
	go test -v -cover ./transit/redis/...
	go test -v -race ./transit/redis/...

nats_streaming_tests:
	/script/natstreaming-run.sh
	go test -v -cover ./transit/natstreaming/...
	go test -v -race ./transit/natstreaming/...

google_pubsub_tests:
	/script/google-pubsub-run.sh
	go test -v -cover ./transit/google/...
	go test -v -race ./transit/google/...
