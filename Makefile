VERSION ?= 0.1
PROJECT_DIR=$(realpath $(PWD)/)

gomod:
	@echo "Prerun: Verify go modules..."
	go mod verify
	@echo "Prerun: Tidying any missing or currupted modules..."
	go mod tidy

build: gomod build-golang-test build-golang-pubsub-test build-redis-pubsub-test build-kafka-pubsub-test build-nats-pubsub-test build-nats-streaming-pubsub-test

tests: gomod run-golang-test run-golang-pubsub-test run-kafka-pubsub-test run-nats-pubsub-test run-nats-streaming-pubsub-test run-redis-pubsub-test

build-golang-test: gomod
	docker build -t actorkit-test -f ./scripts/Dockerfile-actorkit-tests $(PROJECT_DIR)

build-google-pubsub-test: gomod
	docker build -t actorkit-google-pubsub-test -f ./scripts/Dockerfile-google-pubsub-tests $(PROJECT_DIR)
	
build-redis-pubsub-test: gomod
	docker build -t actorkit-redis-pubsub-test -f ./scripts/Dockerfile-redis-pubsub-tests $(PROJECT_DIR)
	
build-kafka-pubsub-test: gomod
	docker build -t actorkit-kafka-pubsub-test -f ./scripts/Dockerfile-kafka-pubsub-tests $(PROJECT_DIR)
	
build-nats-pubsub-test: gomod
	docker build -t actorkit-nats-pubsub-test -f ./scripts/Dockerfile-nats-pubsub-tests $(PROJECT_DIR)
	
build-nats-streaming-pubsub-test: gomod
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
