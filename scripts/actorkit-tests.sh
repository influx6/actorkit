#!/usr/bin/env bash

env CGO_ENABLED=0

echo "Prerun: Verify go modules..."
go mod verify
echo "Prerun: Tidying any missing or currupted modules..."
go mod tidy

sleep 5

echo "Run cover tests for retries"
go test -v -cover ./retries
go test -v ./retries

echo "Run cover tests for platform"
go test -v -cover ./platform
go test -v ./platform

echo "Run cover tests for actorkit"
go test -v -cover .
go test -v .
