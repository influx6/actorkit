#!/usr/bin/env bash

echo "Prerun: Verify go modules..."
go mod verify
echo "Prerun: Tidying any missing or currupted modules..."
go mod tidy

sleep 5

echo "Run cover tests for retries"
go test -v -cover ./retries
go test -v -race ./retries

echo "Run cover tests for platform"
go test -v -cover ./platform
go test -v -race ./platform

echo "Run cover tests for actorkit"
go test -v -cover .
go test -v -race .
