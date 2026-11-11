SHELL := /bin/bash

.PHONY: all build api worker test vet fmt tidy run-api run-worker docker-build clean

all: fmt vet test build

build: api worker

api:
	go build -o bin/api ./cmd/api

worker:
	go build -o bin/worker ./cmd/worker

test:
	go test -race -cover ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

tidy:
	go mod tidy

run-api:
	API_ADDR=:8080 go run ./cmd/api

run-worker:
	go run ./cmd/worker

docker-build:
	docker build -f Dockerfile.api -t idp-api:local .
	docker build -f Dockerfile.worker -t idp-worker:local .

clean:
	rm -rf bin
