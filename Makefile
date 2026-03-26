SHELL := /bin/bash

.PHONY: all api worker run-api run-worker tidy

all: tidy

tidy:
	go mod tidy

api:
	go build -o bin/api ./cmd/api

worker:
	go build -o bin/worker ./cmd/worker

run-api:
	API_ADDR=:8080 go run ./cmd/api

run-worker:
	go run ./cmd/worker

