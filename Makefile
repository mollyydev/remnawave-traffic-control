.PHONY: build run fmt test vet check

build:
	go build ./cmd/whitelists

run:
	go run ./cmd/whitelists

fmt:
	gofmt -w cmd internal

test:
	go test ./...

vet:
	go vet ./...

check: fmt vet test
