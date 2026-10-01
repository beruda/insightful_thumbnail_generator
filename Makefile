.PHONY: build test test-short race cover chaos lint demo

build:
	go build -o bin/thumbnailer ./cmd/thumbnailer

# Full suite, including real-ffmpeg smoke tests (skipped automatically if ffmpeg is missing).
test:
	go test ./...

# Fakes only: no ffmpeg, no subprocesses.
test-short:
	go test -short ./...

race:
	go test -race ./...

cover:
	go test -coverpkg=./internal/... -coverprofile=cover.out ./internal/... && go tool cover -func=cover.out | tail -1

# Reproduce a chaos failure: make chaos SEED=3
chaos:
	CHAOS_SEED=$(SEED) go test ./internal/app -run Chaos -v

lint:
	gofmt -l . && go vet ./...

# Run once over a directory: make demo ROOT=~/Movies
demo: build
	./bin/thumbnailer -root $(ROOT) -data-dir ./data -once
