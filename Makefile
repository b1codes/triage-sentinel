BINARY      := sentinel
PKG         := github.com/b1codes/triage-sentinel
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS     := -s -w -X $(PKG)/internal/version.Version=$(VERSION)
DATA_DIR    ?= ./var

.PHONY: all build web test lint vet fmt fmt-check check dev run migrate replay clean install-service uninstall-service service-status logs

all: check build

## build: compile a static binary into bin/
build: web
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o bin/$(BINARY) ./cmd/sentinel

## web: build the React SPA into internal/webassets/dist
web:
	cd web && npm ci
	cd web && npm run build
	touch internal/webassets/dist/.gitkeep

## test: run all Go tests with the race detector
test:
	go test -race ./...

## fmt: format all Go source
fmt:
	gofmt -w .

## fmt-check: fail if any file is unformatted
fmt-check:
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then echo "unformatted files:"; echo "$$out"; exit 1; fi

## vet: run go vet
vet:
	go vet ./...

## lint: alias for the full static gate
lint: fmt-check vet

## check: the gate every commit must pass
check: fmt-check vet test

## dev: run with the Vite dev server proxied instead of embedded assets
dev:
	go run ./cmd/sentinel -dev serve

## run: run the compiled binary
run: build
	./bin/$(BINARY) serve

## migrate: apply pending database migrations and exit
migrate:
	go run ./cmd/sentinel migrate

## replay: feed a recorded payload through the ingest pipeline
replay:
	go run ./cmd/sentinel replay $(FILE)

clean:
	rm -rf bin internal/webassets/dist
	mkdir -p internal/webassets/dist
	touch internal/webassets/dist/.gitkeep

## install-service: install and start the LaunchAgent
install-service: build
	./deploy/launchd/install.sh

## uninstall-service: stop and remove the LaunchAgent
uninstall-service:
	./deploy/launchd/uninstall.sh

## service-status: show launchd's view of the service
service-status:
	@launchctl print gui/$$(id -u)/com.b1codes.sentinel | sed -n '1,20p'

## logs: follow the service logs
logs:
	tail -f $(DATA_DIR)/log/sentinel.out.log $(DATA_DIR)/log/sentinel.err.log
