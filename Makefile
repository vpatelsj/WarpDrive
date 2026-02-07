.PHONY: all build test clean fmt vet lint test-e2e demo bench

BINARY   := warpdrive-mount
BENCH    := warpdrive-bench
CTL      := warpdrive-ctl
BUILD_DIR := bin
GO       := go
GOFLAGS  := -trimpath
LDFLAGS  := -s -w

all: build

build:
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BUILD_DIR)/$(BINARY) ./cmd/warpdrive-mount
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BUILD_DIR)/$(BENCH) ./cmd/warpdrive-bench
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BUILD_DIR)/$(CTL) ./cmd/warpdrive-ctl

test:
	$(GO) test -race -count=1 ./...

test-v:
	$(GO) test -race -v -count=1 ./...

test-cover:
	$(GO) test -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

test-e2e:
	$(GO) test -race -v -count=1 -timeout 120s ./e2e/...

fmt:
	$(GO) fmt ./...
	gofumpt -w . 2>/dev/null || true

vet:
	$(GO) vet ./...

clean:
	rm -rf $(BUILD_DIR) coverage.out coverage.html

bench: build
	$(BUILD_DIR)/$(BENCH) --dir /tmp/warpdrive-demo-source --readers 4 --duration 10s --chunk 4194304

bench-go:
	$(GO) test -bench=. -benchmem -benchtime=1s ./pkg/cache/...

demo:
	@./scripts/demo-verify.sh

deps:
	$(GO) mod tidy
	$(GO) mod verify
