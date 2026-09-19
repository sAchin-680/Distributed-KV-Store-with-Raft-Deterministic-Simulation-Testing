SHELL := /bin/bash
.DEFAULT_GOAL := help

MODULE      := github.com/sAchin-680/raftkv
BIN         := bin
GO          ?= go
PROTOC      ?= protoc
GOLANGCILINT ?= $(shell $(GO) env GOPATH)/bin/golangci-lint

VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILDDATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS   := -s -w \
	-X $(MODULE)/internal/buildinfo.Version=$(VERSION) \
	-X $(MODULE)/internal/buildinfo.Commit=$(COMMIT) \
	-X $(MODULE)/internal/buildinfo.Date=$(BUILDDATE)

# Discovered rather than hardcoded, so a command builds the moment its package
# exists and `make build` never fails on one that does not yet.
CMDS := $(notdir $(patsubst %/,%,$(sort $(dir $(wildcard cmd/*/*.go)))))

# Code generation is only reproducible if the toolchain is pinned. Both plugins
# stamp their own version into every generated file, so an unpinned `@latest`
# makes proto-check fail the moment upstream tags a release — a failure that has
# nothing to do with this repository. CI installs these same versions.
PROTOC_VERSION          := 32.1
PROTOC_GEN_GO_VERSION   := v1.36.11
PROTOC_GEN_GRPC_VERSION := v1.6.2
GOLANGCILINT_VERSION    := v2.5.0

## ---------------------------------------------------------------------------
## Build
## ---------------------------------------------------------------------------

.PHONY: build
build: $(addprefix $(BIN)/,$(CMDS)) ## Build all binaries into ./bin
	@if [ -z "$(CMDS)" ]; then \
		echo "no command packages yet"; \
	else \
		echo "built: $(CMDS)"; \
	fi

$(BIN)/%:
	@mkdir -p $(BIN)
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $@ ./cmd/$*

.PHONY: clean
clean: ## Remove build and test artifacts
	rm -rf $(BIN) coverage.out coverage.html sim-failures

## ---------------------------------------------------------------------------
## Code generation
## ---------------------------------------------------------------------------

PROTO_FILES := $(wildcard proto/*.proto)

.PHONY: proto
proto: ## Regenerate protobuf and gRPC code
	@command -v $(PROTOC) >/dev/null || { echo "protoc not found; run 'make tools' and install protobuf"; exit 1; }
	@rm -rf proto/raftpb proto/kvpb
	$(PROTOC) -I proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES)
	@echo "generated: proto/raftpb proto/kvpb"

# Regenerates into a scratch directory and compares, rather than diffing against
# git. Checking git state would conflate "the generated code is stale" with "the
# working tree is dirty", and would report nothing at all before the generated
# code is first committed.
.PHONY: proto-check
proto-check: ## Fail if generated code is out of date with the .proto files
	@command -v $(PROTOC) >/dev/null || { echo "protoc not found; run 'make tools'"; exit 1; }
	@tmp=$$(mktemp -d) && trap 'rm -rf "$$tmp"' EXIT && \
	$(PROTOC) -I proto \
		--go_out=$$tmp --go_opt=module=$(MODULE) \
		--go-grpc_out=$$tmp --go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES) && \
	if diff -r -u "$$tmp/proto" proto --exclude='*.proto'; then \
		echo "generated protobuf code is up to date"; \
	else \
		echo "ERROR: generated code does not match proto/*.proto; run 'make proto' and commit"; \
		exit 1; \
	fi

.PHONY: tidy
tidy: ## Sync go.mod/go.sum
	$(GO) mod tidy

## ---------------------------------------------------------------------------
## Test
## ---------------------------------------------------------------------------

.PHONY: test
test: ## Run unit tests with the race detector
	$(GO) test -race -count=1 -timeout=5m ./...

.PHONY: test-short
test-short: ## Run only fast tests (skips long simulation suites)
	$(GO) test -short -count=1 -timeout=2m ./...

.PHONY: cover
cover: ## Run tests and open an HTML coverage report
	$(GO) test -race -count=1 -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

# Seed count for the CI correctness gate. Kept low enough to keep CI honest about
# its own runtime; the nightly job runs FUZZ_SEEDS=10000.
FUZZ_SEEDS ?= 1000

.PHONY: fuzz
fuzz: require-simctl ## Run the deterministic simulation fuzz suite (FUZZ_SEEDS=N)
	@$(MAKE) --no-print-directory $(BIN)/simctl
	$(BIN)/simctl fuzz --count=$(FUZZ_SEEDS)

.PHONY: replay
replay: require-simctl ## Replay one simulation seed (SEED=N)
	@[ -n "$(SEED)" ] || { echo "usage: make replay SEED=12345"; exit 2; }
	@$(MAKE) --no-print-directory $(BIN)/simctl
	$(BIN)/simctl run --seed=$(SEED) --verbose

.PHONY: require-simctl
require-simctl:
	@[ -n "$(wildcard cmd/simctl/*.go)" ] || \
		{ echo "the simulator has not been built yet"; exit 2; }

## ---------------------------------------------------------------------------
## Static analysis
## ---------------------------------------------------------------------------

.PHONY: fmt
fmt: ## Format all Go code
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not gofmt'd
	@unformatted=$$(gofmt -l . | grep -v '^proto/' || true); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt'd:"; echo "$$unformatted" | sed 's/^/  /'; \
		echo "run 'make fmt'"; exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

# Warns rather than fails when the linter is missing, so a fresh clone can run
# `make check` without installing anything. CI has no such escape hatch: it runs
# golangci-lint through its own action, where absence is not a possibility.
.PHONY: lint
lint: ## Run golangci-lint
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	elif [ -x "$(GOLANGCILINT)" ]; then \
		"$(GOLANGCILINT)" run; \
	else \
		echo "warning: golangci-lint not installed; skipping (run 'make tools')"; \
	fi

.PHONY: tools
tools: ## Install the pinned development tools (see the versions at the top)
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCILINT_VERSION)
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GRPC_VERSION)
	@echo "installed into $$($(GO) env GOPATH)/bin"

.PHONY: tools-check
tools-check: ## Report whether the installed codegen tools match the pinned versions
	@ok=0; \
	got=$$(protoc-gen-go --version 2>&1 | awk '{print $$NF}'); \
	[ "$$got" = "$(PROTOC_GEN_GO_VERSION)" ] || { echo "protoc-gen-go: $$got, want $(PROTOC_GEN_GO_VERSION)"; ok=1; }; \
	got=$$(protoc-gen-go-grpc --version 2>&1 | awk '{print $$NF}'); \
	[ "v$$got" = "$(PROTOC_GEN_GRPC_VERSION)" ] || { echo "protoc-gen-go-grpc: v$$got, want $(PROTOC_GEN_GRPC_VERSION)"; ok=1; }; \
	[ $$ok -eq 0 ] && echo "codegen tools match the pinned versions" || { echo "run 'make tools'"; exit 1; }

.PHONY: determinism
determinism: ## Verify the consensus core contains no sources of nondeterminism
	./scripts/check-determinism.sh

.PHONY: check
check: fmt-check vet determinism lint test ## Everything CI runs on a pull request
	@echo "all checks passed"

## ---------------------------------------------------------------------------
## Container
## ---------------------------------------------------------------------------

IMAGE ?= raftkv
TAG   ?= $(COMMIT)

.PHONY: docker
docker: ## Build the production container image
	@[ -f Dockerfile ] || { echo "no Dockerfile yet"; exit 2; }
	docker build -t $(IMAGE):$(TAG) -t $(IMAGE):latest .

## ---------------------------------------------------------------------------

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
