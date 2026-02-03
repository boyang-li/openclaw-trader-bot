# ACC-L1 Ingestion Makefile
# ============================================================================

# Variables
GO := go
GOFLAGS := -mod=readonly
BINARY_DIR := bin
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

# Docker
DOCKER_COMPOSE := docker compose -f deploy/docker-compose.yml
DOCKER_REGISTRY ?= ghcr.io/openclaworg

# Binaries
BINARIES := gdelt fred binance whalealert cot tradingeconomics telegram orchestrator

.PHONY: all build clean test lint fmt help
.PHONY: build-gdelt build-fred build-binance build-whalealert build-cot build-tradingeconomics build-telegram build-orchestrator
.PHONY: run-gdelt run-fred run-binance run-whalealert run-cot run-tradingeconomics run-telegram run-orchestrator
.PHONY: docker-build docker-up docker-down docker-logs
.PHONY: test-coverage test-integration
.PHONY: deps tidy verify

# ============================================================================
# Default target
# ============================================================================

all: build

# ============================================================================
# Build targets
# ============================================================================

## build: Build all binaries
build: $(addprefix build-,$(BINARIES))

## build-gdelt: Build GDELT ingestor
build-gdelt:
	@echo "Building gdelt ingestor..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/gdelt ./cmd/gdelt

## build-fred: Build FRED ingestor
build-fred:
	@echo "Building fred ingestor..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/fred ./cmd/fred

## build-binance: Build Binance ingestor
build-binance:
	@echo "Building binance ingestor..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/binance ./cmd/binance

## build-whalealert: Build Whale Alert ingestor
build-whalealert:
	@echo "Building whalealert ingestor..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/whalealert ./cmd/whalealert

## build-cot: Build CME COT ingestor
build-cot:
	@echo "Building cot ingestor..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/cot ./cmd/cot

## build-tradingeconomics: Build Trading Economics ingestor
build-tradingeconomics:
	@echo "Building tradingeconomics ingestor..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/tradingeconomics ./cmd/tradingeconomics

## build-telegram: Build Telegram ingestor
build-telegram:
	@echo "Building telegram ingestor..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/telegram ./cmd/telegram

## build-orchestrator: Build orchestrator (manages all providers)
build-orchestrator:
	@echo "Building orchestrator..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_DIR)/orchestrator ./cmd/orchestrator

# ============================================================================
# Run targets
# ============================================================================

## run-gdelt: Run GDELT ingestor locally
run-gdelt:
	$(GO) run $(GOFLAGS) ./cmd/gdelt

## run-fred: Run FRED ingestor locally
run-fred:
	$(GO) run $(GOFLAGS) ./cmd/fred

## run-binance: Run Binance ingestor locally
run-binance:
	$(GO) run $(GOFLAGS) ./cmd/binance

## run-whalealert: Run Whale Alert ingestor locally
run-whalealert:
	$(GO) run $(GOFLAGS) ./cmd/whalealert

## run-cot: Run CME COT ingestor locally
run-cot:
	$(GO) run $(GOFLAGS) ./cmd/cot

## run-tradingeconomics: Run Trading Economics ingestor locally
run-tradingeconomics:
	$(GO) run $(GOFLAGS) ./cmd/tradingeconomics

## run-telegram: Run Telegram ingestor locally
run-telegram:
	$(GO) run $(GOFLAGS) ./cmd/telegram

## run-orchestrator: Run orchestrator locally (all providers)
run-orchestrator:
	$(GO) run $(GOFLAGS) ./cmd/orchestrator

# ============================================================================
# Test targets
# ============================================================================

## test: Run all tests
test:
	$(GO) test $(GOFLAGS) -race -v ./...

## test-coverage: Run tests with coverage report
test-coverage:
	@mkdir -p coverage
	$(GO) test $(GOFLAGS) -race -coverprofile=coverage/coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage/coverage.out -o coverage/coverage.html
	@echo "Coverage report: coverage/coverage.html"

## test-integration: Run integration tests (requires Docker)
test-integration:
	$(GO) test $(GOFLAGS) -race -tags=integration -v ./...

# ============================================================================
# Code quality targets
# ============================================================================

## lint: Run golangci-lint
lint:
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...

## fmt: Format code
fmt:
	$(GO) fmt ./...
	@which goimports > /dev/null || (echo "Installing goimports..." && go install golang.org/x/tools/cmd/goimports@latest)
	goimports -w .

## verify: Verify code (fmt + lint + test)
verify: fmt lint test

# ============================================================================
# Dependency targets
# ============================================================================

## deps: Download dependencies
deps:
	$(GO) mod download

## tidy: Tidy and verify dependencies
tidy:
	$(GO) mod tidy
	$(GO) mod verify

# ============================================================================
# Docker targets
# ============================================================================

## docker-build: Build all Docker images
docker-build:
	@echo "Building Docker images..."
	docker build -f deploy/docker/Dockerfile.gdelt -t $(DOCKER_REGISTRY)/l1-gdelt:$(VERSION) .
	docker build -f deploy/docker/Dockerfile.fred -t $(DOCKER_REGISTRY)/l1-fred:$(VERSION) .
	docker build -f deploy/docker/Dockerfile.binance -t $(DOCKER_REGISTRY)/l1-binance:$(VERSION) .
	docker build -f deploy/docker/Dockerfile.slm -t $(DOCKER_REGISTRY)/l1-slm:$(VERSION) .

## docker-up: Start Docker Compose stack
docker-up:
	$(DOCKER_COMPOSE) up -d

## docker-down: Stop Docker Compose stack
docker-down:
	$(DOCKER_COMPOSE) down

## docker-logs: Tail Docker Compose logs
docker-logs:
	$(DOCKER_COMPOSE) logs -f

## docker-ps: Show running containers
docker-ps:
	$(DOCKER_COMPOSE) ps

## docker-clean: Remove all containers, volumes, and images
docker-clean:
	$(DOCKER_COMPOSE) down -v --rmi all

# ============================================================================
# Utility targets
# ============================================================================

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BINARY_DIR)
	@rm -rf coverage
	@rm -rf tmp

## help: Show this help message
help:
	@echo "ACC-L1 Ingestion - Available targets:"
	@echo ""
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'
