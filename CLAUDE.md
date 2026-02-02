# CLAUDE.md - L1 Ingestion Module

> **Purpose**: Long-term memory for AI assistants working on this codebase.
> **Last Updated**: 2025-02-02
> **Status**: Wave 3 Complete (All 7 Providers)

---

## Project Overview

**L1-Ingestion** is a Go-based real-time signal ingestion layer for the ACC (Autonomous Cognitive Core) system. It collects geopolitical, macroeconomic, and crypto signals from multiple data sources and publishes them to Kafka for downstream processing.

### Tech Stack
| Component | Technology |
|-----------|------------|
| Language | Go 1.21+ |
| Message Broker | Apache Kafka (segmentio/kafka-go) |
| Logging | Uber Zap |
| Config | Viper (env vars with `L1_` prefix) |
| HTTP | net/http (stdlib) |
| UUID | google/uuid |
| Metrics | Prometheus (promhttp) |

### Data Sources (7 Providers)
| Provider | Category | Port | Status |
|----------|----------|------|--------|
| GDELT | Geopolitical | 8081 | ✅ |
| FRED | Macro | 8082 | ✅ |
| Binance | Crypto | 8083 | ✅ |
| Whale Alert | Crypto | 8084 | ✅ |
| CME COT | Macro | 8085 | ✅ |
| Trading Economics | Macro | 8086 | ✅ |
| Telegram | Geopolitical | 8087 | ✅ |

---

## Directory Structure

```
l1-ingestion/
├── cmd/                    # Binary entry points (one per provider)
│   ├── gdelt/main.go
│   ├── fred/main.go
│   ├── binance/main.go
│   ├── whalealert/main.go
│   ├── cot/main.go
│   ├── tradingeconomics/main.go
│   └── telegram/main.go
├── internal/
│   ├── provider/           # Provider interface + implementations
│   │   ├── provider.go     # Core interface: Provider, HealthStatus, ProviderConfig
│   │   ├── base.go         # BaseProvider with common logic (Emit, Health, etc.)
│   │   ├── errors.go       # ErrNoHandlers, ProviderError
│   │   ├── registry.go     # Provider registry (unused currently)
│   │   ├── gdelt/          # GDELT GKG 2.0 provider
│   │   ├── fred/           # FRED API provider
│   │   ├── binance/        # Binance WebSocket provider
│   │   ├── whalealert/     # Whale Alert API provider
│   │   ├── cot/            # CME COT reports provider
│   │   ├── tradingeconomics/ # Trading Economics provider
│   │   └── telegram/       # Telegram Bot API provider
│   ├── signal/             # Signal domain model
│   │   ├── signal.go       # Signal struct, validation, JSON methods
│   │   ├── builder.go      # Builder pattern for creating signals
│   │   ├── sources.go      # Source constants (SourceGDELT, etc.)
│   │   ├── categories.go   # Category constants
│   │   └── errors.go       # Validation errors
│   ├── kafka/              # Kafka producer
│   │   ├── producer.go     # Producer with Send(), SendBatch()
│   │   ├── config.go       # Kafka config struct
│   │   └── metrics.go      # Prometheus metrics
│   ├── config/             # Configuration loading
│   │   └── config.go       # Viper-based config with Load*Config() funcs
│   └── health/             # HTTP health server
│       └── server.go       # /health, /ready, /metrics endpoints
├── deploy/
│   └── docker-compose.yml  # Kafka, Zookeeper, Redis, Prometheus, Grafana
├── docs/
│   └── ACC-L1-MVP-ARCHITECTURE-PLAN.md
├── bin/                    # Compiled binaries (gitignored)
├── Makefile               # Build automation
├── go.mod / go.sum
└── .env.example           # Environment variable reference
```

---

## Core Patterns & Conventions

### 1. Provider Interface Pattern

All providers implement the `provider.Provider` interface:

```go
type Provider interface {
    Name() string
    Category() signal.Category
    Start(ctx context.Context) error
    Stop() error
    Subscribe(handler SignalHandler) error
    Health() HealthStatus
}
```

**SignalHandler** is the callback pattern for emitting signals:
```go
type SignalHandler func(ctx context.Context, sig signal.Signal) error
```

### 2. BaseProvider Composition

Providers embed `*provider.BaseProvider` for common functionality:

```go
type Provider struct {
    *provider.BaseProvider  // Embeds health tracking, signal emission, logging
    config     Config
    httpClient *http.Client
    // provider-specific fields...
}
```

**BaseProvider provides:**
- `Emit(ctx, signal)` - Thread-safe signal emission to handlers
- `RecordSuccess()` / `RecordError(err)` - Health tracking
- `Health()` - Returns HealthStatus with counters
- `MarkStarted()` - Sets start time for uptime calculation
- `StopChannel()` - Returns channel for graceful shutdown
- `Logger()` - Returns zap.Logger

### 3. Signal Schema (Semantic Triple)

Signals follow WHO-DID-WHAT-TO-WHOM pattern:

```go
type Signal struct {
    ID        uuid.UUID       // Auto-generated
    Timestamp time.Time
    Source    Source          // SourceGDELT, SourceFRED, etc.
    Category  Category        // CategoryGeopolitical, CategoryMacro, CategoryCrypto
    
    Subject   string          // WHO: Entity performing action
    Action    string          // WHAT: The action/event type
    Object    string          // TO WHOM: Target entity (optional)
    
    Confidence float64        // 0.0-1.0
    Sentiment  float64        // -1.0 to 1.0
    Urgency    UrgencyLevel   // Low, Medium, High, Critical
    Tags       []string
    RawData    json.RawMessage
    Metadata   Metadata
}
```

**Signal Builder Pattern:**
```go
sig, err := signal.NewBuilder(signal.SourceGDELT, signal.CategoryGeopolitical).
    WithTimestamp(time.Now()).
    WithSubject("Russia").
    WithAction("military_activity").
    WithObject("Ukraine").
    WithConfidence(0.8).
    WithSentiment(-0.6).
    WithUrgency(signal.UrgencyHigh).
    WithTags("conflict", "europe").
    Build()
```

### 4. Kafka Producer Pattern

```go
// In cmd/*/main.go
producer, _ := kafka.NewProducer(kafka.Config{
    Brokers: []string{"localhost:9092"},
    Topic:   "l1-signals-raw",
}, logger)

provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
    return producer.Send(ctx, sig)
})
```

### 5. Configuration Pattern

Environment variables with `L1_` prefix via Viper:

```go
// internal/config/config.go
func LoadGDELTConfig() (gdelt.Config, error) {
    viper.SetEnvPrefix("L1")
    viper.AutomaticEnv()
    
    return gdelt.Config{
        Enabled:      viper.GetBool("GDELT_ENABLED"),
        PollInterval: viper.GetDuration("GDELT_POLL_INTERVAL"),
        BatchSize:    viper.GetInt("GDELT_BATCH_SIZE"),
    }, nil
}
```

### 6. Main Binary Pattern

Standard structure for `cmd/*/main.go`:

```go
func main() {
    // 1. Setup logger
    logger, _ := zap.NewProduction()
    
    // 2. Load config
    cfg, _ := config.LoadGDELTConfig()
    
    // 3. Create provider
    provider := gdelt.New(cfg, logger)
    
    // 4. Create Kafka producer
    kafkaCfg, _ := config.LoadKafkaConfig()
    producer, _ := kafka.NewProducer(kafkaCfg, logger)
    defer producer.Close()
    
    // 5. Subscribe handler
    provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
        return producer.Send(ctx, sig)
    })
    
    // 6. Start health server
    healthServer := health.NewServer(":8081", logger)
    healthServer.RegisterChecker("provider", provider)
    go healthServer.Start()
    
    // 7. Start provider with graceful shutdown
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    
    go provider.Start(ctx)
    
    <-sigCh
    provider.Stop()
}
```

---

## Build & Run Commands

```bash
# Build all providers
make build

# Build specific provider
make build-gdelt

# Run tests
make test
go test ./... -v

# Run with coverage
make test-coverage

# Run specific provider locally
make run-gdelt

# Start infrastructure (Kafka, Redis, Prometheus)
make docker-up

# Stop infrastructure
make docker-down

# View Docker logs
make docker-logs

# Lint code
make lint

# Format code
make fmt

# Full verify (fmt + lint + test)
make verify

# Clean build artifacts
make clean
```

---

## Environment Variables

```bash
# Kafka
L1_KAFKA_BROKERS=localhost:9092
L1_KAFKA_TOPIC_RAW=l1-signals-raw
L1_KAFKA_TOPIC_ENRICHED=l1-signals-enriched

# GDELT
L1_GDELT_ENABLED=true
L1_GDELT_POLL_INTERVAL=15m
L1_GDELT_BATCH_SIZE=250

# FRED
L1_FRED_API_KEY=your-api-key
L1_FRED_POLL_INTERVAL=1h
L1_FRED_SERIES=GDP,UNRATE,CPIAUCSL

# Binance
L1_BINANCE_PAIRS=BTCUSDT,ETHUSDT
L1_BINANCE_LARGE_TRADE_THRESHOLD_USD=100000

# Whale Alert
L1_WHALEALERT_API_KEY=your-api-key
L1_WHALEALERT_MIN_VALUE_USD=1000000

# CME COT
L1_COT_ENABLED=true
L1_COT_POLL_INTERVAL=24h

# Trading Economics
L1_TRADINGECONOMICS_API_KEY=your-api-key
L1_TRADINGECONOMICS_POLL_INTERVAL=5m

# Telegram
L1_TELEGRAM_BOT_TOKEN=your-bot-token
L1_TELEGRAM_CHAT_IDS=-1001234567890
L1_TELEGRAM_KEYWORDS=bitcoin,fed,inflation
```

---

## Technical Debt & Known Issues

### High Priority
1. **Missing Rate Limiter**: Plan specified `internal/ratelimit/` but not implemented
2. **Missing Circuit Breaker**: Plan specified `internal/circuit/` but not implemented
3. **Health Check Stubs**: `KafkaChecker` and `RedisChecker` have TODO comments

### Medium Priority
4. **Missing Tests**: Only 5/7 providers have unit tests (missing: binance, fred)
5. **No Integration Tests**: `test-integration` target exists but no tests written
6. **Missing Orchestrator**: Plan included `cmd/orchestrator/` for lifecycle management

### Low Priority / Deviations from Plan
7. **Naming**: Plan used `sensor-*` prefix, implementation uses just provider name
8. **Directory**: Plan had `internal/providers/` (plural), actual is `internal/provider/<name>/`
9. **Signals Channel**: Plan used channels, implementation uses callback handlers

---

## Testing Strategy

### Unit Tests
- Located in `*_test.go` files alongside implementation
- Use table-driven tests with `t.Run()`
- Mock HTTP responses for API providers
- Test signal validation and building

### Running Tests
```bash
# All tests
go test ./...

# Specific package
go test ./internal/provider/gdelt/...

# With coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Test Patterns
```go
func TestProvider_ParseGKGLine(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        wantOk   bool
        wantSubj string
    }{
        {"valid line", "...", true, "US"},
        {"invalid line", "...", false, ""},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            p := New(DefaultConfig(), nil)
            sig, ok := p.parseGKGLine(tt.input, time.Now())
            if ok != tt.wantOk {
                t.Errorf("got ok=%v, want %v", ok, tt.wantOk)
            }
            if ok && sig.Subject != tt.wantSubj {
                t.Errorf("got subject=%q, want %q", sig.Subject, tt.wantSubj)
            }
        })
    }
}
```

---

## Adding a New Provider

### Checklist
1. [ ] Create `internal/provider/<name>/<name>.go`
2. [ ] Create `internal/provider/<name>/<name>_test.go`
3. [ ] Add `Source<Name>` constant to `internal/signal/sources.go`
4. [ ] Add `Load<Name>Config()` to `internal/config/config.go`
5. [ ] Create `cmd/<name>/main.go` entry point
6. [ ] Add build targets to `Makefile`
7. [ ] Add env vars to `.env.example`
8. [ ] Test with `make build-<name> && make run-<name>`

### Provider Template
```go
package myprovider

import (
    "context"
    "github.com/openclaworg/l1-ingestion/internal/provider"
    "github.com/openclaworg/l1-ingestion/internal/signal"
    "go.uber.org/zap"
)

type Config struct {
    Enabled      bool
    PollInterval time.Duration
    // provider-specific config...
}

type Provider struct {
    *provider.BaseProvider
    config Config
    // provider-specific fields...
}

func New(cfg Config, logger *zap.Logger) *Provider {
    baseCfg := provider.ProviderConfig{
        Name:     "myprovider",
        Category: signal.CategoryMacro,
        // ...
    }
    return &Provider{
        BaseProvider: provider.NewBaseProvider(baseCfg, logger.Named("myprovider")),
        config:       cfg,
    }
}

func (p *Provider) Start(ctx context.Context) error {
    p.MarkStarted()
    // Start polling/listening...
    return nil
}

func (p *Provider) Stop() error {
    return p.BaseProvider.Stop()
}
```

---

## Architecture Decisions

### Why Go?
- High-concurrency native support (goroutines)
- Excellent HTTP/WebSocket performance
- Single binary deployment
- Strong typing catches bugs at compile time

### Why Callback Handlers vs Channels?
- Original plan used channels, but callbacks are simpler
- Allows multiple handlers (e.g., Kafka + logging)
- Avoids channel management complexity

### Why segmentio/kafka-go?
- Pure Go implementation (no CGO)
- Good batch support
- Simple API

### Why Viper for Config?
- Environment variable support with prefix
- Easy testing (can override in tests)
- Industry standard

---

## Common Pitfalls

1. **Forgetting `MarkStarted()`**: Health uptime will be zero
2. **Not calling `RecordSuccess()`/`RecordError()`**: Health status won't update
3. **Blocking in signal handlers**: Use goroutines for slow operations
4. **Missing context cancellation check**: Long-running loops should check `ctx.Done()`
5. **HTTP client without timeout**: Always set `http.Client{Timeout: ...}`

---

## Future Work (Wave 4+)

- **Wave 4**: Python SLM Worker (Kafka consumer + Qwen2.5-1.5B)
- **Wave 5**: Snowflake Integration (Kafka Connector + dbt models)
- **Wave 6**: Grafana dashboards, Prometheus alerting
- **Wave 7**: Kubernetes manifests, CI/CD pipeline
