# CLAUDE.md - L1 Ingestion Module

> **Purpose**: Long-term memory for AI assistants working on this codebase.
> **Last Updated**: 2026-02-04
> **Status**: Wave 8.5 Complete (L2 Reasoner + Alerting Upgrade)

---

## Project Overview

**L1-Ingestion** is a Go-based real-time signal ingestion layer for the ACC (Autonomous Cognitive Core) system. It collects geopolitical, macroeconomic, community, and crypto signals from multiple data sources and publishes them to Redpanda (Kafka-compatible) for downstream processing. The downstream Python stack (SLM worker → Persister → L2 Reasoner → Alerter) now produces both enriched L1 events and higher-order L2 insights that reach the Telegram/Discord channels.

### Tech Stack
| Component | Technology |
|-----------|------------|
| Language | Go 1.21+ |
| Message Broker | Redpanda (Kafka-compatible, single binary) |
| Kafka Client | segmentio/kafka-go |
| Logging | Uber Zap |
| Config | Viper (env vars with `L1_` prefix) |
| HTTP | net/http (stdlib) |
| UUID | google/uuid |
| Metrics | Prometheus (promhttp) |
| Container | Docker with multi-stage builds |
| SLM | Python + Qwen 2.5-1.5B-Instruct |
| Alerting | Python + Telegram Bot API |
| Observability | Prometheus + Grafana |

### Data Sources (7 Providers)
| Provider | Category | Port | API Key Required | Status |
|----------|----------|------|------------------|--------|
| GDELT | Geopolitical | 8081 | ❌ No (public files) | ✅ Running |
| FRED | Macro | 8086 | ✅ Yes (free) | ✅ Running |
| Binance | Crypto | 8083 | ❌ No (public WebSocket) | ✅ Running |
| Whale Alert | Crypto | 8084 | ✅ Yes (paid) | ⏸️ Deferred |
| CME COT | Macro | 8085 | ❌ No (public files) | ✅ Running |
| Trading Economics | Macro | - | ✅ Yes (paid) | ⏸️ Deferred |
| Telegram | Geopolitical/Crypto | 8087 | ✅ Yes (bot token) | ✅ Running |

### Python Processing Services
| Service | Description | Status |
|---------|-------------|--------|
| SLM Worker | Enriches `l1.signals.raw` via Qwen 2.5-1.5B → `l1.signals.enriched` | ✅ Running |
| Alerter | Sends Telegram/Discord notifications for ACC L1 Signals + optional L2 Insights | ✅ Running |
| Persister | Stores enriched signals to SQLite (snappy-enabled consumer) + Query API (:8088) | ✅ Running |
| L2 Reasoner | Correlates enriched signals into insights/entities (`l2.insights`, `l2.situations`, `l2.entities`) | ✅ Running |

### Observability Stack
| Service | Port | Description | Status |
|---------|------|-------------|--------|
| Prometheus | 9090 | Metrics collection (providers, Python services, L2 reasoner) | ✅ Running |
| Grafana | 3000 | Dashboards (L1 overview, Redpanda, providers, **new L2 reasoner board**) | ✅ Running |

---

## Directory Structure

```
l1-ingestion/
├── cmd/                    # Binary entry points (one per provider + orchestrator)
│   ├── gdelt/main.go
│   ├── fred/main.go
│   ├── binance/main.go
│   ├── whalealert/main.go
│   ├── cot/main.go
│   ├── tradingeconomics/main.go
│   ├── telegram/main.go
│   └── orchestrator/main.go # Unified manager for all providers
├── internal/
│   ├── provider/           # Provider interface + implementations
│   │   ├── provider.go     # Core interface: Provider, HealthStatus, ProviderConfig
│   │   ├── base.go         # BaseProvider with common logic (Emit, Health, etc.)
│   │   ├── errors.go       # ErrNoHandlers, ProviderError
│   │   ├── registry.go     # Provider registry (used by orchestrator)
│   │   ├── gdelt/          # GDELT GKG 2.0 provider
│   │   ├── fred/           # FRED API provider
│   │   ├── binance/        # Binance WebSocket provider
│   │   ├── whalealert/     # Whale Alert API provider
│   │   ├── cot/            # CME COT reports provider
│   │   ├── tradingeconomics/ # Trading Economics provider
│   │   └── telegram/       # Telegram Bot API provider
│   ├── orchestrator/       # Provider lifecycle management
│   │   └── orchestrator.go # Orchestrator with supervision, rate limiting, circuit breakers
│   ├── ratelimit/          # Rate limiting
│   │   └── limiter.go      # Token bucket + keyed limiter
│   ├── circuit/            # Circuit breaker
│   │   └── breaker.go      # Closed/open/half-open states
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
├── python/                 # Python processing layer
│   ├── slm_worker/         # SLM enrichment service
│   │   ├── __init__.py
│   │   ├── main.py         # Kafka consumer loop
│   │   ├── processor.py    # Qwen 2.5-1.5B model inference
│   │   ├── signal_schema.py # Signal dataclass (matches Go)
│   │   └── config.py       # Environment configuration
│   ├── alerter/            # Alert notification service
│   │   ├── __init__.py
│   │   ├── main.py         # Kafka consumer (l1 + optional l2 topics)
│   │   ├── filter.py       # Alert filtering rules
│   │   ├── notifier.py     # Telegram/Discord senders (L1 vs L2 templates)
│   │   ├── config.py       # Environment configuration (`L2_ALERTS_ENABLED`, etc.)
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   ├── persister/          # Signal persistence service
│   │   ├── __init__.py
│   │   ├── main.py         # Kafka consumer loop (snappy-enabled)
│   │   ├── database.py     # SQLite manager with schema
│   │   ├── api.py          # HTTP query API (:8088)
│   │   ├── config.py       # Environment configuration
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   ├── l2_reasoner/        # L2 insight & entity engine
│   │   ├── __init__.py
│   │   ├── main.py         # Consume `l1.signals.enriched`, emit `l2.*`
│   │   ├── engine/         # Correlation, rules, entity tracking
│   │   ├── storage/        # SQLite state/cache
│   │   ├── config.py
│   │   └── requirements.txt
│   ├── requirements.txt    # Shared Python deps
│   └── Dockerfile          # SLM worker Docker build
├── deploy/
│   ├── docker-compose.yml  # Redpanda, providers, Python services, monitoring
│   ├── docker/
│   │   └── Dockerfile.provider  # Multi-stage Go build
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   ├── dashboards/           # Dashboard JSON files
│   │   │   ├── l1-overview.json  # System overview dashboard
│   │   │   ├── redpanda.json     # Kafka/Redpanda metrics
│   │   │   └── providers.json    # Provider health dashboard
│   │   └── provisioning/
│   │       ├── dashboards/
│   │       │   └── dashboards.yml
│   │       └── datasources/
│   │           └── datasources.yml
│   └── .env                # Environment variables (gitignored)
├── docs/
│   ├── ACC-L1-MVP-ARCHITECTURE-PLAN.md
│   ├── L2_L3_SPEC.md             # L2 Reasoning + L3 Decision specs
│   ├── L4_SPEC.md                # L4 Paper Trading + Strategy Allocation
│   └── oracle_provider_research.md # Low-cost provider options (Oracle, 2026-02)
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

# Build orchestrator (manages all providers)
make build-orchestrator

# Run tests
make test
go test ./... -v

# Run with coverage
make test-coverage

# Run specific provider locally
make run-gdelt

# Run orchestrator (all providers in one process)
make run-orchestrator
# Or with specific providers:
L1_ORCHESTRATOR_PROVIDERS=gdelt,binance make run-orchestrator
# Enable supervision (auto-restart unhealthy providers):
L1_ORCHESTRATOR_SUPERVISION=true make run-orchestrator

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

# Telegram Ingestor
L1_TELEGRAM_BOT_TOKEN=your-bot-token
L1_TELEGRAM_CHAT_IDS=-1001234567890,-1009876543210
L1_TELEGRAM_KEYWORDS=bitcoin,fed,inflation
L1_TELEGRAM_POLL_INTERVAL=5s
L1_TELEGRAM_ENABLED=true

# Alerter (L1 + optional L2 alerts)
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=bot-token
TELEGRAM_CHAT_ID=-1001234567890
DISCORD_ENABLED=false
L2_ALERTS_ENABLED=true
L2_INSIGHTS_TOPIC=l2.insights
L2_MIN_SEVERITY=warning
ALERTER_DRY_RUN=false

# L2 Reasoner
L2_KAFKA_BROKERS=redpanda:9092
L2_KAFKA_INPUT_TOPIC=l1.signals.enriched
L2_OUTPUT_INSIGHTS_TOPIC=l2.insights
L2_OUTPUT_SITUATIONS_TOPIC=l2.situations
L2_OUTPUT_ENTITIES_TOPIC=l2.entities
L2_DB_PATH=/data/l2_reasoner.db
L2_CORRELATION_WINDOW_HOURS=4
L2_ANOMALY_ZSCORE_THRESHOLD=2.5
```

---

## Technical Debt & Known Issues

### High Priority
None currently.

### Medium Priority
1. **No Integration Tests**: `test-integration` target exists but no tests written

### Low Priority / Deviations from Plan
2. **Naming**: Plan used `sensor-*` prefix, implementation uses just provider name
3. **Directory**: Plan had `internal/providers/` (plural), actual is `internal/provider/<name>/`
4. **Signals Channel**: Plan used channels, implementation uses callback handlers

### Resolved
- ✅ All 7 providers now have unit tests (binance, fred added Feb 2025)
- ✅ GitHub Actions CI added (test, lint, build jobs)
- ✅ Kafka topic names reconciled to `l1.signals.*` convention
- ✅ setup.sh updated for Redpanda (was referencing old Zookeeper/Kafka)
- ✅ Health checkers (`KafkaChecker`, `RedisChecker`) are fully implemented
- ✅ Package tests added for `internal/config`, `internal/health`, `internal/kafka`
- ✅ Rate limiter implemented (`internal/ratelimit/`) with token bucket + keyed limiter
- ✅ Circuit breaker implemented (`internal/circuit/`) with closed/open/half-open states
- ✅ Orchestrator implemented (`internal/orchestrator/`, `cmd/orchestrator/`) for lifecycle management

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

### Critical: Viper Environment Variable Binding

**Problem**: Viper's `UnmarshalKey()` does NOT automatically read environment variables for nested config keys, even with `AutomaticEnv()` enabled.

**Wrong** (env vars ignored):
```go
v.SetEnvPrefix("L1")
v.AutomaticEnv()
var cfg BinanceConfig
v.UnmarshalKey("binance", &cfg)  // L1_BINANCE_PAIRS won't be read!
```

**Correct** (explicit binding + manual override):
```go
v.SetEnvPrefix("L1")
v.AutomaticEnv()

// Explicit bindings for nested keys
v.BindEnv("binance.pairs", "L1_BINANCE_PAIRS")
v.BindEnv("binance.large_trade_threshold_usd", "L1_BINANCE_LARGE_TRADE_THRESHOLD_USD")

var cfg BinanceConfig
v.UnmarshalKey("binance", &cfg)

// Manual override using Get* methods
if v.IsSet("binance.pairs") {
    cfg.Pairs = v.GetStringSlice("binance.pairs")
}
if v.IsSet("binance.large_trade_threshold_usd") {
    cfg.LargeTradeThresholdUSD = v.GetFloat64("binance.large_trade_threshold_usd")
}
```

### Critical: Comma-Separated Env Vars

**Problem**: Viper's `GetStringSlice()` returns `["a,b,c"]` (single element) instead of `["a", "b", "c"]` for comma-separated env vars.

**Solution**: Check and split manually:
```go
if pairs := v.GetStringSlice("binance.pairs"); len(pairs) > 0 {
    if len(pairs) == 1 && strings.Contains(pairs[0], ",") {
        cfg.Pairs = strings.Split(pairs[0], ",")
    } else {
        cfg.Pairs = pairs
    }
}
```

### Critical: ZIP vs GZIP Decompression

**Problem**: Many government/data sources use `.zip` archives, NOT `.gz` gzip files. Using `compress/gzip` on ZIP files produces "invalid header" errors.

| File Extension | Correct Package |
|----------------|-----------------|
| `.gz` | `compress/gzip` |
| `.zip` | `archive/zip` |

**ZIP decompression pattern**:
```go
import (
    "archive/zip"
    "bytes"
    "io"
)

body, _ := io.ReadAll(resp.Body)
zipReader, _ := zip.NewReader(bytes.NewReader(body), int64(len(body)))

for _, f := range zipReader.File {
    if strings.HasSuffix(f.Name, ".csv") {
        rc, _ := f.Open()
        defer rc.Close()
        // Parse rc as CSV reader
    }
}
```

### Provider-Specific Data Source Notes

| Provider | Data Format | URL Pattern | Notes |
|----------|-------------|-------------|-------|
| GDELT | ZIP → CSV | `data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.gkg.csv.zip` | Files every 15 min |
| COT | ZIP → TXT/CSV | `cftc.gov/files/dea/history/deacot{YYYY}.zip` | Annual files, try current year then previous |
| Binance | WebSocket JSON | `wss://stream.binance.com:9443/stream` | Public market data, no auth needed |

1. **Forgetting `MarkStarted()`**: Health uptime will be zero
2. **Not calling `RecordSuccess()`/`RecordError()`**: Health status won't update
3. **Blocking in signal handlers**: Use goroutines for slow operations
4. **Missing context cancellation check**: Long-running loops should check `ctx.Done()`
5. **HTTP client without timeout**: Always set `http.Client{Timeout: ...}`

---

## Completed Waves

### Wave 4: Python SLM Worker ✅
- Consumes raw signals from `l1.signals.raw` topic
- Enriches signals using **Qwen 2.5-1.5B-Instruct** model (local inference)
- Publishes enriched signals to `l1.signals.enriched` topic
- Adds: sentiment analysis, urgency classification, market impact assessment, summary
- Location: `python/slm_worker/`

### Wave 4.5: Telegram Alerter ✅
- Consumes enriched signals from `l1.signals.enriched` topic
- Filters based on: urgency (≥high), sentiment magnitude (≥0.5), market impact
- Sends real-time notifications via Telegram Bot API
- Rate limiting (30/min) and deduplication (5min window)
- Location: `python/alerter/`

### Wave 5: Persistence Layer ✅
- Consumes enriched signals from `l1.signals.enriched` topic
- Persists to SQLite database with WAL mode for concurrent access
- Deduplication prevents storing duplicate signals
- HTTP Query API on port 8088 with filtering (source, category, urgency, time range)
- Backup script with compression and retention policy
- Location: `python/persister/`

### Wave 6: Observability ✅
- **Grafana Dashboards** provisioned automatically via JSON files
- Three dashboards created:
  - **L1 Overview** (`/d/l1-overview`): Service health, signal rates, resource usage
  - **Redpanda Metrics** (`/d/redpanda`): Topic throughput, consumer lag, request latency
  - **Provider Health** (`/d/providers`): Per-provider memory, CPU, goroutines
- **Prometheus** scrapes metrics from Redpanda and Go providers
- Datasource provisioning with explicit UID for dashboard compatibility
- Access: http://localhost:3000 (admin/admin)
- Location: `deploy/grafana/dashboards/`

### Wave 7: FRED Provider ✅
- **Federal Reserve Economic Data** provider now running
- Polls 8 key economic indicators every hour:
  - DFF: Federal Funds Effective Rate
  - T10Y2Y: 10-Year Treasury Minus 2-Year (yield curve)
  - UNRATE: Unemployment Rate
  - CPIAUCSL: Consumer Price Index (CPI)
  - GDP: Gross Domestic Product
  - MORTGAGE30US: 30-Year Fixed Rate Mortgage
  - DTWEXBGS: Trade Weighted Dollar Index
  - VIXCLS: CBOE Volatility Index (VIX)
- Sentiment calculation based on economic implications (e.g., inverted yield curve = negative)
- Alerter enhanced with FRED-specific formatting showing indicator values and changes
- Port: 8086 | API Key: Free (get from https://fred.stlouisfed.org/docs/api/api_key.html)

### Wave 7.5: Telegram Ingestor ✅
- **Telegram channel/group monitoring** for breaking news and market signals
- Uses Telegram Bot API `getUpdates` long polling
- Features:
  - **Keyword filtering**: Only processes messages containing configured keywords
  - **Default keywords** (20): breaking, urgent, alert, warning, military, attack, strike, explosion, sanction, tariff, embargo, fed, ecb, boj, rate, inflation, bitcoin, ethereum, crypto, whale
  - **Category classification**: Auto-classifies as geopolitical, macro, or crypto based on content
  - **Sentiment analysis**: Basic keyword-based sentiment scoring
  - **Urgency detection**: Flags "breaking", "urgent", "emergency" as high urgency
  - **Deduplication**: Tracks seen messages to avoid duplicates
- Alerter enhanced with Telegram-specific formatting showing channel, username, and message preview
- Port: 8087 | Bot Token: Reuses alerter bot (or create separate bot via @BotFather)
- Configuration:
  - `L1_TELEGRAM_BOT_TOKEN`: Bot token from @BotFather
  - `L1_TELEGRAM_CHAT_IDS`: Comma-separated channel/group IDs (negative numbers)
  - `L1_TELEGRAM_KEYWORDS`: Comma-separated keywords to filter messages
  - `L1_TELEGRAM_POLL_INTERVAL`: Polling interval (default: 5s)
- **Note**: Bot must be added to channels/groups to receive messages. To get channel IDs, forward a message to @userinfobot on Telegram.

### Wave 8: Orchestrator ✅
- **Unified provider management** with lifecycle supervision
- **Features**:
  - **Supervision**: Auto-restart for unhealthy providers with configurable thresholds
  - **Rate limiting**: Global and per-provider rate limiting via token bucket algorithm
  - **Circuit breaking**: Per-provider circuit breakers with closed/open/half-open states
  - **Health monitoring**: Centralized health status for all providers
  - **Graceful shutdown**: Coordinated shutdown with timeout handling
  - **Provider registry**: Dynamic provider registration and discovery
- **Configuration**:
  - `L1_ORCHESTRATOR_SUPERVISION=true` enables auto-restart
  - `L1_ORCHESTRATOR_PROVIDERS=gdelt,binance,fred` comma-separated provider list
- **Commands**:
  - `make build-orchestrator` builds the orchestrator binary
  - `make run-orchestrator` runs all providers in a single process
  - `L1_ORCHESTRATOR_PROVIDERS=gdelt,binance make run-orchestrator` runs specific providers
- **Health endpoint**: `/health` endpoint shows orchestrator and provider status

## Future Work (Wave 9+)

- **Wave 9**: Kubernetes manifests, CI/CD pipeline
- **Wave 10**: L2 Reasoning layer implementation

---

## L2/L3/L4 Architecture Reference

The L1 ingestion layer feeds into higher layers of the ACC system. Specifications exist for:

### L2: Reasoning Layer
- **Purpose**: Correlation detection, entity tracking, situation rollups
- **Inputs**: `l1.signals.enriched`
- **Outputs**: `l2.insights`, `l2.situations`, `l2.entities`
- **Spec**: `docs/L2_L3_SPEC.md`

### L3: Decision/Execution Layer
- **Purpose**: Action proposals, guardrails, paper execution, audit trail
- **Inputs**: `l2.insights`, `l2.situations`, `l4.plans`
- **Outputs**: `l3.proposals`, `l3.actions`, `l3.audit`
- **Paper Trading**: Executes L4 rebalance plans with fee/slippage simulation
- **Spec**: `docs/L2_L3_SPEC.md`

### L4: Paper Trading + Strategy Allocation
- **Purpose**: Portfolio management, All-Weather risk parity, contextual bandit learning
- **Inputs**: `l1.signals.enriched`, `l2.situations`, `l3.actions`
- **Outputs**: `l4.plans`, `l4.portfolio.snapshots`, `l4.evaluations`, `l4.bandit.state`
- **Assets**: Binance tradables + FRED-priced synthetics (SP500, bonds, gold, oil)
- **Learning**: Conservative contextual bandit allocates between known strategies
- **Spec**: `docs/L4_SPEC.md`

### Full Pipeline
```
L1 (Ingestion) → L2 (Reasoning) → L4 (Portfolio) → L3 (Execution) → L4 (Evaluation)
     │                │                │                │
     ▼                ▼                ▼                ▼
 l1.signals.raw  l2.insights      l4.plans        l3.actions
 l1.signals.enriched  l2.situations              l4.evaluations
```

---

## Docker Commands Reference

```bash
# Start full stack
cd deploy && docker compose up -d

# Rebuild specific provider
docker compose build binance --no-cache && docker compose up -d binance

# View logs
docker compose logs -f binance
docker compose logs gdelt --tail 50
docker compose logs telegram-ingestor --tail 50

# Check health
curl http://localhost:8081/health  # GDELT
curl http://localhost:8083/health  # Binance
curl http://localhost:8085/health  # COT
curl http://localhost:8086/health  # FRED
curl http://localhost:8087/health  # Telegram

# Redpanda commands
docker exec l1-redpanda rpk topic list --brokers localhost:9092
docker exec l1-redpanda rpk topic consume l1.signals.raw --brokers localhost:9092 --num 5
docker exec l1-redpanda rpk topic describe l1.signals.raw --brokers localhost:9092

# Prometheus targets
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[].health'

# Persister API
curl http://localhost:8088/health
curl http://localhost:8088/stats
curl "http://localhost:8088/signals?limit=10&urgency=high"
curl "http://localhost:8088/signals?source=telegram&limit=5"

# Grafana dashboards
open http://localhost:3000  # Login: admin/admin
# Dashboard URLs:
#   - Overview: http://localhost:3000/d/l1-overview
#   - Redpanda: http://localhost:3000/d/redpanda
#   - Providers: http://localhost:3000/d/providers
```

---

## Kafka Topics

| Topic | Partitions | Purpose |
|-------|------------|---------|
| `l1.signals.raw` | 6 | Raw signals from providers |
| `l1.signals.enriched` | 6 | SLM-enriched signals |
| `l1.signals.filtered` | 3 | High-priority filtered signals |
| `l1.signals.dlq` | 1 | Dead letter queue |

### L2/L3/L4 Topics (Defined in Specs)

| Topic | Partitions | Purpose |
|-------|------------|---------|
| `l2.insights` | 6 | Correlation/anomaly detections |
| `l2.situations` | 3 | Stateful situation rollups |
| `l2.entities` | 6 | Entity state tracking |
| `l3.proposals` | 3 | Action proposals |
| `l3.actions` | 3 | Executed action results |
| `l3.audit` | 1 | Complete audit trail |
| `l4.plans` | 3 | Portfolio rebalance plans |
| `l4.portfolio.snapshots` | 3 | Portfolio state |
| `l4.evaluations` | 3 | Performance attribution |
| `l4.bandit.state` | 1 | Learner state (compacted) |
