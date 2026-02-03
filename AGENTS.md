# AGENTS.md - Agent Workflow Guidelines

> **Purpose**: Coordination rules for AI agents working on L1-Ingestion
> **Last Updated**: 2026-02-03
> **Module**: l1-ingestion (Go sensors for ACC)

---

## Quick Start for New Agents

Before working on this codebase, read these critical files:
1. `CLAUDE.md` - Full project documentation and patterns
2. `internal/provider/provider.go` - Provider interface contract
3. `internal/signal/signal.go` - Signal schema (DO NOT MODIFY)

### Current System Status
```
✅ GDELT    (8081) - Geopolitical events from public file archive
✅ Binance  (8083) - Crypto large trades via public WebSocket  
✅ COT      (8085) - Futures positioning from CFTC public files
⏸️ FRED    (8082) - Needs free API key from fred.stlouisfed.org
⏸️ Others  - Require paid API keys
```

---

## Agent Roles

### 🔧 Sensor-Dev (Go Provider Development)

**Responsibility**: Building and maintaining Go-based data providers

**Skills Required**:
- Go concurrency patterns (goroutines, channels, context)
- HTTP client handling (timeouts, retries)
- WebSocket connections (Binance)
- Signal schema compliance

**Typical Tasks**:
- Implement new providers
- Fix provider bugs
- Add unit tests
- Optimize polling/parsing logic

**Before Starting Work**:
1. Read `internal/provider/provider.go` (interface contract)
2. Read `internal/provider/base.go` (composition pattern)
3. Read one existing provider as reference (e.g., `internal/provider/gdelt/gdelt.go`)
4. Review signal schema in `internal/signal/signal.go`

---

### 🧠 Processor-Dev (Python SLM Development)

**Responsibility**: Building the Python SLM worker (Wave 4)

**Skills Required**:
- Python async patterns (asyncio, aiokafka)
- Hugging Face Transformers
- Kafka consumer/producer patterns
- Signal enrichment logic

**Typical Tasks**:
- Implement SLM processor
- Kafka consumer setup
- Signal enrichment pipeline
- Dead letter queue handling

**Before Starting Work**:
1. Read signal schema in `internal/signal/signal.go` (source of truth)
2. Review architecture plan in `docs/ACC-L1-MVP-ARCHITECTURE-PLAN.md` (Section 4)
3. Understand input topic: `l1-signals-raw`
4. Understand output topic: `l1-signals-enriched`

---

### 📊 Data-Dev (Snowflake/dbt Development)

**Responsibility**: Building data pipeline to Snowflake (Wave 5)

**Skills Required**:
- Snowflake SQL
- dbt (data build tool)
- Kafka Connect configuration

**Typical Tasks**:
- Create Snowflake DDL
- Configure Kafka Connector
- Build dbt staging/mart models
- Verify data flow

---

## Workflow Constraints

### ⚠️ CRITICAL RULES

1. **Signal Schema is Sacred**
   - NEVER modify `internal/signal/signal.go` without explicit approval
   - All providers MUST produce valid signals (call `signal.Validate()`)
   - Required fields: `ID`, `Timestamp`, `Source`, `Category`, `Subject`, `Action`

2. **Provider Interface is Stable**
   - Do not modify `internal/provider/provider.go` interface
   - New methods should be added to specific providers, not the interface

3. **Health Tracking is Required**
   - All providers MUST call `MarkStarted()` in `Start()`
   - All providers MUST call `RecordSuccess()`/`RecordError()` appropriately
   - Health endpoints are used for Kubernetes probes

4. **Context Cancellation is Required**
   - All polling loops MUST check `ctx.Done()` and `p.StopChannel()`
   - All HTTP requests MUST use `http.NewRequestWithContext(ctx, ...)`

---

## Provider Implementation Checklist

When implementing a new provider, complete ALL items:

### Code Structure
- [ ] Create `internal/provider/<name>/<name>.go`
- [ ] Provider embeds `*provider.BaseProvider`
- [ ] Provider implements `provider.Provider` interface
- [ ] `New()` constructor accepts Config and *zap.Logger
- [ ] `Start()` calls `p.MarkStarted()` first
- [ ] `Stop()` calls `p.BaseProvider.Stop()` and waits for goroutines

### Signal Generation
- [ ] Use `signal.NewBuilder(source, category)` pattern
- [ ] Set all required fields (Subject, Action)
- [ ] Set Confidence (0.0-1.0 range)
- [ ] Set Sentiment (-1.0 to 1.0 range)
- [ ] Set Urgency (use constants: `UrgencyLow`, `UrgencyMedium`, etc.)
- [ ] Include raw data in `WithRawData()`
- [ ] Call `Build()` and check error

### Error Handling
- [ ] Call `p.RecordError(err)` on failures
- [ ] Call `p.RecordSuccess()` on successful operations
- [ ] Log errors with `p.Logger().Error(...)`
- [ ] Handle HTTP timeouts gracefully

### Configuration
- [ ] Add `Load<Name>Config()` in `internal/config/config.go`
- [ ] Use `L1_<NAME>_<FIELD>` env var pattern
- [ ] Add `Default<Name>Config()` function
- [ ] Document env vars in `.env.example`

### Entry Point
- [ ] Create `cmd/<name>/main.go`
- [ ] Follow standard main pattern (see CLAUDE.md)
- [ ] Register health checker
- [ ] Handle graceful shutdown (SIGINT/SIGTERM)

### Build System
- [ ] Add `build-<name>` target to Makefile
- [ ] Add `run-<name>` target to Makefile
- [ ] Add to `BINARIES` list in Makefile

### Testing
- [ ] Create `<name>_test.go` with unit tests
- [ ] Test signal generation
- [ ] Test configuration loading
- [ ] Mock HTTP responses (don't hit real APIs in tests)

---

## Verification Steps Before Commit

### 1. Build Check
```bash
make build
# Must succeed with no errors
```

### 2. Test Check
```bash
make test
# All tests must pass
```

### 3. Lint Check
```bash
make lint
# No lint errors (warnings acceptable)
```

### 4. Signal Validation
```go
// Every signal must pass validation
sig, err := builder.Build()
if err != nil {
    return err // Handle error!
}
if err := sig.Validate(); err != nil {
    return err // Signal is invalid!
}
```

### 5. Integration Check (Optional)
```bash
# Start infrastructure
make docker-up

# Run provider
make run-<name>

# Check Kafka topic (use kafka-console-consumer)
# Verify signals appear in topic

make docker-down
```

---

## Signal Schema Compliance

### Required Fields
| Field | Type | Validation |
|-------|------|------------|
| ID | UUID | Auto-generated by builder |
| Timestamp | time.Time | Must be set |
| Source | signal.Source | Must be valid (SourceGDELT, etc.) |
| Category | signal.Category | Must be valid (CategoryGeopolitical, etc.) |
| Subject | string | Non-empty |
| Action | string | Non-empty |

### Optional Fields
| Field | Type | Default |
|-------|------|---------|
| Object | string | "" |
| Confidence | float64 | 0.5 |
| Sentiment | float64 | 0.0 |
| Urgency | UrgencyLevel | UrgencyMedium |
| Tags | []string | nil |
| RawData | json.RawMessage | nil |

### Valid Sources (signal.Source)
```go
SourceGDELT        = "gdelt"
SourceFRED         = "fred"
SourceBinance      = "binance"
SourceWhaleAlert   = "whalealert"
SourceCMECOT       = "cot"
SourceTradingEcon  = "tradingeconomics"
SourceTelegram     = "telegram"
```

### Valid Categories (signal.Category)
```go
CategoryGeopolitical = "geopolitical"
CategoryMacro        = "macro"
CategoryCrypto       = "crypto"
```

### Valid Urgency Levels
```go
UrgencyLow      = "low"       // 1
UrgencyMedium   = "medium"    // 2
UrgencyHigh     = "high"      // 3
UrgencyCritical = "critical"  // 4
```

---

## Common Mistakes to Avoid

### ❌ DON'T: Forget to track health
```go
func (p *Provider) poll(ctx context.Context) {
    data, err := p.fetch()
    if err != nil {
        p.Logger().Error("fetch failed", zap.Error(err))
        return // Missing RecordError!
    }
    // Missing RecordSuccess!
}
```

### ✅ DO: Always track health
```go
func (p *Provider) poll(ctx context.Context) {
    data, err := p.fetch()
    if err != nil {
        p.Logger().Error("fetch failed", zap.Error(err))
        p.RecordError(err)
        return
    }
    p.RecordSuccess()
}
```

### ❌ DON'T: Use gzip for ZIP files
```go
// WRONG - GDELT/COT files are ZIP, not GZIP!
gzReader, err := gzip.NewReader(resp.Body)
```

### ✅ DO: Use archive/zip for .zip files
```go
body, _ := io.ReadAll(resp.Body)
zipReader, _ := zip.NewReader(bytes.NewReader(body), int64(len(body)))
for _, f := range zipReader.File {
    rc, _ := f.Open()
    defer rc.Close()
    // Use rc as reader
}
```

### ❌ DON'T: Trust Viper UnmarshalKey with env vars
```go
// WRONG - env vars won't be read for nested keys!
v.AutomaticEnv()
v.UnmarshalKey("binance", &cfg)
```

### ✅ DO: Explicitly bind and override
```go
v.AutomaticEnv()
v.BindEnv("binance.threshold", "L1_BINANCE_THRESHOLD")
v.UnmarshalKey("binance", &cfg)
// Manual override
if v.IsSet("binance.threshold") {
    cfg.Threshold = v.GetFloat64("binance.threshold")
}
```

### ❌ DON'T: Block in signal handler
```go
provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
    time.Sleep(5 * time.Second) // Blocks polling!
    return producer.Send(ctx, sig)
})
```

### ✅ DO: Keep handlers fast
```go
provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
    // Kafka producer handles batching internally
    return producer.Send(ctx, sig)
})
```

### ❌ DON'T: Ignore context cancellation
```go
func (p *Provider) pollLoop(ctx context.Context) {
    for {
        p.poll(ctx)
        time.Sleep(p.config.PollInterval) // Doesn't check cancellation!
    }
}
```

### ✅ DO: Check for cancellation
```go
func (p *Provider) pollLoop(ctx context.Context) {
    ticker := time.NewTicker(p.config.PollInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-p.StopChannel():
            return
        case <-ticker.C:
            p.poll(ctx)
        }
    }
}
```

---

## Data Source Specifics

### GDELT
- **URL**: `http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.gkg.csv.zip`
- **Format**: ZIP archive containing CSV
- **Frequency**: Files published every 15 minutes
- **Auth**: None (public)

### Binance
- **URL**: `wss://stream.binance.com:9443/stream`
- **Format**: WebSocket JSON streams
- **Auth**: None for public market data (only needed for account APIs)
- **Streams**: `{symbol}@trade`, `{symbol}@ticker`

### CME COT (CFTC)
- **URL**: `https://www.cftc.gov/files/dea/history/deacot{YYYY}.zip`
- **Format**: ZIP archive containing TXT (CSV format)
- **Frequency**: Weekly (Friday release), annual files
- **Auth**: None (public government data)
- **Fallback**: Try current year, then previous year

### FRED
- **URL**: `https://api.stlouisfed.org/fred/series/observations`
- **Format**: JSON API
- **Auth**: Required (free key from fred.stlouisfed.org)

---

## Inter-Agent Communication

### Sensor-Dev → Processor-Dev
- **Contract**: Signals published to `l1-signals-raw` topic
- **Format**: JSON-encoded `signal.Signal`
- **Key**: `{source}:{subject}` (for partition affinity)

### Processor-Dev → Data-Dev
- **Contract**: Enriched signals to `l1-signals-enriched` topic
- **Format**: JSON with additional fields (`processed_at`, enhanced `confidence`)
- **Key**: Same as input

---

## Escalation Rules

### When to Ask for Help
1. **Interface Changes**: Need to modify Provider interface
2. **Schema Changes**: Need to modify Signal struct
3. **Cross-Provider Impact**: Change affects multiple providers
4. **Infrastructure Changes**: Docker Compose, Kafka topics
5. **Unclear Requirements**: Missing API documentation

### How to Document Blockers
Create a comment in code:
```go
// BLOCKED: Need API key for Trading Economics
// See: https://github.com/openclaworg/l1-ingestion/issues/XX
```

---

## Quick Reference

### Health Status Threshold
```go
const maxConsecutiveErrors = 5  // After 5 errors, status = Unhealthy
```

### Default Intervals
```go
GDELT:            15 minutes
FRED:             1 hour
Binance:          Real-time (WebSocket)
Whale Alert:      1 minute
CME COT:          24 hours (weekly data)
Trading Economics: 5 minutes
Telegram:         Real-time (long polling)
```

### Port Assignments
```
GDELT:            8081
FRED:             8082
Binance:          8083
Whale Alert:      8084
CME COT:          8085
Trading Economics: 8086
Telegram:         8087
```
