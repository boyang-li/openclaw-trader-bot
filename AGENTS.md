# AGENTS.md - Agent Workflow Guidelines

> **Purpose**: Coordination rules for AI agents working on L1-Ingestion
> **Last Updated**: 2026-02-04
> **Module**: l1-ingestion (Go sensors for ACC)

---

## Quick Start for New Agents

Before working on this codebase, read these critical files:
1. `CLAUDE.md` - Full project documentation and patterns
2. `internal/provider/provider.go` - Provider interface contract
3. `internal/signal/signal.go` - Signal schema (DO NOT MODIFY)
4. `docs/oracle_provider_research.md` - Low-cost provider candidates (useful when proposing new sensors)

### Current System Status
```
✅ GDELT      (8081) - Geopolitical events from public file archive
✅ Binance    (8083) - Crypto large trades via public WebSocket  
✅ COT        (8085) - Futures positioning from CFTC public files
✅ FRED       (8086) - Economic indicators (needs free API key)
✅ Telegram   (8087) - Channel/group message ingestion
✅ SLM Worker        - Signal enrichment via Qwen 2.5-1.5B-Instruct
✅ Alerter           - Telegram/Discord alerts (ACC L1 Signals + optional L2 Insights)
✅ Persister  (8088) - SQLite storage with Query API (snappy-enabled consumer)
✅ L2 Reasoner (8089) - Correlation/entity engine emitting `l2.insights`
✅ Orchestrator      - Unified provider lifecycle management with supervision
⏸️ Whale Alert      - Requires paid API key
⏸️ TradingEcon      - Requires paid API key
📋 L3 Planner       - Specified (docs/L2_L3_SPEC.md)
📋 L4 Paper Trading - Specified (docs/L4_SPEC.md)
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

**Responsibility**: Building and maintaining Python processing services

**Skills Required**:
- Python async patterns (asyncio, aiokafka)
- Hugging Face Transformers
- Kafka consumer/producer patterns
- Signal enrichment logic

**Typical Tasks**:
- Implement SLM processor enhancements
- Kafka consumer optimization
- Signal enrichment pipeline improvements
- Dead letter queue handling

**Before Starting Work**:
1. Read signal schema in `internal/signal/signal.go` (source of truth)
2. Review existing Python code in `python/slm_worker/` and `python/alerter/`
3. Understand input topic: `l1.signals.raw` → output: `l1.signals.enriched`

**Current Python Services**:
| Service | Location | Purpose |
|---------|----------|---------|
| SLM Worker | `python/slm_worker/` | Enriches signals with Qwen 2.5-1.5B model |
| Alerter | `python/alerter/` | Sends Telegram notifications for high-priority signals |
| Persister | `python/persister/` | Stores signals to SQLite with Query API |
| L2 Reasoner | `python/l2_reasoner/` | Correlates enriched signals → `l2.insights` |

**Python Code Patterns**:
```python
# Async Kafka consumer pattern (slm_worker/main.py)
async def consume_loop(consumer: AIOKafkaConsumer, processor):
    async for msg in consumer:
        signal = json.loads(msg.value.decode())
        enriched = await processor.enrich(signal)
        await producer.send_and_wait(OUTPUT_TOPIC, json.dumps(enriched).encode())

# Alert filtering pattern (alerter/filter.py)
def should_alert(signal: dict) -> bool:
    urgency = signal.get("enrichment", {}).get("urgency", "low")
    if urgency in ["high", "critical"]:
        return True
    sentiment_magnitude = abs(signal.get("enrichment", {}).get("sentiment", 0))
    if sentiment_magnitude >= 0.5:
        return True
    return False
```

---

### 🧬 Insight-Dev (L2 Reasoner + Alerting Integration)

**Responsibility**: Maintain the L2 correlation/entity engine and ensure L2 insights flow cleanly to downstream consumers (alerter, dashboards).

**Skills Required**:
- Async Python + aiokafka (multiple topic subscriptions)
- Time-series correlation, rule graphs, entity resolution
- SQLite/SQL for lightweight state + caching

**Typical Tasks**:
- Tune correlation windows / severity scoring
- Extend `engine/` rules or entity trackers
- Add new Grafana panels (e.g., `deploy/grafana/dashboards/l2-reasoner.json`)
- Ensure alerter templates cleanly distinguish ACC L1 Signal vs ACC L2 Insight

**Before Starting Work**:
1. Read `python/l2_reasoner/` (engine + storage packages)
2. Inspect `python/alerter/main.py` for dual-topic consumption + `AlertEnvelope`
3. Review env vars `L2_ALERTS_ENABLED`, `L2_INSIGHTS_TOPIC`, `L2_MIN_SEVERITY`
4. Tail the `l2.insights` topic via `rpk` before/after changes

---

### 🎛️ Orchestrator-Dev (System Integration Development)

**Responsibility**: Building and maintaining the orchestrator system for unified provider management

**Skills Required**:
- Go concurrency patterns (goroutines, channels, context)
- Circuit breaker patterns
- Rate limiting algorithms
- Health monitoring and supervision
- Kafka producer integration

**Typical Tasks**:
- Enhance orchestrator supervision logic
- Add new provider integrations
- Optimize rate limiting and circuit breaking
- Add monitoring and metrics
- Improve graceful shutdown handling

**Before Starting Work**:
1. Read `internal/orchestrator/orchestrator.go` (core implementation)
2. Read `internal/circuit/breaker.go` (circuit breaker pattern)
3. Read `internal/ratelimit/limiter.go` (rate limiting implementation)
4. Review `cmd/orchestrator/main.go` (entry point)

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

### Signal Pipeline (Current)
```
Go Providers (GDELT, Binance, COT, FRED, Telegram)
    │
    ▼
l1.signals.raw (Kafka topic)
    │
    ▼
SLM Worker (Python - Qwen 2.5-1.5B)
    │
    ▼
l1.signals.enriched (Kafka topic)
    │
    ├──────────────────────┬────────────────────┐
    ▼                      ▼                    ▼
Alerter                 Persister           L2 Reasoner (future)
(Telegram)              (SQLite)                │
    │                      │                    ▼
    ▼                      ▼              l2.insights, l2.situations
📱 User's Phone        💾 Query API (:8088)     │
                                                ▼
                                          L4 Paper Trading (future)
                                                │
                                                ▼
                                          l4.plans → L3 Executor
                                                │
                                                ▼
                                          l3.actions → L4 Evaluation
```

### Future Pipeline: L2 → L4 → L3 Loop

The full ACC architecture includes higher layers (specs in `docs/`):

| Layer | Purpose | Status |
|-------|---------|--------|
| L1 | Signal Ingestion | ✅ Implemented |
| L2 | Reasoning (correlations, entities, situations) | 📋 Specified |
| L3 | Decision/Execution (guardrails, paper executor, audit) | 📋 Specified |
| L4 | Paper Trading + Strategy Allocation (All-Weather) | 📋 Specified |

### Sensor-Dev → Processor-Dev
- **Contract**: Signals published to `l1.signals.raw` topic
- **Format**: JSON-encoded `signal.Signal`
- **Key**: `{source}:{subject}` (for partition affinity)

### Processor-Dev → Alerter
- **Contract**: Enriched signals to `l1.signals.enriched` topic
- **Format**: JSON with `enrichment` field containing:
  - `summary`: AI-generated summary
  - `sentiment`: -1.0 to 1.0
  - `urgency`: low/medium/high/critical
  - `market_impact`: neutral/positive/negative/highly_positive/highly_negative
  - `key_entities`: extracted entities
  - `processed_at`: timestamp
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

---

## Python Service Development Guidelines

### Project Structure
```
python/
├── slm_worker/           # SLM enrichment service
│   ├── __init__.py
│   ├── main.py          # Kafka consumer loop
│   ├── processor.py     # Model inference
│   ├── signal_schema.py # Signal dataclass (matches Go)
│   ├── config.py        # Environment configuration
│   ├── requirements.txt
│   └── Dockerfile
├── alerter/             # Notification service
│   ├── __init__.py
│   ├── main.py          # Kafka consumer loop
│   ├── filter.py        # Alert filtering rules
│   ├── notifier.py      # Telegram/Discord senders
│   ├── config.py        # Environment configuration
│   ├── requirements.txt
│   └── Dockerfile
└── shared/              # (future) Shared utilities
```

### Python Development Checklist

When implementing a new Python service:

#### Code Structure
- [ ] Create `python/<service>/` directory
- [ ] Create `__init__.py` with package info
- [ ] Create `main.py` with async Kafka consumer loop
- [ ] Create `config.py` using environment variables
- [ ] Create `requirements.txt` with pinned versions
- [ ] Create `Dockerfile` with health check

#### Kafka Consumer Pattern
```python
# Standard consumer loop pattern
async def main():
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await process_message(msg)
    finally:
        await consumer.stop()
```

#### Health Check Pattern
```python
# Health endpoint for Docker health check
from aiohttp import web

async def health_handler(request):
    return web.json_response({"status": "healthy"})

app = web.Application()
app.router.add_get('/health', health_handler)
```

#### Environment Variables
```bash
# Required for all Python services
KAFKA_BROKERS=redpanda:9092
INPUT_TOPIC=l1.signals.raw
OUTPUT_TOPIC=l1.signals.enriched
CONSUMER_GROUP=<service-name>

# SLM Worker specific
MODEL_NAME=Qwen/Qwen2.5-1.5B-Instruct
DEVICE=cpu
BATCH_SIZE=5

# Alerter specific
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=<from-botfather>
TELEGRAM_CHAT_ID=<your-chat-id>
MIN_URGENCY=high
MIN_SENTIMENT_MAGNITUDE=0.5
```

### Docker Integration

#### Dockerfile Pattern
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

CMD ["python", "-m", "<service>.main"]
```

#### docker-compose.yml Service Pattern
```yaml
<service-name>:
  build:
    context: ../python
    dockerfile: <service>/Dockerfile
  container_name: l1-<service-name>
  depends_on:
    redpanda:
      condition: service_healthy
  environment:
    - KAFKA_BROKERS=redpanda:9092
    - INPUT_TOPIC=l1.signals.enriched
    # ... other env vars
  restart: unless-stopped
  healthcheck:
    test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"]
    interval: 30s
    timeout: 10s
    retries: 3
  networks:
    - l1-network
```

### Testing Python Services

```bash
# Run service locally
cd python/<service>
pip install -r requirements.txt
python -m <service>.main

# Check logs in Docker
docker compose logs -f <service-name>

# Verify Kafka consumption
docker exec l1-redpanda rpk topic consume l1.signals.enriched --brokers localhost:9092 --num 5
```

### Common Python Pitfalls

#### ❌ DON'T: Block the event loop
```python
# WRONG - blocks async loop
time.sleep(5)
response = requests.get(url)
```

#### ✅ DO: Use async operations
```python
# CORRECT - non-blocking
await asyncio.sleep(5)
async with aiohttp.ClientSession() as session:
    response = await session.get(url)
```

#### ❌ DON'T: Ignore Kafka consumer errors
```python
# WRONG - silent failure
async for msg in consumer:
    process(msg)  # If this fails, message is lost
```

#### ✅ DO: Handle errors with retries/DLQ
```python
# CORRECT - error handling
async for msg in consumer:
    try:
        await process(msg)
    except Exception as e:
        logger.error(f"Failed to process: {e}")
        await send_to_dlq(msg)
```
