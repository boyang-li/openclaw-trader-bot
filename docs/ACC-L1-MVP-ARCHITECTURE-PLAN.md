# ACC-L1 Ingestion Layer: MVP Architecture Plan

> **Version**: 2.0  
> **Date**: 2026-02-04  
> **Status**: REVISED - Cost-Optimized Containerized Deployment

---

## Executive Summary

This plan delivers a **fully containerized, cost-efficient Go-based sensor architecture** for real-time signal ingestion. The system is designed to run 24/7 on a Mac Mini (or any Docker host) with zero vendor costs for the MVP phase.

### Key Changes from v1.0
| Aspect | v1.0 (Original) | v2.0 (Revised) |
|--------|-----------------|----------------|
| **Deployment** | Native binaries | Fully containerized (Docker Compose) |
| **Message Broker** | Apache Kafka + Zookeeper | Redpanda (Kafka-compatible, single binary) |
| **Analytics Storage** | Snowflake | SQLite/DuckDB (deferred to later phase) |
| **Alerting** | Not specified | Telegram Bot (real-time alerts) |
| **Cloud** | Confluent Cloud | Self-hosted (zero vendor cost) |
| **Target Host** | Unspecified | Mac Mini / Linux / Any Docker host |

### Technical Stack (MVP)
| Component | Technology | Cost |
|-----------|------------|------|
| **Ingestors** | Go (containerized) | $0 |
| **Message Broker** | Redpanda (self-hosted) | $0 |
| **Edge Processing** | Python SLM (Qwen 2.5-1.5B) + L2 Reasoner (correlation/entity engine) | $0 |
| **Persistence** | SQLite (Persister w/ snappy-enabled Kafka consumer) / DuckDB (later) | $0 |
| **Alerting** | Telegram Bot API (ACC L1 Signals + L2 Insights) | $0 |
| **Observability** | Prometheus + Grafana (includes L2 reasoner dashboard) | $0 |

### Data Sources (MVP)
| Category | Sources | Cost | Status |
|----------|---------|------|--------|
| **Geopolitical** | GDELT (GKG 2.0) | FREE | ✅ Running |
| **Macro** | FRED API, CME COT archives | FREE | ✅ Running |
| **Crypto** | Binance WebSocket (large trade monitor) | FREE | ✅ Running |
| **Community / Messaging** | Telegram channel/group ingestion | FREE (bot token) | ✅ Running |
| **Deferred** | Whale Alert, Trading Economics, paid news feeds | PAID | ⏸️ Planned |

---

## 1. Deployment Architecture

### Target Environment

```
┌─────────────────────────────────────────────────────────────────────┐
│                     DOCKER HOST (Mac Mini / Linux / Cloud VM)        │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                     Docker Compose Stack                      │   │
│  │                                                               │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │   │
│  │  │  Redpanda   │  │  Prometheus │  │      Grafana        │  │   │
│  │  │  (Broker)   │  │  (Metrics)  │  │   (Dashboards)      │  │   │
│  │  │  :9092      │  │  :9090      │  │   :3000             │  │   │
│  │  └──────┬──────┘  └─────────────┘  └─────────────────────┘  │   │
│  │         │                                                     │   │
│  │         ▼                                                     │   │
│  │  ┌─────────────────────────────────────────────────────────┐ │   │
│  │  │              Go Provider Containers                      │ │   │
│  │  │  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────────┐ │ │   │
│  │  │  │ GDELT │ │ FRED  │ │Binance│ │  COT  │ │ (future)  │ │ │   │
│  │  │  │ :8081 │ │ :8082 │ │ :8083 │ │ :8085 │ │           │ │ │   │
│  │  │  └───────┘ └───────┘ └───────┘ └───────┘ └───────────┘ │ │   │
│  │  └─────────────────────────────────────────────────────────┘ │   │
│  │         │                                                     │   │
│  │         ▼                                                     │   │
│  │  ┌─────────────────────────────────────────────────────────┐ │   │
│  │  │              Python Processing Layer                     │ │   │
│  │  │  ┌───────────────┐  ┌───────────────┐  ┌─────────────┐ │ │   │
│  │  │  │  SLM Worker   │  │   Alerter     │  │  Persister  │ │ │   │
│  │  │  │ (Qwen 2.5)    │  │  (Telegram)   │  │ (SQLite)    │ │ │   │
│  │  │  └───────────────┘  └───────────────┘  └─────────────┘ │ │   │
│  │  └─────────────────────────────────────────────────────────┘ │   │
│  │                                                               │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Volumes: redpanda-data, prometheus-data, grafana-data, sqlite-data │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
📱 Telegram Alerts
```

### Updated Signal Flow (2026)
1. **Go Providers** emit raw semantic signals to `l1.signals.raw`.
2. **Python SLM Worker** (Qwen 2.5) enriches each signal and publishes to `l1.signals.enriched`.
3. **Persister** consumes the enriched topic (snappy-enabled Kafka consumer) and writes to SQLite for historical queries.
4. **L2 Reasoner** ingests the same enriched stream, performs correlation/entity tracking, and emits derived insights to `l2.insights` (with planned `l2.situations` / `l2.entities`).
5. **Alerter** now consumes **both** `l1.signals.enriched` (high-urgency L1 events) and, when `L2_ALERTS_ENABLED=true`, `l2.insights`, formatting Telegram/Discord messages with a clear layer prefix ("ACC L1 Signal" vs "ACC L2 Insight").
6. **Grafana + Prometheus** monitor Redpanda, providers, SLM worker, L2 reasoner, and alert delivery health, including a dedicated L2 dashboard (`deploy/grafana/dashboards/l2-reasoner.json`).

## Recent Enhancements (2026-02)
- **L2 Reasoner Service** (`python/l2_reasoner`) now ships with Docker support, SQLite-backed state, and emits `l2.insights` that the alerter can forward.
- **Alerter** consumes both `l1.signals.enriched` and (optionally) `l2.insights`, tagging messages with "ACC L1 Signal" or "ACC L2 Insight" for clarity; new env vars `L2_ALERTS_ENABLED`, `L2_INSIGHTS_TOPIC`, `L2_MIN_SEVERITY` control behavior.
- **Persister** container includes `libsnappy` + `python-snappy`, allowing it to ingest compressed Kafka batches without crashes.
- **Grafana** now includes `deploy/grafana/dashboards/l2-reasoner.json` for monitoring L2 throughput, correlation windows, and insight generation.
- **docs/oracle_provider_research.md** captures Oracle’s shortlist of low-cost complementary data providers for future sensors.

### Portability

The same `docker-compose.yml` works on:
- **Mac Mini** (macOS with Docker Desktop)
- **Linux Server** (Ubuntu, Debian, etc.)
- **Windows** (WSL2 with Docker Desktop)
- **Cloud VM** (AWS EC2, GCP Compute Engine, etc.)

```bash
# Same command everywhere:
git clone <repo>
cd l1-ingestion
cp .env.example .env
# Edit .env with your config
docker compose up -d
```

---

## 2. Directory Structure (Revised)

```
l1-ingestion/
├── cmd/                           # Binary entry points
│   ├── gdelt/main.go
│   ├── fred/main.go
│   ├── binance/main.go
│   ├── cot/main.go
│   ├── whalealert/main.go         # Deferred (paid API)
│   ├── tradingeconomics/main.go   # Deferred (paid API)
│   └── telegram/main.go           # Ingestion (deferred)
│
├── internal/                      # Private packages
│   ├── provider/                  # Provider interface & implementations
│   ├── signal/                    # Signal domain model
│   ├── kafka/                     # Kafka/Redpanda producer
│   ├── config/                    # Configuration loading
│   ├── health/                    # HTTP health server
│   └── persistence/               # NEW: SQLite/DuckDB storage
│       ├── sqlite.go
│       └── dedup.go
│
├── python/                        # Python processing layer
│   ├── slm_worker/               # SLM enrichment
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── processor.py
│   │   └── config.py
│   ├── alerter/                  # ACC alerts (L1 signals + L2 insights)
│   │   ├── __init__.py
│   │   ├── main.py               # Kafka consumer (l1 + optional l2 topics)
│   │   ├── filter.py             # Alert filtering rules
│   │   ├── notifier.py           # Telegram/Discord templates w/ layer prefix
│   │   ├── config.py
│   │   └── requirements.txt
│   ├── persister/                # Signal persistence service
│   │   ├── __init__.py
│   │   ├── main.py               # Kafka consumer loop (snappy-enabled)
│   │   ├── database.py           # SQLite schema & inserts
│   │   ├── api.py                # HTTP query API (:8088)
│   │   ├── config.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   ├── l2_reasoner/              # NEW: Insight/correlation engine (L2)
│   │   ├── __init__.py
│   │   ├── main.py               # Ingest l1.signals.enriched → emit l2.insights/situations/entities
│   │   ├── engine/               # Correlation, entity tracker, rules
│   │   ├── storage/              # SQLite models/cache
│   │   ├── config.py
│   │   └── requirements.txt
│   ├── requirements.txt          # Shared Python deps (if any)
│   └── Dockerfile                # SLM worker Docker build
│
├── deploy/                        # Deployment configs
│   ├── docker/
│   │   ├── Dockerfile.provider   # Multi-stage Go build
│   │   ├── Dockerfile.python     # Python services
│   │   └── .dockerignore
│   ├── docker-compose.yml        # REVISED: Redpanda + all services
│   ├── docker-compose.dev.yml    # Development overrides
│   ├── docker-compose.prod.yml   # Production overrides
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       └── provisioning/
│
├── data/                          # Persistent data (gitignored)
│   ├── sqlite/                   # SQLite databases
│   └── redpanda/                 # Redpanda data
│
├── scripts/
│   ├── setup.sh                  # Initial setup
│   ├── backup.sh                 # Data backup
│   └── health-check.sh           # System health check
│
├── docs/
│   ├── ACC-L1-MVP-ARCHITECTURE-PLAN.md
│   ├── L2_L3_SPEC.md              # L2 Reasoning + L3 Decision specs
│   ├── L4_SPEC.md                 # L4 Paper Trading + Strategy Allocation
│   └── oracle_provider_research.md # Low-cost provider options (Oracle, 2026-02)
│
├── CLAUDE.md                      # AI assistant memory
├── AGENTS.md                      # Agent workflow guidelines
├── Makefile
├── go.mod
├── go.sum
└── .env.example
```

---

## 3. Revised Wave Execution Plan

### Wave Status Summary

| Wave | Description | Status |
|------|-------------|--------|
| Wave 1 | Foundation (Project structure, Go module, Signal schema) | ✅ COMPLETE |
| Wave 2 | Core Abstractions (Provider interface, Kafka producer) | ✅ COMPLETE |
| Wave 3 | Free Providers (GDELT, FRED, Binance, COT) | ✅ COMPLETE |
| Wave 3.5 | Containerization + Redpanda Migration | ✅ COMPLETE |
| Wave 4 | Python SLM Worker (Qwen 2.5-1.5B enrichment) | ✅ COMPLETE |
| Wave 4.5 | Telegram Alerter (real-time notifications) | ✅ COMPLETE |
| Wave 5 | Persistence Layer (SQLite storage + Query API) | ✅ COMPLETE |
| Wave 6 | Observability (Grafana dashboards) | ✅ COMPLETE |
| Wave 7 | FRED Provider | ✅ COMPLETE |
| Wave 7.5 | Telegram Ingestor | ✅ COMPLETE |
| Wave 8 | Orchestrator (unified provider management) | ✅ COMPLETE |
| Wave 8.5 | L2 Reasoner service + Grafana dashboard + alerter integration | ✅ COMPLETE |
| Wave 9 | Kubernetes manifests, CI/CD pipeline | ⏸️ DEFERRED |
| Wave 10 | Snowflake Integration | ⏸️ DEFERRED |

### Higher Layers (Specified, Not Implemented)

| Layer | Specification | Status |
|-------|---------------|--------|
| L2 Reasoning | `docs/L2_L3_SPEC.md` | ✅ Running (python/l2_reasoner) |
| L3 Decision/Execution | `docs/L2_L3_SPEC.md` | 📋 Specified |
| L4 Paper Trading | `docs/L4_SPEC.md` | 📋 Specified |

**L4 Highlights:**
- All-Weather risk parity as seed strategy
- Multi-asset: Binance tradables + FRED-priced synthetics (SP500, bonds, gold, oil)
- Conservative contextual bandit for strategy allocation
- Clean attribution: signal vs sizing vs execution
- Paper trading only (no real execution)

---

### Wave 3.5: Containerization + Redpanda Migration (NEW)

**Goal**: Make the system fully containerized and portable

#### Tasks

| # | Task | Effort | Priority |
|---|------|--------|----------|
| 3.5.1 | Create multi-stage Dockerfile for Go providers | 30 min | HIGH |
| 3.5.2 | Replace Kafka/Zookeeper with Redpanda in docker-compose | 30 min | HIGH |
| 3.5.3 | Add all Go provider services to docker-compose | 1 hr | HIGH |
| 3.5.4 | Create docker-compose.dev.yml for local development | 30 min | MEDIUM |
| 3.5.5 | Create docker-compose.prod.yml for production | 30 min | MEDIUM |
| 3.5.6 | Add health checks to all containers | 30 min | MEDIUM |
| 3.5.7 | Test full stack: `docker compose up` | 1 hr | HIGH |

#### Dockerfile.provider (Multi-stage)

```dockerfile
# Build stage
FROM golang:1.21-alpine AS builder
WORKDIR /app

# Install dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build binary
ARG PROVIDER
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /provider ./cmd/${PROVIDER}

# Runtime stage
FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /provider /provider
EXPOSE 8080
ENTRYPOINT ["/provider"]
```

#### docker-compose.yml (Revised)

```yaml
version: '3.8'

services:
  # ==========================================================================
  # Message Broker: Redpanda (Kafka-compatible, single binary, no Zookeeper)
  # ==========================================================================
  redpanda:
    image: redpandadata/redpanda:v23.3.5
    container_name: l1-redpanda
    command:
      - redpanda
      - start
      - --smp=1
      - --memory=1G
      - --reserve-memory=0M
      - --overprovisioned
      - --node-id=0
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://redpanda:9092
      - --pandaproxy-addr=0.0.0.0:8082
      - --advertise-pandaproxy-addr=redpanda:8082
    ports:
      - "9092:9092"
      - "8082:8082"   # REST Proxy
      - "9644:9644"   # Admin API
    volumes:
      - redpanda-data:/var/lib/redpanda/data
    healthcheck:
      test: ["CMD", "rpk", "cluster", "health"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - l1-network

  # ==========================================================================
  # Go Providers (Free APIs only for MVP)
  # ==========================================================================
  gdelt:
    build:
      context: .
      dockerfile: deploy/docker/Dockerfile.provider
      args:
        PROVIDER: gdelt
    container_name: l1-gdelt
    depends_on:
      redpanda:
        condition: service_healthy
    environment:
      - L1_KAFKA_BROKERS=redpanda:9092
      - L1_KAFKA_TOPIC_RAW=l1.signals.raw
      - L1_GDELT_ENABLED=true
      - L1_GDELT_POLL_INTERVAL=15m
    ports:
      - "8081:8080"
    restart: unless-stopped
    networks:
      - l1-network

  fred:
    build:
      context: .
      dockerfile: deploy/docker/Dockerfile.provider
      args:
        PROVIDER: fred
    container_name: l1-fred
    depends_on:
      redpanda:
        condition: service_healthy
    environment:
      - L1_KAFKA_BROKERS=redpanda:9092
      - L1_KAFKA_TOPIC_RAW=l1.signals.raw
      - L1_FRED_API_KEY=${FRED_API_KEY}
      - L1_FRED_POLL_INTERVAL=1h
    ports:
      - "8082:8080"
    restart: unless-stopped
    networks:
      - l1-network

  binance:
    build:
      context: .
      dockerfile: deploy/docker/Dockerfile.provider
      args:
        PROVIDER: binance
    container_name: l1-binance
    depends_on:
      redpanda:
        condition: service_healthy
    environment:
      - L1_KAFKA_BROKERS=redpanda:9092
      - L1_KAFKA_TOPIC_RAW=l1.signals.raw
      - L1_BINANCE_PAIRS=BTCUSDT,ETHUSDT
    ports:
      - "8083:8080"
    restart: unless-stopped
    networks:
      - l1-network

  cot:
    build:
      context: .
      dockerfile: deploy/docker/Dockerfile.provider
      args:
        PROVIDER: cot
    container_name: l1-cot
    depends_on:
      redpanda:
        condition: service_healthy
    environment:
      - L1_KAFKA_BROKERS=redpanda:9092
      - L1_KAFKA_TOPIC_RAW=l1.signals.raw
      - L1_COT_ENABLED=true
    ports:
      - "8085:8080"
    restart: unless-stopped
    networks:
      - l1-network

  # ==========================================================================
  # Python Processing Layer
  # ==========================================================================
  slm-worker:
    build:
      context: ./python
      dockerfile: Dockerfile
      target: slm-worker
    container_name: l1-slm-worker
    depends_on:
      redpanda:
        condition: service_healthy
    environment:
      - KAFKA_BROKERS=redpanda:9092
      - INPUT_TOPIC=l1.signals.raw
      - OUTPUT_TOPIC=l1.signals.enriched
      - MODEL_NAME=Qwen/Qwen2.5-1.5B-Instruct
    volumes:
      - huggingface-cache:/root/.cache/huggingface
    restart: unless-stopped
    networks:
      - l1-network

  telegram-alerter:
    build:
      context: ./python
      dockerfile: Dockerfile
      target: alerter
    container_name: l1-telegram-alerter
    depends_on:
      redpanda:
        condition: service_healthy
    environment:
      - KAFKA_BROKERS=redpanda:9092
      - INPUT_TOPIC=l1.signals.enriched
      - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
      - TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
      - ALERT_URGENCY_THRESHOLD=high
    restart: unless-stopped
    networks:
      - l1-network

  # ==========================================================================
  # Observability
  # ==========================================================================
  prometheus:
    image: prom/prometheus:v2.47.0
    container_name: l1-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./deploy/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.retention.time=15d'
    networks:
      - l1-network

  grafana:
    image: grafana/grafana:10.2.0
    container_name: l1-grafana
    depends_on:
      - prometheus
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD:-admin}
    volumes:
      - grafana-data:/var/lib/grafana
      - ./deploy/grafana/provisioning:/etc/grafana/provisioning:ro
    networks:
      - l1-network

networks:
  l1-network:
    driver: bridge

volumes:
  redpanda-data:
  prometheus-data:
  grafana-data:
  huggingface-cache:
```

---

### Wave 4: Python SLM Worker

**Goal**: Semantic enrichment of signals using local SLM

| # | Task | Effort |
|---|------|--------|
| 4.1 | Create Python project structure | 30 min |
| 4.2 | Implement Kafka consumer (aiokafka) | 1 hr |
| 4.3 | Implement SLM processor (Qwen 2.5) | 2-3 hr |
| 4.4 | Add DLQ (dead letter queue) handling | 30 min |
| 4.5 | Create Dockerfile | 30 min |
| 4.6 | Integration test with Go providers | 1 hr |

---

### Wave 4.5: Telegram Alerter (NEW)

**Goal**: Real-time alerts to user via Telegram

| # | Task | Effort |
|---|------|--------|
| 4.5.1 | Create Telegram Bot (via BotFather) | 15 min |
| 4.5.2 | Implement Kafka consumer for enriched signals | 1 hr |
| 4.5.3 | Implement alert filtering (urgency, keywords) | 1 hr |
| 4.5.4 | Implement Telegram message formatter | 30 min |
| 4.5.5 | Add rate limiting (avoid Telegram spam) | 30 min |
| 4.5.6 | Add deduplication (prevent duplicate alerts) | 30 min |

#### Telegram Alerter Design

```python
# python/alerter/telegram_bot.py

class TelegramAlerter:
    """Sends filtered signals to Telegram."""
    
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.seen_ids = set()  # Deduplication
        self.rate_limiter = RateLimiter(max_per_minute=20)
    
    async def should_alert(self, signal: dict) -> bool:
        """Filter: only alert on high urgency or matching keywords."""
        if signal['id'] in self.seen_ids:
            return False
        if signal.get('urgency') in ['high', 'critical']:
            return True
        # Add keyword matching, sentiment thresholds, etc.
        return False
    
    async def send_alert(self, signal: dict):
        """Format and send alert to Telegram."""
        message = self.format_message(signal)
        await self.rate_limiter.acquire()
        # Send via Telegram Bot API
        self.seen_ids.add(signal['id'])
    
    def format_message(self, signal: dict) -> str:
        """Format signal as Telegram message."""
        emoji = {'critical': '🚨', 'high': '⚠️', 'medium': 'ℹ️', 'low': '📝'}
        return f"""
{emoji.get(signal['urgency'], '📢')} **{signal['category'].upper()}**

**{signal['subject']}** → {signal['action']}
{f"Target: {signal['object']}" if signal.get('object') else ""}

Confidence: {signal['confidence']:.0%}
Sentiment: {signal['sentiment']:+.2f}
Source: {signal['source']}
Time: {signal['timestamp']}
        """.strip()
```

---

### Wave 5: Persistence Layer (SQLite/DuckDB)

**Goal**: Store signals for audit, replay, and analytics

| # | Task | Effort |
|---|------|--------|
| 5.1 | Design SQLite schema | 30 min |
| 5.2 | Implement persistence service (Go or Python) | 2 hr |
| 5.3 | Add deduplication table | 30 min |
| 5.4 | Add simple query API | 1 hr |
| 5.5 | Add backup script | 30 min |

---

### Wave 6: Observability (Grafana Dashboards) ✅ COMPLETE

**Goal**: Visual monitoring of system health and signal flow

#### Tasks Completed

| # | Task | Status |
|---|------|--------|
| 6.1 | Create L1 Overview Dashboard | ✅ DONE |
| 6.2 | Create Redpanda/Kafka Dashboard | ✅ DONE |
| 6.3 | Create Provider Health Dashboard | ✅ DONE |
| 6.4 | Fix docker-compose volume mounts | ✅ DONE |
| 6.5 | Set explicit Prometheus datasource UID | ✅ DONE |

#### Grafana Dashboards

| Dashboard | UID | Panels | Description |
|-----------|-----|--------|-------------|
| **L1 Overview** | `l1-overview` | 13 | Service health, signal rates, total signals, memory/CPU |
| **Redpanda Metrics** | `redpanda` | 14 | Topic throughput, consumer lag, request latency, cluster health |
| **Provider Health** | `providers` | 20 | Per-provider memory, CPU, goroutines, comparative views |

#### Dashboard Access

```bash
# Open Grafana
open http://localhost:3000
# Login: admin / admin

# Direct dashboard URLs:
# - Overview:  http://localhost:3000/d/l1-overview
# - Redpanda:  http://localhost:3000/d/redpanda
# - Providers: http://localhost:3000/d/providers
```

#### Key Metrics Visualized

**L1 Overview Dashboard:**
- Service health status (UP/DOWN) for all providers
- Signal production rate (per second)
- Total raw and enriched signals
- Topic message counts
- Provider memory and CPU usage

**Redpanda Dashboard:**
- Cluster health (brokers, topics, partitions)
- Records produced/fetched per topic
- Consumer group committed offsets
- Consumer lag by group
- Request throughput and latency (p50, p99)
- Redpanda CPU usage

**Provider Health Dashboard:**
- Per-provider status, goroutines, memory
- Individual provider memory/goroutine graphs
- Comparative views across all providers
- Open file descriptors

#### Files Created

```
deploy/grafana/
├── dashboards/
│   ├── l1-overview.json    # System overview
│   ├── redpanda.json       # Kafka/Redpanda metrics
│   └── providers.json      # Provider health
└── provisioning/
    ├── dashboards/
    │   └── dashboards.yml  # Dashboard provisioning config
    └── datasources/
        └── datasources.yml # Prometheus datasource (uid: prometheus)

#### SQLite Schema

```sql
-- signals.db

CREATE TABLE signals (
    id TEXT PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    ingested_at DATETIME NOT NULL,
    processed_at DATETIME,
    
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    
    subject TEXT NOT NULL,
    action TEXT NOT NULL,
    object TEXT,
    
    confidence REAL,
    sentiment REAL,
    urgency TEXT,
    
    tags TEXT,  -- JSON array
    raw_data TEXT,  -- JSON
    metadata TEXT,  -- JSON
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_signals_timestamp ON signals(timestamp);
CREATE INDEX idx_signals_source ON signals(source);
CREATE INDEX idx_signals_category ON signals(category);
CREATE INDEX idx_signals_urgency ON signals(urgency);

-- Deduplication tracking
CREATE TABLE processed_ids (
    signal_id TEXT PRIMARY KEY,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

---

## 4. Provider API Costs & Priority

### Free APIs (MVP Phase)

| Provider | API | Rate Limit | Notes |
|----------|-----|------------|-------|
| **GDELT** | HTTP | ~1 req/sec | Public data, no key needed |
| **Binance** | WebSocket | N/A | Public market data |
| **CME COT** | HTTP | Weekly | Public CFTC data |
| **FRED** | HTTP | 120 req/min | Free API key required |
| **Telegram Bot** | HTTP | 30 msg/sec | Free, sending only |

### Paid APIs (Deferred)

| Provider | Cost | When to Enable |
|----------|------|----------------|
| **Whale Alert** | $9-99/mo | When crypto alerts needed |
| **Trading Economics** | $50+/mo | When macro calendar needed |
| **Telegram Scraping** | Complex | When channel monitoring needed |

---

## 5. Cost Summary

### MVP Phase (Immediate)

| Item | Monthly Cost |
|------|--------------|
| Docker hosting (Mac Mini) | $0 (already owned) |
| Redpanda (self-hosted) | $0 |
| Free APIs (GDELT, Binance, COT, FRED) | $0 |
| Telegram Bot | $0 |
| Prometheus + Grafana | $0 |
| **Total** | **$0** |

### Growth Phase (Later)

| Item | Monthly Cost |
|------|--------------|
| Cloud VM (if needed) | $10-60 |
| Whale Alert API | $9-99 |
| Trading Economics | $50+ |
| Snowflake (if needed) | Variable |
| **Total** | **$70-200+** |

---

## 6. Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Home power outage | Missed signals | UPS for Mac Mini + router |
| Home ISP downtime | Missed signals | Local queue buffer, auto-restart |
| API rate limits | Blocked requests | Conservative polling, backoff |
| Telegram spam | User annoyance | Rate limiting, dedup, filtering |
| SLM latency | Delayed alerts | Batch processing, GPU if needed |
| Docker resource usage | System slowdown | Resource limits in compose |

---

## 7. Success Criteria

### MVP Complete When:

1. ✅ `docker compose up` starts entire stack — **DONE** (2026-02-03)
2. ✅ 3 free providers collecting signals (GDELT, Binance, COT) — **DONE** (FRED needs API key)
3. ✅ Signals flowing through Redpanda topics — **DONE**
4. ✅ SLM worker enriching signals — **DONE** (Qwen 2.5-1.5B-Instruct)
5. ✅ Telegram alerts arriving for high-urgency signals — **DONE** (Live on user's phone!)
6. ✅ Grafana dashboard showing system health — **DONE** (3 dashboards: Overview, Redpanda, Providers)
7. ⏳ System runs 24/7 for 1 week without manual intervention — **IN PROGRESS**

### L1 MVP Complete — Next: L2/L3/L4 Implementation

With L1 ingestion operational, the next phase is implementing higher layers:

1. **L2 Reasoning** (`docs/L2_L3_SPEC.md`): Correlations, entity tracking, situation rollups
2. **L3 Decision** (`docs/L2_L3_SPEC.md`): Guardrails, paper executor, audit trail
3. **L4 Paper Trading** (`docs/L4_SPEC.md`): All-Weather risk parity, contextual bandit

---

## 8. Commands Reference

```bash
# Start entire stack
docker compose up -d

# View logs
docker compose logs -f

# View specific service logs
docker compose logs -f gdelt

# Stop stack
docker compose down

# Rebuild after code changes
docker compose build gdelt
docker compose up -d gdelt

# Check Redpanda topics
docker exec l1-redpanda rpk topic list

# Consume messages (debug)
docker exec l1-redpanda rpk topic consume l1.signals.raw

# Backup SQLite
docker cp l1-persister:/data/signals.db ./backup/

# System health
./scripts/health-check.sh
```
