# ACC-L1 Ingestion Layer: MVP Architecture Plan

> **Version**: 1.0  
> **Date**: 2026-02-02  
> **Status**: APPROVED FOR IMPLEMENTATION

---

## Executive Summary

This plan delivers a **production-grade Go-based sensor architecture** for real-time signal ingestion from geopolitical, macroeconomic, and crypto sources. The system follows antifragile design principles with graceful degradation, circuit breakers, and semantic compression via edge SLM processing.

### Technical Stack
| Component | Technology |
|-----------|------------|
| **Ingestors** | Go (high-concurrency native sensors) |
| **Message Broker** | Apache Kafka |
| **Edge Processing** | Python SLM (Qwen 2.5 / Phi-3-mini) |
| **Analytics Storage** | Snowflake (via Kafka Connector) |
| **Transformations** | dbt |

### Data Sources (MVP)
| Category | Sources |
|----------|---------|
| **Geopolitical** | GDELT (GKG 2.0), Telegram |
| **Macro** | FRED, Trading Economics, CME COT |
| **Crypto** | Whale Alert, Binance WebSocket |

---

## 1. Directory Structure

```
l1-ingestion/
├── cmd/                           # Binary entry points
│   ├── sensor-gdelt/             # GDELT GKG 2.0 sensor
│   │   └── main.go
│   ├── sensor-telegram/          # Telegram scraper/API sensor
│   │   └── main.go
│   ├── sensor-fred/              # FRED economic data sensor
│   │   └── main.go
│   ├── sensor-tradingeconomics/  # Trading Economics calendar
│   │   └── main.go
│   ├── sensor-whalealert/        # Whale Alert sensor
│   │   └── main.go
│   ├── sensor-binance/           # Binance WebSocket sensor
│   │   └── main.go
│   ├── sensor-cot/               # CME COT Reports sensor
│   │   └── main.go
│   └── orchestrator/             # Health monitor & orchestrator
│       └── main.go
│
├── internal/                      # Private packages
│   ├── provider/                 # Provider interface & base impl
│   │   ├── provider.go           # Core interface
│   │   ├── base.go               # Base provider with common logic
│   │   ├── config.go             # Provider configuration
│   │   └── registry.go           # Provider registry
│   │
│   ├── providers/                # Concrete provider implementations
│   │   ├── gdelt/
│   │   │   ├── client.go         # GDELT HTTP client
│   │   │   ├── parser.go         # GKG 2.0 parser
│   │   │   └── provider.go       # Provider implementation
│   │   ├── telegram/
│   │   ├── fred/
│   │   ├── tradingeconomics/
│   │   ├── whalealert/
│   │   ├── binance/
│   │   └── cot/
│   │
│   ├── kafka/                    # Kafka producer abstraction
│   │   ├── producer.go           # Async producer with batching
│   │   ├── config.go             # Kafka configuration
│   │   └── metrics.go            # Producer metrics
│   │
│   ├── signal/                   # Signal domain model
│   │   ├── signal.go             # Signal struct & methods
│   │   ├── validation.go         # Schema validation
│   │   └── serialization.go      # JSON/Avro serialization
│   │
│   ├── ratelimit/                # Rate limiting utilities
│   │   ├── limiter.go            # Token bucket implementation
│   │   └── backoff.go            # Exponential backoff
│   │
│   ├── circuit/                  # Circuit breaker pattern
│   │   └── breaker.go
│   │
│   ├── config/                   # Configuration loading
│   │   ├── loader.go             # YAML/env config loader
│   │   └── types.go              # Config structs
│   │
│   └── metrics/                  # Prometheus metrics
│       └── metrics.go
│
├── pkg/                          # Public packages (if needed)
│   └── signal/                   # Signal types for external use
│       └── types.go
│
├── python/                       # Python SLM worker
│   ├── slm_worker/
│   │   ├── __init__.py
│   │   ├── main.py               # Kafka consumer entrypoint
│   │   ├── processor.py          # SLM inference logic
│   │   ├── models.py             # Pydantic signal models
│   │   └── config.py             # Configuration
│   ├── requirements.txt
│   └── Dockerfile
│
├── configs/                      # Configuration files
│   ├── base.yaml                 # Base configuration
│   ├── development.yaml          # Dev overrides
│   ├── production.yaml           # Prod overrides
│   └── providers/                # Per-provider configs
│       ├── gdelt.yaml
│       ├── binance.yaml
│       └── ...
│
├── deployments/                  # Deployment configs
│   ├── docker/
│   │   ├── Dockerfile.sensor     # Multi-stage Go build
│   │   ├── Dockerfile.slm        # Python SLM worker
│   │   └── docker-compose.yaml   # Local dev environment
│   ├── kubernetes/
│   │   ├── sensors/              # Sensor deployments
│   │   ├── slm-worker/           # SLM worker deployment
│   │   └── kafka-connector/      # Snowflake connector config
│   └── terraform/                # Infrastructure as code
│
├── scripts/                      # Build & utility scripts
│   ├── build.sh                  # Build all binaries
│   ├── dev-setup.sh              # Local dev environment setup
│   └── generate-mocks.sh         # Generate test mocks
│
├── dbt/                          # dbt transformation project
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/              # Raw signal staging
│   │   ├── intermediate/         # Cleaned signals
│   │   └── marts/                # Analytics-ready tables
│   └── macros/
│
├── schemas/                      # JSON schemas
│   └── signal.v1.json
│
├── go.mod
├── go.sum
├── Makefile                      # Build automation
└── README.md
```

---

## 2. Interface Design

### Core Provider Interface (`internal/provider/provider.go`)

```go
package provider

import (
    "context"
    "time"
    
    "github.com/openclaw/l1-ingestion/internal/signal"
)

// Provider defines the contract for all data source sensors.
// Follows the antifragile principle: each provider is independent,
// can fail without affecting others, and recovers gracefully.
type Provider interface {
    // Lifecycle management
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    
    // Health & observability
    Health() HealthStatus
    Metrics() ProviderMetrics
    
    // Identity
    Name() string
    Category() SignalCategory  // Geopolitical, Macro, Crypto
    
    // Signal channel - providers push signals here
    Signals() <-chan *signal.Signal
    
    // Errors channel - non-fatal errors for monitoring
    Errors() <-chan error
}

// HealthStatus represents provider operational state
type HealthStatus struct {
    Status      Status    `json:"status"`       // Healthy, Degraded, Unhealthy
    LastCheck   time.Time `json:"last_check"`
    LastSuccess time.Time `json:"last_success"`
    LastError   string    `json:"last_error,omitempty"`
    Details     map[string]interface{} `json:"details,omitempty"`
}

type Status string

const (
    StatusHealthy   Status = "healthy"
    StatusDegraded  Status = "degraded"   // Working with limitations
    StatusUnhealthy Status = "unhealthy"
)

// ProviderMetrics for Prometheus export
type ProviderMetrics struct {
    SignalsEmitted     int64         `json:"signals_emitted"`
    SignalsFailed      int64         `json:"signals_failed"`
    LastLatency        time.Duration `json:"last_latency"`
    AvgLatency         time.Duration `json:"avg_latency"`
    RateLimitHits      int64         `json:"rate_limit_hits"`
    CircuitBreakerOpen bool          `json:"circuit_breaker_open"`
}

// SignalCategory for routing and filtering
type SignalCategory string

const (
    CategoryGeopolitical SignalCategory = "geopolitical"
    CategoryMacro        SignalCategory = "macro"
    CategoryCrypto       SignalCategory = "crypto"
)

// ProviderConfig common configuration for all providers
type ProviderConfig struct {
    Enabled        bool                 `yaml:"enabled"`
    PollInterval   time.Duration        `yaml:"poll_interval"`
    RateLimit      RateLimitConfig      `yaml:"rate_limit"`
    CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
    RetryPolicy    RetryConfig          `yaml:"retry"`
}

type RateLimitConfig struct {
    RequestsPerSecond float64 `yaml:"requests_per_second"`
    Burst             int     `yaml:"burst"`
}

type CircuitBreakerConfig struct {
    Threshold   int           `yaml:"threshold"`    // Failures before open
    Timeout     time.Duration `yaml:"timeout"`      // Time before half-open
    MaxRequests int           `yaml:"max_requests"` // Requests in half-open
}

type RetryConfig struct {
    MaxAttempts int           `yaml:"max_attempts"`
    InitialWait time.Duration `yaml:"initial_wait"`
    MaxWait     time.Duration `yaml:"max_wait"`
    Multiplier  float64       `yaml:"multiplier"`
}
```

### Base Provider Implementation (`internal/provider/base.go`)

```go
package provider

import (
    "context"
    "sync"
    "time"
    
    "github.com/openclaw/l1-ingestion/internal/circuit"
    "github.com/openclaw/l1-ingestion/internal/ratelimit"
    "github.com/openclaw/l1-ingestion/internal/signal"
    "github.com/rs/zerolog"
)

// BaseProvider provides common functionality for all providers.
// Concrete providers embed this and implement Fetch().
type BaseProvider struct {
    name     string
    category SignalCategory
    config   ProviderConfig
    
    signals  chan *signal.Signal
    errors   chan error
    
    limiter  *ratelimit.Limiter
    breaker  *circuit.Breaker
    
    health   HealthStatus
    metrics  ProviderMetrics
    mu       sync.RWMutex
    
    logger   zerolog.Logger
    
    cancel   context.CancelFunc
    wg       sync.WaitGroup
}

// NewBaseProvider creates a new base provider with common setup
func NewBaseProvider(name string, category SignalCategory, config ProviderConfig, logger zerolog.Logger) *BaseProvider {
    return &BaseProvider{
        name:     name,
        category: category,
        config:   config,
        signals:  make(chan *signal.Signal, 1000),  // Buffered for backpressure
        errors:   make(chan error, 100),
        limiter:  ratelimit.New(config.RateLimit.RequestsPerSecond, config.RateLimit.Burst),
        breaker:  circuit.New(config.CircuitBreaker),
        logger:   logger.With().Str("provider", name).Logger(),
    }
}

// Emit sends a signal with rate limiting and circuit breaker protection
func (b *BaseProvider) Emit(ctx context.Context, sig *signal.Signal) error {
    // Wait for rate limit
    if err := b.limiter.Wait(ctx); err != nil {
        b.mu.Lock()
        b.metrics.RateLimitHits++
        b.mu.Unlock()
        return err
    }
    
    // Check circuit breaker
    if !b.breaker.Allow() {
        b.mu.Lock()
        b.metrics.CircuitBreakerOpen = true
        b.mu.Unlock()
        return ErrCircuitOpen
    }
    
    select {
    case b.signals <- sig:
        b.mu.Lock()
        b.metrics.SignalsEmitted++
        b.mu.Unlock()
        b.breaker.Success()
        return nil
    case <-ctx.Done():
        return ctx.Err()
    default:
        // Channel full - backpressure
        b.mu.Lock()
        b.metrics.SignalsFailed++
        b.mu.Unlock()
        return ErrBackpressure
    }
}

// Name returns the provider name
func (b *BaseProvider) Name() string {
    return b.name
}

// Category returns the signal category
func (b *BaseProvider) Category() SignalCategory {
    return b.category
}

// Signals returns the signal channel
func (b *BaseProvider) Signals() <-chan *signal.Signal {
    return b.signals
}

// Errors returns the error channel
func (b *BaseProvider) Errors() <-chan error {
    return b.errors
}

// Health returns current health status
func (b *BaseProvider) Health() HealthStatus {
    b.mu.RLock()
    defer b.mu.RUnlock()
    return b.health
}

// Metrics returns current metrics
func (b *BaseProvider) Metrics() ProviderMetrics {
    b.mu.RLock()
    defer b.mu.RUnlock()
    return b.metrics
}
```

---

## 3. Signal Metadata Schema

### Go Definition (`internal/signal/signal.go`)

```go
package signal

import (
    "encoding/json"
    "time"
    
    "github.com/google/uuid"
)

// Signal is the canonical data structure for all ingested events.
// Designed for semantic queryability: WHO did WHAT to WHOM/WHAT.
type Signal struct {
    // Core identity
    ID      string `json:"id"`      // UUID v7 (time-ordered)
    Version string `json:"version"` // Schema version "1.0"
    
    // Semantic triple (Subject-Action-Object)
    Subject string `json:"subject"` // WHO: Entity performing action
    Action  string `json:"action"`  // WHAT: The action/event type
    Object  string `json:"object"`  // TO WHOM/WHAT: Target entity
    
    // Confidence & scoring
    Confidence float64 `json:"confidence"` // 0.0-1.0, set by SLM
    Sentiment  float64 `json:"sentiment"`  // -1.0 to 1.0, optional
    Urgency    int     `json:"urgency"`    // 1-5 scale
    
    // Source provenance
    Source    string `json:"source"`              // Provider name (e.g., "gdelt")
    SourceID  string `json:"source_id"`           // Original record ID
    SourceURL string `json:"source_url,omitempty"` // Reference URL
    
    // Timestamps
    Timestamp   time.Time  `json:"timestamp"`             // Event time
    IngestedAt  time.Time  `json:"ingested_at"`           // Ingestion time
    ProcessedAt *time.Time `json:"processed_at,omitempty"` // SLM processing time
    
    // Classification
    Category    string   `json:"category"`              // geopolitical, macro, crypto
    Subcategory string   `json:"subcategory,omitempty"` // e.g., "conflict", "rate_decision"
    Tags        []string `json:"tags,omitempty"`
    
    // Entities extracted
    Entities []Entity `json:"entities,omitempty"`
    
    // Original payload (for audit & reprocessing)
    RawData json.RawMessage `json:"raw_data"`
    
    // Provider-specific metadata
    Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// Entity represents an extracted named entity
type Entity struct {
    Name string `json:"name"`
    Type string `json:"type"` // PERSON, ORG, LOCATION, ASSET, etc.
    Role string `json:"role"` // subject, object, mentioned
}

// NewSignal creates a new signal with auto-generated ID and timestamps
func NewSignal(source, category string) *Signal {
    return &Signal{
        ID:         uuid.Must(uuid.NewV7()).String(),
        Version:    "1.0",
        Source:     source,
        Category:   category,
        Timestamp:  time.Now().UTC(),
        IngestedAt: time.Now().UTC(),
        Metadata:   make(map[string]interface{}),
    }
}

// Validate checks if the signal has all required fields
func (s *Signal) Validate() error {
    if s.ID == "" {
        return ErrMissingID
    }
    if s.Subject == "" {
        return ErrMissingSubject
    }
    if s.Action == "" {
        return ErrMissingAction
    }
    if s.Source == "" {
        return ErrMissingSource
    }
    if s.Category == "" {
        return ErrMissingCategory
    }
    return nil
}
```

### JSON Schema (`schemas/signal.v1.json`)

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://acc.openclaw.md/schemas/signal/v1.0",
  "title": "ACC Signal",
  "description": "Canonical signal format for ACC L1 Ingestion Layer",
  "type": "object",
  "required": ["id", "version", "subject", "action", "source", "timestamp", "category"],
  "properties": {
    "id": {
      "type": "string",
      "format": "uuid",
      "description": "UUID v7 (time-ordered)"
    },
    "version": {
      "type": "string",
      "const": "1.0"
    },
    "subject": {
      "type": "string",
      "minLength": 1,
      "description": "WHO: Entity performing action"
    },
    "action": {
      "type": "string",
      "minLength": 1,
      "description": "WHAT: The action/event type"
    },
    "object": {
      "type": "string",
      "description": "TO WHOM/WHAT: Target entity"
    },
    "confidence": {
      "type": "number",
      "minimum": 0,
      "maximum": 1,
      "description": "Confidence score from SLM (0.0-1.0)"
    },
    "sentiment": {
      "type": "number",
      "minimum": -1,
      "maximum": 1,
      "description": "Sentiment score (-1.0 negative to 1.0 positive)"
    },
    "urgency": {
      "type": "integer",
      "minimum": 1,
      "maximum": 5,
      "description": "Urgency level (1=low to 5=critical)"
    },
    "source": {
      "type": "string",
      "description": "Provider name (e.g., gdelt, binance)"
    },
    "source_id": {
      "type": "string",
      "description": "Original record ID from source"
    },
    "source_url": {
      "type": "string",
      "format": "uri",
      "description": "Reference URL"
    },
    "timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "Event timestamp (ISO 8601)"
    },
    "ingested_at": {
      "type": "string",
      "format": "date-time",
      "description": "Ingestion timestamp"
    },
    "processed_at": {
      "type": ["string", "null"],
      "format": "date-time",
      "description": "SLM processing timestamp"
    },
    "category": {
      "type": "string",
      "enum": ["geopolitical", "macro", "crypto"],
      "description": "Signal category"
    },
    "subcategory": {
      "type": "string",
      "description": "Signal subcategory"
    },
    "tags": {
      "type": "array",
      "items": { "type": "string" },
      "description": "Classification tags"
    },
    "entities": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["name", "type"],
        "properties": {
          "name": { "type": "string" },
          "type": { "type": "string" },
          "role": { "type": "string" }
        }
      },
      "description": "Extracted named entities"
    },
    "raw_data": {
      "type": "object",
      "description": "Original payload for audit"
    },
    "metadata": {
      "type": "object",
      "description": "Provider-specific metadata"
    }
  }
}
```

---

## 4. SLM Integration Architecture

### Data Flow

```
┌──────────────┐     ┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
│  Go Sensors  │────▶│  raw-signals    │────▶│  Python SLM      │────▶│ enriched-   │
│  (Providers) │     │  (Kafka Topic)  │     │  Worker          │     │ signals     │
└──────────────┘     └─────────────────┘     └──────────────────┘     └─────────────┘
                                                     │
                                                     ▼
                                             ┌──────────────┐
                                             │ Local SLM    │
                                             │ (Qwen 2.5 /  │
                                             │  Phi-3-mini) │
                                             └──────────────┘
```

### Python SLM Worker (`python/slm_worker/main.py`)

```python
"""SLM Worker - Semantic compression for ACC signals."""

import asyncio
import json
from typing import AsyncGenerator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel
from loguru import logger

from .processor import SLMProcessor
from .config import Settings


class SignalEnricher:
    """Consumes raw signals, enriches via SLM, publishes enriched signals."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.processor = SLMProcessor(settings.model_name)
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None
        
    async def start(self) -> None:
        """Initialize Kafka connections and SLM model."""
        logger.info("Starting SLM worker...")
        
        # Load SLM model
        await self.processor.load_model()
        
        # Initialize Kafka consumer
        self.consumer = AIOKafkaConsumer(
            self.settings.input_topic,
            bootstrap_servers=self.settings.kafka_brokers,
            group_id=self.settings.consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        
        # Initialize Kafka producer
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka_brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        
        await self.consumer.start()
        await self.producer.start()
        logger.info("SLM worker started")
        
    async def stop(self) -> None:
        """Graceful shutdown."""
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        logger.info("SLM worker stopped")
        
    async def process_signals(self) -> None:
        """Main processing loop."""
        async for msg in self.consumer:
            try:
                signal = json.loads(msg.value.decode("utf-8"))
                
                # Skip if already processed
                if signal.get("processed_at"):
                    continue
                
                # Enrich via SLM
                enriched = await self.processor.enrich(signal)
                
                # Publish to enriched topic
                await self.producer.send_and_wait(
                    self.settings.output_topic,
                    value=enriched,
                    key=enriched["id"].encode("utf-8"),
                )
                
                # Commit offset
                await self.consumer.commit()
                
                logger.debug(f"Processed signal {enriched['id']}")
                
            except Exception as e:
                logger.error(f"Failed to process signal: {e}")
                # Send to dead letter queue
                await self._send_to_dlq(msg, str(e))
                
    async def _send_to_dlq(self, msg, error: str) -> None:
        """Send failed message to dead letter queue."""
        dlq_msg = {
            "original": msg.value.decode("utf-8"),
            "error": error,
            "topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
        }
        await self.producer.send_and_wait(
            f"{self.settings.input_topic}.dlq",
            value=dlq_msg,
        )


async def main() -> None:
    settings = Settings()
    enricher = SignalEnricher(settings)
    
    await enricher.start()
    
    try:
        await enricher.process_signals()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await enricher.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

### SLM Processor (`python/slm_worker/processor.py`)

```python
"""SLM inference logic for semantic extraction."""

from datetime import datetime, timezone
from typing import Any

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from loguru import logger


class SLMProcessor:
    """Processes signals through local SLM for semantic enrichment."""
    
    SYSTEM_PROMPT = """You are a signal analyzer. Extract the following from the text:
1. Subject: WHO is performing the action (entity name)
2. Action: WHAT action/event occurred (verb phrase)
3. Object: TO WHOM/WHAT is the action directed (entity name or "N/A")
4. Confidence: How certain are you (0.0-1.0)
5. Sentiment: Overall sentiment (-1.0 negative to 1.0 positive)
6. Urgency: How time-sensitive (1=low to 5=critical)
7. Entities: List of named entities with types

Respond in JSON format only."""

    def __init__(self, model_name: str = "Qwen/Qwen2.5-1.5B-Instruct"):
        self.model_name = model_name
        self.model = None
        self.tokenizer = None
        
    async def load_model(self) -> None:
        """Load the SLM model."""
        logger.info(f"Loading SLM model: {self.model_name}")
        
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_name,
            torch_dtype=torch.float16,
            device_map="auto",
        )
        
        logger.info("SLM model loaded")
        
    async def enrich(self, signal: dict[str, Any]) -> dict[str, Any]:
        """Enrich a signal with SLM-extracted semantics."""
        # Prepare input text
        raw_text = self._extract_text(signal)
        
        # Generate semantic extraction
        extraction = await self._inference(raw_text)
        
        # Merge into signal
        enriched = {**signal}
        enriched.update({
            "subject": extraction.get("subject", signal.get("subject", "")),
            "action": extraction.get("action", signal.get("action", "")),
            "object": extraction.get("object", signal.get("object", "")),
            "confidence": extraction.get("confidence", 0.5),
            "sentiment": extraction.get("sentiment", 0.0),
            "urgency": extraction.get("urgency", 3),
            "entities": extraction.get("entities", []),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        })
        
        return enriched
        
    def _extract_text(self, signal: dict[str, Any]) -> str:
        """Extract text content from signal for SLM processing."""
        raw_data = signal.get("raw_data", {})
        
        # Try common text fields
        for field in ["text", "content", "title", "description", "message"]:
            if field in raw_data:
                return str(raw_data[field])[:2000]  # Truncate for SLM context
                
        return str(raw_data)[:2000]
        
    async def _inference(self, text: str) -> dict[str, Any]:
        """Run SLM inference."""
        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": f"Analyze this signal:\n\n{text}"},
        ]
        
        inputs = self.tokenizer.apply_chat_template(
            messages, 
            return_tensors="pt",
            add_generation_prompt=True,
        ).to(self.model.device)
        
        with torch.no_grad():
            outputs = self.model.generate(
                inputs,
                max_new_tokens=512,
                temperature=0.1,
                do_sample=True,
            )
            
        response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        # Parse JSON from response
        try:
            import json
            # Find JSON in response
            start = response.find("{")
            end = response.rfind("}") + 1
            if start >= 0 and end > start:
                return json.loads(response[start:end])
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse SLM response: {response[:200]}")
            
        return {}
```

---

## 5. Snowflake Integration

### Architecture

```
┌──────────────┐     ┌─────────────────────────────┐     ┌──────────────────┐
│ enriched-    │────▶│  Snowflake Kafka Connector  │────▶│  SIGNALS_RAW     │
│ signals      │     │  (Kafka Connect)            │     │  (staging table) │
└──────────────┘     └─────────────────────────────┘     └──────────────────┘
                                                                  │
                                                                  ▼
                                                         ┌──────────────────┐
                                                         │  dbt transforms  │
                                                         └──────────────────┘
                                                                  │
                          ┌───────────────────────────────────────┼───────────────────┐
                          ▼                                       ▼                   ▼
                   ┌──────────────┐                    ┌──────────────────┐  ┌──────────────────┐
                   │ STG_SIGNALS  │                    │ INT_SIGNALS_     │  │ MART_SIGNALS     │
                   │ (cleaned)    │                    │ ENRICHED         │  │ (analytics)      │
                   └──────────────┘                    └──────────────────┘  └──────────────────┘
```

### Kafka Connector Configuration

```json
{
  "name": "snowflake-signals-sink",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "tasks.max": "4",
    "topics": "enriched-signals",
    "snowflake.url.name": "${SNOWFLAKE_URL}",
    "snowflake.user.name": "${SNOWFLAKE_USER}",
    "snowflake.private.key": "${SNOWFLAKE_PRIVATE_KEY}",
    "snowflake.database.name": "ACC_RAW",
    "snowflake.schema.name": "SIGNALS",
    "snowflake.topic2table.map": "enriched-signals:SIGNALS_RAW",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "buffer.count.records": "10000",
    "buffer.flush.time": "60",
    "buffer.size.bytes": "5000000"
  }
}
```

### Snowflake Schema DDL

```sql
-- Create database and schema
CREATE DATABASE IF NOT EXISTS ACC_RAW;
CREATE SCHEMA IF NOT EXISTS ACC_RAW.SIGNALS;

-- Raw signals table (auto-ingested from Kafka)
CREATE TABLE IF NOT EXISTS ACC_RAW.SIGNALS.SIGNALS_RAW (
    RECORD_METADATA VARIANT,
    RECORD_CONTENT VARIANT,
    _INGESTION_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Analytics database
CREATE DATABASE IF NOT EXISTS ACC_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ACC_ANALYTICS.STAGING;
CREATE SCHEMA IF NOT EXISTS ACC_ANALYTICS.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS ACC_ANALYTICS.MARTS;

-- Staged signals (dbt model: stg_signals)
CREATE TABLE IF NOT EXISTS ACC_ANALYTICS.STAGING.STG_SIGNALS (
    SIGNAL_ID VARCHAR(36) PRIMARY KEY,
    VERSION VARCHAR(10),
    SUBJECT VARCHAR(500),
    ACTION VARCHAR(500),
    OBJECT VARCHAR(500),
    CONFIDENCE FLOAT,
    SENTIMENT FLOAT,
    URGENCY INT,
    SOURCE VARCHAR(50),
    SOURCE_ID VARCHAR(255),
    SOURCE_URL VARCHAR(2000),
    TIMESTAMP TIMESTAMP_NTZ,
    INGESTED_AT TIMESTAMP_NTZ,
    PROCESSED_AT TIMESTAMP_NTZ,
    CATEGORY VARCHAR(50),
    SUBCATEGORY VARCHAR(100),
    TAGS ARRAY,
    ENTITIES VARIANT,
    RAW_DATA VARIANT,
    METADATA VARIANT,
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create clustering for performance
ALTER TABLE ACC_ANALYTICS.STAGING.STG_SIGNALS 
CLUSTER BY (CATEGORY, DATE_TRUNC('DAY', TIMESTAMP));
```

### dbt Model (`dbt/models/staging/stg_signals.sql`)

```sql
{{ config(
    materialized='incremental',
    unique_key='signal_id',
    incremental_strategy='merge',
    cluster_by=['category', 'timestamp::date']
) }}

WITH raw_signals AS (
    SELECT
        RECORD_CONTENT:id::VARCHAR(36) AS signal_id,
        RECORD_CONTENT:version::VARCHAR(10) AS version,
        RECORD_CONTENT:subject::VARCHAR(500) AS subject,
        RECORD_CONTENT:action::VARCHAR(500) AS action,
        RECORD_CONTENT:object::VARCHAR(500) AS object,
        RECORD_CONTENT:confidence::FLOAT AS confidence,
        RECORD_CONTENT:sentiment::FLOAT AS sentiment,
        RECORD_CONTENT:urgency::INT AS urgency,
        RECORD_CONTENT:source::VARCHAR(50) AS source,
        RECORD_CONTENT:source_id::VARCHAR(255) AS source_id,
        RECORD_CONTENT:source_url::VARCHAR(2000) AS source_url,
        TRY_TO_TIMESTAMP_NTZ(RECORD_CONTENT:timestamp::VARCHAR) AS timestamp,
        TRY_TO_TIMESTAMP_NTZ(RECORD_CONTENT:ingested_at::VARCHAR) AS ingested_at,
        TRY_TO_TIMESTAMP_NTZ(RECORD_CONTENT:processed_at::VARCHAR) AS processed_at,
        RECORD_CONTENT:category::VARCHAR(50) AS category,
        RECORD_CONTENT:subcategory::VARCHAR(100) AS subcategory,
        RECORD_CONTENT:tags AS tags,
        RECORD_CONTENT:entities AS entities,
        RECORD_CONTENT:raw_data AS raw_data,
        RECORD_CONTENT:metadata AS metadata,
        _INGESTION_TIME
    FROM {{ source('acc_raw', 'signals_raw') }}
    {% if is_incremental() %}
    WHERE _INGESTION_TIME > (SELECT COALESCE(MAX(_loaded_at), '1970-01-01') FROM {{ this }})
    {% endif %}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM raw_signals
WHERE signal_id IS NOT NULL
  AND subject IS NOT NULL
  AND action IS NOT NULL
```

---

## 6. Provider Implementation Details

| Provider | API/Protocol | Rate Limits | Auth | Kafka Topic |
|----------|-------------|-------------|------|-------------|
| **GDELT** | HTTP REST (GKG 2.0) | 1 req/sec | None | `raw-signals.geopolitical.gdelt` |
| **Telegram** | MTProto + scraping | 30 req/min | API credentials | `raw-signals.geopolitical.telegram` |
| **FRED** | HTTP REST | 120 req/min | API key | `raw-signals.macro.fred` |
| **Trading Economics** | HTTP REST + WebSocket | 10 req/sec | API key | `raw-signals.macro.tradingeconomics` |
| **Whale Alert** | HTTP REST | 10 req/min (free) | API key | `raw-signals.crypto.whalealert` |
| **Binance** | WebSocket streams | N/A (push) | None (public) | `raw-signals.crypto.binance` |
| **CME COT** | HTTP (weekly PDF/CSV) | N/A (weekly) | None | `raw-signals.macro.cot` |

### GDELT GKG 2.0 Details

- **Base URL**: `http://data.gdeltproject.org/gdeltv2/`
- **Update Frequency**: Every 15 minutes
- **File Format**: CSV (tab-delimited)
- **Key Fields**: GKGRECORDID, DATE, SourceCollectionIdentifier, Themes, Locations, Persons, Organizations, Tone

### Binance WebSocket Streams

- **URL**: `wss://stream.binance.com:9443/ws`
- **Recommended Streams**:
  - `<symbol>@trade` - Real-time trades
  - `<symbol>@depth@100ms` - Order book updates
  - `<symbol>@aggTrade` - Aggregated trades
- **Reconnection**: Required every 24 hours

### Whale Alert API

- **Base URL**: `https://api.whale-alert.io/v1/transactions`
- **Free Tier**: 10 requests/minute, $1M+ transactions
- **Parameters**: `min_value`, `cursor`, `start`, `end`

---

## 7. Parallel Execution Task Graph

```
═══════════════════════════════════════════════════════════════════════════════
                           WAVE 1: FOUNDATION (Parallel)
═══════════════════════════════════════════════════════════════════════════════
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ T1.1: Project   │  │ T1.2: Go Module │  │ T1.3: Docker    │  │ T1.4: Signal    │
│ Structure       │  │ Setup           │  │ Compose (Kafka) │  │ Schema          │
│                 │  │                 │  │                 │  │                 │
│ Category: quick │  │ Category: quick │  │ Category: quick │  │ Category: quick │
│ Effort: XS      │  │ Effort: XS      │  │ Effort: S       │  │ Effort: S       │
└─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                         WAVE 2: CORE ABSTRACTIONS
                         (Depends on Wave 1)
═══════════════════════════════════════════════════════════════════════════════
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ T2.1: Provider  │  │ T2.2: Kafka     │  │ T2.3: Rate      │  │ T2.4: Config    │
│ Interface       │  │ Producer        │  │ Limiter &       │  │ Loader          │
│                 │  │                 │  │ Circuit Breaker │  │                 │
│ Category:       │  │ Category:       │  │ Category:       │  │ Category: quick │
│ ultrabrain      │  │ unspec-low      │  │ ultrabrain      │  │ Effort: S       │
│ Effort: M       │  │ Effort: M       │  │ Effort: M       │  │                 │
└─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                         WAVE 3: PROVIDERS (Parallel)
                         (Depends on Wave 2)
═══════════════════════════════════════════════════════════════════════════════
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ T3.1: GDELT  │ │ T3.2: FRED   │ │ T3.3: Whale  │ │ T3.4: Binance│ │ T3.5: CME COT│
│ Provider     │ │ Provider     │ │ Alert        │ │ WebSocket    │ │ Provider     │
│              │ │              │ │ Provider     │ │ Provider     │ │              │
│ Category:    │ │ Category:    │ │ Category:    │ │ Category:    │ │ Category:    │
│ unspec-low   │ │ unspec-low   │ │ unspec-low   │ │ ultrabrain   │ │ unspec-low   │
│ Effort: M    │ │ Effort: S    │ │ Effort: S    │ │ Effort: L    │ │ Effort: S    │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘

┌──────────────┐ ┌──────────────┐
│ T3.6: Trad.  │ │ T3.7:Telegram│
│ Economics    │ │ Provider     │
│ Provider     │ │              │
│ Category:    │ │ Category:    │
│ unspec-low   │ │ ultrabrain   │
│ Effort: M    │ │ Effort: XL   │
└──────────────┘ └──────────────┘

═══════════════════════════════════════════════════════════════════════════════
                         WAVE 4: SLM WORKER (Parallel with Wave 3)
                         (Depends on Wave 1 Signal Schema)
═══════════════════════════════════════════════════════════════════════════════
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ T4.1: Python    │  │ T4.2: SLM       │  │ T4.3: Kafka     │
│ Project Setup   │  │ Processor       │  │ Consumer/       │
│                 │  │ (Qwen 2.5)      │  │ Producer        │
│ Category: quick │  │ Category:       │  │ Category:       │
│ Effort: XS      │  │ ultrabrain      │  │ unspec-low      │
│                 │  │ Effort: L       │  │ Effort: M       │
└─────────────────┘  └─────────────────┘  └─────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                         WAVE 5: SNOWFLAKE INTEGRATION
                         (Depends on Wave 1, Parallel with Wave 3-4)
═══════════════════════════════════════════════════════════════════════════════
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ T5.1: Snowflake │  │ T5.2: Kafka     │  │ T5.3: dbt       │
│ Schema DDL      │  │ Connector       │  │ Project Setup   │
│                 │  │ Config          │  │ & Models        │
│ Category: quick │  │ Category:       │  │ Category:       │
│ Effort: S       │  │ unspec-low      │  │ unspec-high     │
│                 │  │ Effort: M       │  │ Effort: L       │
└─────────────────┘  └─────────────────┘  └─────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                         WAVE 6: ORCHESTRATION & OBSERVABILITY
                         (Depends on Wave 2-4)
═══════════════════════════════════════════════════════════════════════════════
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ T6.1: Sensor    │  │ T6.2: Prometheus│  │ T6.3: Health    │
│ Orchestrator    │  │ Metrics Export  │  │ Check Endpoints │
│                 │  │                 │  │                 │
│ Category:       │  │ Category:       │  │ Category: quick │
│ unspec-high     │  │ unspec-low      │  │ Effort: S       │
│ Effort: M       │  │ Effort: M       │  │                 │
└─────────────────┘  └─────────────────┘  └─────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                         WAVE 7: TESTING & DEPLOYMENT
                         (Depends on all previous waves)
═══════════════════════════════════════════════════════════════════════════════
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ T7.1: Unit      │  │ T7.2: Integra-  │  │ T7.3: K8s       │
│ Tests           │  │ tion Tests      │  │ Manifests       │
│                 │  │                 │  │                 │
│ Category:       │  │ Category:       │  │ Category:       │
│ unspec-low      │  │ unspec-high     │  │ unspec-low      │
│ Effort: M       │  │ Effort: L       │  │ Effort: M       │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

---

## 8. Unresolved Technical Questions & Risks

| # | Risk/Question | Impact | Mitigation Strategy |
|---|---------------|--------|---------------------|
| **R1** | **GDELT Rate Limiting**: Undocumented limits during high-activity | Medium | Adaptive backoff; cache last-known; BigQuery for backfill |
| **R2** | **Telegram Scraping Legality**: May violate ToS | High | Use official Bot API; user-agent rotation; legal review |
| **R3** | **SLM Latency**: May bottleneck at high throughput | Medium | Batch processing; vLLM; horizontal scaling |
| **R4** | **Snowflake Connector Costs**: Streaming compute costs | Low | Monitor credits; batching windows; Snowpipe REST fallback |
| **R5** | **Binance Reconnection**: 24h disconnects; ordering during reconnect | Medium | Heartbeat monitoring; sequence IDs; graceful reconnect |
| **R6** | **API Key Management**: Multiple keys need secure storage | High | HashiCorp Vault / AWS Secrets Manager; rotation |
| **R7** | **Kafka Exactly-Once**: Duplicate prevention | Medium | Idempotent producers; signal ID dedup; MERGE upserts |
| **R8** | **CME COT Timeliness**: 3-day lag on weekly data | Low | Acceptable for macro; document in metadata |

---

## 9. Numbered Execution Steps for Atlas (Executor)

### Phase 1: Foundation (Day 1-2)

| Step | Task | Success Criteria |
|------|------|------------------|
| **1** | Create directory structure | All directories exist |
| **2** | Initialize Go module | `go.mod` created |
| **3** | Add core dependencies | Dependencies in `go.mod` |
| **4** | Create Docker Compose | `docker-compose up` starts Kafka |
| **5** | Define Signal struct | Compiles without errors |
| **6** | Create JSON schema | Valid JSON Schema |

### Phase 2: Core Abstractions (Day 2-4)

| Step | Task | Success Criteria |
|------|------|------------------|
| **7** | Define Provider interface | Interface compiles |
| **8** | Implement BaseProvider | BaseProvider with Emit() works |
| **9** | Implement rate limiter | Unit tests pass |
| **10** | Implement circuit breaker | Unit tests pass |
| **11** | Create Kafka producer | Can publish to local Kafka |
| **12** | Create config loader | Loads YAML configs |

### Phase 3: First Provider - GDELT (Day 4-5)

| Step | Task | Success Criteria |
|------|------|------------------|
| **13** | Create GDELT client | Can fetch GKG data |
| **14** | Create GKG parser | Parses GKG 2.0 format |
| **15** | Implement GDELT provider | Emits signals to channel |
| **16** | Create sensor binary | Binary runs, publishes to Kafka |
| **17** | Write unit tests | `go test` passes |
| **18** | Integration test | Signals appear in topic |

### Phase 4: Remaining Providers (Day 5-10)

| Step | Task | Parallel Group |
|------|------|----------------|
| **19** | FRED provider | A |
| **20** | Whale Alert provider | A |
| **21** | CME COT provider | A |
| **22** | Binance WebSocket provider | B |
| **23** | Trading Economics provider | B |
| **24** | Telegram provider | C (complex) |

### Phase 5: SLM Worker (Day 6-9)

| Step | Task | Success Criteria |
|------|------|------------------|
| **25** | Setup Python project | `pip install -e .` works |
| **26** | Implement SLM processor | Model loads, inference works |
| **27** | Implement Kafka consumer | Consumes from `raw-signals` |
| **28** | Add DLQ handling | Failed messages preserved |
| **29** | Create Dockerfile | Container builds |
| **30** | Integration test | Enriched signals appear |

### Phase 6: Snowflake Integration (Day 8-11)

| Step | Task | Success Criteria |
|------|------|------------------|
| **31** | Create Snowflake DDL | Tables created |
| **32** | Configure Kafka Connector | Connector deployed |
| **33** | Setup dbt project | dbt project initialized |
| **34** | Create staging models | `dbt run` succeeds |
| **35** | Create mart models | Analytics tables populated |
| **36** | Verify data flow | Data visible within 2 min |

### Phase 7: Orchestration & Observability (Day 10-12)

| Step | Task | Success Criteria |
|------|------|------------------|
| **37** | Create orchestrator | Manages provider lifecycles |
| **38** | Add Prometheus metrics | `/metrics` endpoint works |
| **39** | Create health endpoints | K8s probes pass |
| **40** | Add Grafana dashboards | Visualizations work |
| **41** | Setup alerting rules | Alerts fire correctly |

### Phase 8: Testing & Deployment (Day 12-14)

| Step | Task | Success Criteria |
|------|------|------------------|
| **42** | Write unit tests | >80% coverage |
| **43** | Write integration tests | E2E tests pass |
| **44** | Create K8s manifests | `kubectl apply` works |
| **45** | Create Helm chart | `helm install` works |
| **46** | Document runbook | Operations documented |
| **47** | Final E2E validation | Full pipeline verified |

---

## 10. Configuration Reference (`configs/base.yaml`)

```yaml
version: "1.0"

kafka:
  brokers:
    - localhost:9092
  producer:
    batch_size: 100
    linger_ms: 10
    compression: snappy
  topics:
    raw_signals: "raw-signals"
    enriched_signals: "enriched-signals"

providers:
  gdelt:
    enabled: true
    poll_interval: 15m
    base_url: "http://data.gdeltproject.org/gdeltv2"
    rate_limit:
      requests_per_second: 1.0
      burst: 2
    circuit_breaker:
      threshold: 5
      timeout: 60s
      max_requests: 3
    retry:
      max_attempts: 3
      initial_wait: 1s
      max_wait: 30s
      multiplier: 2.0

  fred:
    enabled: true
    poll_interval: 1h
    api_key: "${FRED_API_KEY}"
    series_ids:
      - "DFF"
      - "CPIAUCSL"
      - "UNRATE"
    rate_limit:
      requests_per_second: 2.0
      burst: 5

  binance:
    enabled: true
    websocket_url: "wss://stream.binance.com:9443/ws"
    streams:
      - "btcusdt@trade"
      - "ethusdt@trade"
    reconnect_delay: 5s

  whale_alert:
    enabled: true
    poll_interval: 1m
    api_key: "${WHALE_ALERT_API_KEY}"
    min_value_usd: 1000000
    rate_limit:
      requests_per_second: 0.15
      burst: 1

  telegram:
    enabled: false
    api_id: "${TELEGRAM_API_ID}"
    api_hash: "${TELEGRAM_API_HASH}"

  trading_economics:
    enabled: true
    poll_interval: 5m
    api_key: "${TRADING_ECONOMICS_API_KEY}"

  cot:
    enabled: true
    poll_interval: 24h

observability:
  metrics:
    enabled: true
    port: 9090
    path: "/metrics"
  health:
    port: 8080
    path: "/health"
  logging:
    level: "info"
    format: "json"
```

---

## 11. Makefile

```makefile
.PHONY: all build test lint clean docker-up docker-down

GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
BINARY_DIR=bin

SENSORS=gdelt telegram fred tradingeconomics whalealert binance cot orchestrator

all: build

build: $(SENSORS)

$(SENSORS):
	$(GOBUILD) -o $(BINARY_DIR)/sensor-$@ ./cmd/sensor-$@/...

test:
	$(GOTEST) -v -race -cover ./...

test-integration:
	$(GOTEST) -v -tags=integration ./tests/integration/...

lint:
	golangci-lint run ./...

clean:
	rm -rf $(BINARY_DIR)

deps:
	$(GOCMD) mod download && $(GOCMD) mod tidy

docker-build:
	docker build -f deployments/docker/Dockerfile.sensor -t acc-sensor:latest .
	docker build -f deployments/docker/Dockerfile.slm -t acc-slm-worker:latest ./python

docker-up:
	docker-compose -f deployments/docker/docker-compose.yaml up -d

docker-down:
	docker-compose -f deployments/docker/docker-compose.yaml down

dev-setup: docker-up deps
	@echo "Development environment ready"

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test
```

---

## Summary

This architecture plan provides:

1. **Modular Sensor Architecture**: Each provider is independent, following antifragile principles
2. **Generic Provider Interface**: Easy addition of new data sources
3. **Standard Signal Schema**: Semantic triple (Subject-Action-Object) for queryability
4. **SLM Edge Processing**: Local semantic compression before storage
5. **Snowflake Integration**: Kafka Connector → dbt pipeline
6. **47 Concrete Execution Steps**: Organized in 7 parallel waves

**Estimated Timeline**: 14 days for MVP with all 7 providers operational.

**Next Steps**: Begin Wave 1 (Foundation) tasks in parallel.
