# L2/L3 Architecture Specification

## Overview

This document defines the Kafka topic schemas, data flows, and architecture for L2 (Reasoning Layer) and L3 (Decision/Action Layer) of the ACC system.

```
┌─────────────────────────────────────────────────────────────────┐
│                         L1: INGESTION                            │
│  GDELT │ FRED │ Binance │ COT │ Telegram                        │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
              ┌─────────────────────┐
              │ l1.signals.enriched │
              └─────────┬───────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                         L2: REASONING                            │
│           Correlation Engine │ Entity Tracker                    │
└───────────────────────┬─────────────────────────────────────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
   ┌────────────┐ ┌────────────┐ ┌────────────┐
   │l2.insights │ │l2.situations│ │l2.entities │
   └─────┬──────┘ └─────┬──────┘ └────────────┘
         │              │
         └──────┬───────┘
                ▼
┌─────────────────────────────────────────────────────────────────┐
│                         L3: DECISION                             │
│             Planner │ Guardrails │ Executor                      │
└───────────────────────┬─────────────────────────────────────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
   ┌────────────┐ ┌────────────┐ ┌────────────┐
   │l3.proposals│ │l3.actions  │ │l3.audit    │
   └────────────┘ └────────────┘ └────────────┘
```

---

## L1 Reference: Enriched Signal Schema

Input to L2 from `l1.signals.enriched`:

```json
{
  "id": "uuid",
  "timestamp": "2024-01-15T10:30:00Z",
  "source": "gdelt|fred|binance|cme_cot|telegram",
  "category": "geopolitical|macro|crypto",
  "subject": "Russia",
  "action": "announced sanctions on",
  "object": "European banks",
  "confidence": 0.85,
  "sentiment": -0.7,
  "urgency": "high",
  "tags": ["sanctions", "russia", "europe", "banks"],
  "metadata": {
    "ingested_at": "2024-01-15T10:30:05Z",
    "processed_at": "2024-01-15T10:30:10Z",
    "schema_version": "1.0.0",
    "slm_model": "Qwen/Qwen2.5-1.5B-Instruct",
    "enrichment_latency_ms": 450.5
  },
  "enrichment": {
    "refined_sentiment": -0.75,
    "refined_urgency": "high",
    "entities": ["Russia", "European Central Bank", "Deutsche Bank"],
    "summary": "Russia announces retaliatory sanctions targeting European financial institutions",
    "market_impact": "negative",
    "confidence_adjustment": 0.05
  }
}
```

---

## L2 Layer: Reasoning

### Purpose
- Consume enriched signals from L1
- Track entities across time (entity timelines)
- Detect cross-signal correlations
- Emit insights (events) and situations (stateful rollups)

### Kafka Topics

#### `l2.insights` - Event-like detections

Schema version: `2.0.0`

```json
{
  "id": "uuid",
  "timestamp": "2024-01-15T10:35:00Z",
  "type": "correlation_detected|anomaly_detected|regime_shift|threshold_breach|multi_source_confirmation",
  "severity": "info|warning|alert|critical",
  "title": "Multi-source confirmation: Russia sanctions escalation",
  "description": "3 independent sources (GDELT, Telegram, FRED) confirm escalating Russia-EU tensions within 2-hour window",
  
  "trigger_signals": [
    {"id": "signal-uuid-1", "source": "gdelt", "weight": 0.4},
    {"id": "signal-uuid-2", "source": "telegram", "weight": 0.35},
    {"id": "signal-uuid-3", "source": "fred", "weight": 0.25}
  ],
  
  "entities": ["Russia", "European Union", "EUR/USD"],
  "categories": ["geopolitical", "macro"],
  
  "metrics": {
    "correlation_score": 0.87,
    "confidence": 0.82,
    "signal_count": 3,
    "time_window_hours": 2
  },
  
  "context": {
    "historical_frequency": "rare",
    "last_similar_event": "2022-03-01T00:00:00Z",
    "baseline_deviation_sigma": 2.5
  },
  
  "metadata": {
    "created_at": "2024-01-15T10:35:00Z",
    "schema_version": "2.0.0",
    "reasoner_version": "1.0.0",
    "processing_latency_ms": 125
  }
}
```

**Insight Types:**

| Type | Description | Trigger |
|------|-------------|---------|
| `multi_source_confirmation` | Same event detected across 2+ sources | Signal clustering by entity/topic |
| `correlation_detected` | Statistical correlation between signals | Cross-category correlation engine |
| `anomaly_detected` | Signal deviates from baseline | Z-score > 2.0 on rolling window |
| `regime_shift` | Sustained change in signal patterns | CUSUM or change-point detection |
| `threshold_breach` | Metric crosses predefined threshold | Rule-based monitoring |

---

#### `l2.situations` - Stateful rollups

Schema version: `2.0.0`

```json
{
  "id": "uuid",
  "entity": "Russia",
  "type": "ongoing_conflict|macro_risk|market_regime|entity_status",
  "status": "escalating|stable|de-escalating|new|resolved",
  
  "title": "Russia-EU Tensions",
  "summary": "Escalating diplomatic and economic confrontation between Russia and European Union",
  
  "timeline": {
    "started_at": "2024-01-10T00:00:00Z",
    "last_updated": "2024-01-15T10:35:00Z",
    "duration_hours": 130,
    "signal_count": 47
  },
  
  "current_state": {
    "sentiment_avg": -0.65,
    "sentiment_trend": "declining",
    "urgency_mode": "high",
    "confidence": 0.78
  },
  
  "risk_assessment": {
    "level": "elevated",
    "score": 0.72,
    "factors": ["sanctions_escalation", "energy_supply_risk", "currency_volatility"]
  },
  
  "related_entities": ["European Union", "Germany", "EUR/USD", "Natural Gas"],
  "related_insights": ["insight-uuid-1", "insight-uuid-2"],
  
  "metadata": {
    "created_at": "2024-01-10T00:00:00Z",
    "updated_at": "2024-01-15T10:35:00Z",
    "schema_version": "2.0.0",
    "update_count": 23
  }
}
```

**Situation Types:**

| Type | Description | Lifecycle |
|------|-------------|-----------|
| `ongoing_conflict` | Active geopolitical conflict | Created on first signal, updated continuously |
| `macro_risk` | Macroeconomic risk condition | Created on FRED indicator breach |
| `market_regime` | Crypto/market regime state | Risk-on/risk-off classification |
| `entity_status` | Entity health/status tracking | Per-entity state machine |

---

#### `l2.entities` - Entity state updates

Schema version: `2.0.0`

```json
{
  "id": "uuid",
  "entity": "Russia",
  "entity_type": "country|organization|asset|person",
  "canonical_name": "Russian Federation",
  "aliases": ["Russia", "RU", "Russian Fed"],
  
  "current_state": {
    "sentiment_7d": -0.45,
    "sentiment_30d": -0.32,
    "mention_count_7d": 156,
    "mention_count_30d": 423,
    "urgency_distribution": {"low": 0.2, "medium": 0.5, "high": 0.25, "critical": 0.05},
    "top_actions": ["sanctioned", "announced", "threatened"],
    "top_objects": ["European Union", "United States", "Ukraine"]
  },
  
  "relationships": [
    {"entity": "Ukraine", "relationship": "conflict", "strength": 0.95},
    {"entity": "European Union", "relationship": "adversarial", "strength": 0.75},
    {"entity": "China", "relationship": "aligned", "strength": 0.60}
  ],
  
  "active_situations": ["situation-uuid-1", "situation-uuid-2"],
  
  "metadata": {
    "first_seen": "2024-01-01T00:00:00Z",
    "last_seen": "2024-01-15T10:35:00Z",
    "total_signals": 892,
    "schema_version": "2.0.0"
  }
}
```

---

### L2 Correlation Rules

#### Rule 1: Multi-Source Confirmation
```python
if (
    signals_with_same_entity >= 2 and
    distinct_sources >= 2 and
    time_window_hours <= 4 and
    avg_confidence >= 0.6
):
    emit_insight(type="multi_source_confirmation")
```

#### Rule 2: Crypto Risk Spike
```python
if (
    source == "binance" and
    urgency == "critical" and
    abs(sentiment) >= 0.8
):
    check_fred_signals(entity="VIX", window_hours=24)
    if fred_vix_elevated:
        emit_insight(type="correlation_detected", title="Crypto-Macro Risk Alignment")
```

#### Rule 3: Risk-Off Shift Detection
```python
recent_signals = get_signals(hours=24)
crypto_sentiment = avg([s.sentiment for s in recent_signals if s.category == "crypto"])
macro_sentiment = avg([s.sentiment for s in recent_signals if s.category == "macro"])

if crypto_sentiment < -0.5 and macro_sentiment < -0.3:
    if previous_regime != "risk_off":
        emit_insight(type="regime_shift", title="Risk-Off Regime Detected")
        update_situation(type="market_regime", status="risk_off")
```

#### Rule 4: Entity Anomaly
```python
entity_baseline = get_entity_baseline(entity, days=30)
current_rate = get_signal_rate(entity, hours=4)

z_score = (current_rate - entity_baseline.mean) / entity_baseline.std
if abs(z_score) > 2.5:
    emit_insight(type="anomaly_detected", metrics={"z_score": z_score})
```

---

## L3 Layer: Decision & Action

### Purpose
- Consume L2 insights and situations
- Generate action proposals with risk assessment
- Execute approved actions through guardrails
- Maintain complete audit trail

### Risk Tiers

| Tier | Description | Approval | Examples |
|------|-------------|----------|----------|
| 0 | Notifications only | Auto-execute | Telegram alerts, log entries |
| 1 | Create artifacts | Auto-execute | Create notes, tickets, reports |
| 2 | Configuration changes | Requires approval | Adjust thresholds, enable/disable rules |
| 3 | Financial/Irreversible | Multi-step approval | Trading signals, external API calls |

### Kafka Topics

#### `l3.proposals` - Action proposals

Schema version: `3.0.0`

```json
{
  "id": "uuid",
  "timestamp": "2024-01-15T10:40:00Z",
  "status": "pending|approved|rejected|expired|executed",
  
  "trigger": {
    "type": "insight|situation|schedule|manual",
    "source_id": "insight-uuid-1",
    "source_type": "l2.insights"
  },
  
  "action": {
    "type": "notify|create_artifact|update_config|execute_trade",
    "tier": 0,
    "target": "telegram",
    "operation": "send_message",
    "parameters": {
      "chat_id": "-1001234567890",
      "message": "⚠️ ALERT: Multi-source confirmation of Russia sanctions escalation",
      "parse_mode": "HTML"
    }
  },
  
  "risk_assessment": {
    "tier": 0,
    "reversible": true,
    "impact_score": 0.1,
    "confidence": 0.95,
    "reasoning": "Notification-only action with no side effects"
  },
  
  "approval": {
    "required": false,
    "auto_approve_reason": "Tier 0 action",
    "approvers": [],
    "approved_at": null,
    "approved_by": null
  },
  
  "execution": {
    "scheduled_at": "2024-01-15T10:40:00Z",
    "executed_at": null,
    "result": null,
    "error": null,
    "retries": 0
  },
  
  "metadata": {
    "created_at": "2024-01-15T10:40:00Z",
    "schema_version": "3.0.0",
    "planner_version": "1.0.0",
    "ttl_hours": 24
  }
}
```

---

#### `l3.actions` - Executed action results

Schema version: `3.0.0`

```json
{
  "id": "uuid",
  "proposal_id": "proposal-uuid",
  "timestamp": "2024-01-15T10:40:05Z",
  
  "action": {
    "type": "notify",
    "tier": 0,
    "target": "telegram",
    "operation": "send_message"
  },
  
  "result": {
    "status": "success|failure|partial",
    "output": {
      "message_id": 12345,
      "chat_id": "-1001234567890"
    },
    "error": null,
    "duration_ms": 150
  },
  
  "metadata": {
    "executed_at": "2024-01-15T10:40:05Z",
    "executor_version": "1.0.0",
    "schema_version": "3.0.0",
    "dry_run": false
  }
}
```

---

#### `l3.audit` - Complete audit trail

Schema version: `3.0.0`

```json
{
  "id": "uuid",
  "timestamp": "2024-01-15T10:40:05Z",
  "event_type": "proposal_created|proposal_approved|proposal_rejected|action_executed|action_failed|guardrail_triggered",
  
  "actor": {
    "type": "system|human|rule",
    "id": "planner-v1",
    "name": "L3 Planner"
  },
  
  "subject": {
    "type": "proposal|action|config",
    "id": "proposal-uuid"
  },
  
  "details": {
    "previous_state": "pending",
    "new_state": "executed",
    "reason": "Tier 0 auto-execution",
    "metadata": {}
  },
  
  "context": {
    "trigger_insight_id": "insight-uuid-1",
    "trigger_situation_id": null,
    "correlation_id": "trace-uuid"
  },
  
  "metadata": {
    "schema_version": "3.0.0",
    "retention_days": 365
  }
}
```

---

### L3 Guardrails

#### Rate Limits
```yaml
rate_limits:
  tier_0:
    per_minute: 60
    per_hour: 500
    per_day: 5000
  tier_1:
    per_minute: 10
    per_hour: 100
    per_day: 500
  tier_2:
    per_minute: 1
    per_hour: 10
    per_day: 50
  tier_3:
    per_minute: 0.1
    per_hour: 1
    per_day: 5
```

#### Kill Switch
```yaml
kill_switch:
  enabled: true
  trigger_conditions:
    - error_rate_1m > 0.5
    - action_rate_1m > 100
    - tier_2_plus_rate_1h > 20
  cooldown_minutes: 15
  notify_channels: ["telegram_ops"]
```

#### Allowlist
```yaml
allowed_actions:
  tier_0:
    - target: telegram
      operations: [send_message]
    - target: log
      operations: [info, warn, error]
  tier_1:
    - target: sqlite
      operations: [insert_note, insert_report]
    - target: file
      operations: [write_json, write_csv]
  tier_2:
    - target: config
      operations: [update_threshold, toggle_rule]
  tier_3:
    - target: external_api
      operations: [*]  # Requires explicit approval
```

---

## Implementation Phases

### Phase 1: L2 MVP (Week 1-2)
1. Create `python/l2_reasoner/` directory structure
2. Implement Kafka consumer for `l1.signals.enriched`
3. Build in-memory entity tracker
4. Implement Rule 1 (Multi-Source Confirmation)
5. Emit to `l2.insights` topic
6. Add SQLite persistence for replay

### Phase 2: L2 Full (Week 3-4)
1. Implement Rules 2-4
2. Add `l2.situations` emission
3. Add `l2.entities` tracking
4. Build evaluation harness with backtesting
5. Add Grafana dashboards for L2 metrics

### Phase 3: L3 MVP (Week 5-6)
1. Create `python/l3_planner/` directory structure
2. Implement Tier 0 action planner
3. Build executor with Telegram integration
4. Add guardrails (rate limits, allowlist)
5. Emit to `l3.proposals`, `l3.actions`, `l3.audit`

### Phase 4: L3 Full (Week 7-8)
1. Add Tier 1-2 action support
2. Implement approval workflow
3. Add kill switch
4. Build operator dashboard
5. Integration testing

---

## Directory Structure

```
l1-ingestion/
├── python/
│   ├── l2_reasoner/
│   │   ├── __init__.py
│   │   ├── main.py              # Kafka consumer loop
│   │   ├── config.py            # Configuration
│   │   ├── schemas/
│   │   │   ├── __init__.py
│   │   │   ├── insight.py       # L2 Insight schema
│   │   │   ├── situation.py     # L2 Situation schema
│   │   │   └── entity.py        # L2 Entity schema
│   │   ├── engine/
│   │   │   ├── __init__.py
│   │   │   ├── correlation.py   # Correlation detection
│   │   │   ├── entity_tracker.py # Entity state tracking
│   │   │   ├── rules.py         # Correlation rules
│   │   │   └── anomaly.py       # Anomaly detection
│   │   ├── persistence/
│   │   │   ├── __init__.py
│   │   │   └── database.py      # SQLite storage
│   │   └── requirements.txt
│   │
│   └── l3_planner/
│       ├── __init__.py
│       ├── main.py              # Kafka consumer loop
│       ├── config.py            # Configuration
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── proposal.py      # L3 Proposal schema
│       │   ├── action.py        # L3 Action schema
│       │   └── audit.py         # L3 Audit schema
│       ├── planner/
│       │   ├── __init__.py
│       │   ├── planner.py       # Action planning
│       │   └── risk.py          # Risk assessment
│       ├── executor/
│       │   ├── __init__.py
│       │   ├── executor.py      # Action execution
│       │   ├── telegram.py      # Telegram executor
│       │   └── file.py          # File executor
│       ├── guardrails/
│       │   ├── __init__.py
│       │   ├── rate_limiter.py  # Rate limiting
│       │   ├── allowlist.py     # Action allowlist
│       │   └── kill_switch.py   # Emergency stop
│       └── requirements.txt
│
└── deploy/
    └── docker-compose.yml       # Add l2-reasoner, l3-planner services
```

---

## Kafka Topic Configuration

```yaml
topics:
  # L2 Topics
  l2.insights:
    partitions: 6
    replication_factor: 1
    retention_ms: 604800000  # 7 days
    cleanup_policy: delete
    
  l2.situations:
    partitions: 3
    replication_factor: 1
    retention_ms: 2592000000  # 30 days
    cleanup_policy: compact
    
  l2.entities:
    partitions: 6
    replication_factor: 1
    retention_ms: -1  # Infinite
    cleanup_policy: compact
    
  # L3 Topics
  l3.proposals:
    partitions: 3
    replication_factor: 1
    retention_ms: 604800000  # 7 days
    cleanup_policy: delete
    
  l3.actions:
    partitions: 3
    replication_factor: 1
    retention_ms: 2592000000  # 30 days
    cleanup_policy: delete
    
  l3.audit:
    partitions: 1
    replication_factor: 1
    retention_ms: 31536000000  # 365 days
    cleanup_policy: delete
```

---

## Environment Variables

```bash
# L2 Reasoner
L2_KAFKA_BROKERS=redpanda:9092
L2_KAFKA_INPUT_TOPIC=l1.signals.enriched
L2_KAFKA_CONSUMER_GROUP=l2-reasoner
L2_OUTPUT_INSIGHTS_TOPIC=l2.insights
L2_OUTPUT_SITUATIONS_TOPIC=l2.situations
L2_OUTPUT_ENTITIES_TOPIC=l2.entities
L2_DB_PATH=/data/l2_reasoner.db
L2_CORRELATION_WINDOW_HOURS=4
L2_ANOMALY_ZSCORE_THRESHOLD=2.5
L2_LOG_LEVEL=INFO

# L3 Planner
L3_KAFKA_BROKERS=redpanda:9092
L3_KAFKA_INPUT_TOPICS=l2.insights,l2.situations
L3_KAFKA_CONSUMER_GROUP=l3-planner
L3_OUTPUT_PROPOSALS_TOPIC=l3.proposals
L3_OUTPUT_ACTIONS_TOPIC=l3.actions
L3_OUTPUT_AUDIT_TOPIC=l3.audit
L3_DRY_RUN=true
L3_TELEGRAM_BOT_TOKEN=<token>
L3_TELEGRAM_ALERT_CHAT_ID=<chat_id>
L3_RATE_LIMIT_TIER0_PER_MINUTE=60
L3_RATE_LIMIT_TIER1_PER_MINUTE=10
L3_KILL_SWITCH_ENABLED=true
L3_LOG_LEVEL=INFO
```
