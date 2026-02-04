# ACC L1 Ingestion (Experimental)

> **Note**: This repository is part of an **experimental "Agent on Steroids" workflow**—the codebase evolves rapidly as autonomous AI agents refactor, document, and integrate new services. Expect frequent changes while we harden the stack.

## Overview
ACC L1 Ingestion ingests multi-source signals (geopolitical, macro, crypto, community) via Go-based providers, streams them through Redpanda, enriches them with a Python SLM pipeline, persists them in SQLite, derives L2 insights, and delivers multi-layer alerts to Telegram/Discord.

### Key Components
- **Go Providers** (GDELT, FRED, Binance, CME COT, Telegram)
- **Redpanda** (Kafka-compatible broker)
- **Python SLM Worker** (Qwen 2.5-1.5B enrichment)
- **Persister** (SQLite + HTTP query API, snappy-enabled consumer)
- **L2 Reasoner** (correlation/entity engine emitting `l2.insights`)
- **Alerter** (ACC L1 Signals + L2 Insights → Telegram/Discord)
- **Prometheus + Grafana** (dashboards include dedicated L2 view)

## Getting Started
```bash
git clone <repo>
cd openclaw-acc
cp .env.example .env            # Fill in API keys, bot tokens, etc.
docker compose up -d            # Start Redpanda, providers, Python services, monitoring

# Tail logs (example)

# Tear down
docker compose down
```

## Documentation
- `docs/ACC-L1-MVP-ARCHITECTURE-PLAN.md` – full architecture & waves
- `docs/L2_L3_SPEC.md` / `docs/L4_SPEC.md` – higher-layer plans
- `docs/oracle_provider_research.md` – low-cost provider options
- `CLAUDE.md`, `AGENTS.md` – living references for AI agents & contributors

## Status
- ✅ Containers for providers + Python stack
- ✅ L2 Reasoner + L2-aware alerting
- ✅ Persister snappy support, Grafana dashboards
- 🔬 Active experimentation continues under the Agent-on-Steroids workflow

---
Questions? Check the docs or ping the agent running this repo—nothing here stays static for long.
