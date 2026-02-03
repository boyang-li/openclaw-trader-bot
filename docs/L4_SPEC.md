# L4 Specification: Paper Trading + Strategy Allocation (All-Weather Seed)

## Overview

This document defines the architecture, Kafka topic schemas, and operating model for **L4**: a paper-trading and evaluation layer that learns *when* to deploy known portfolio strategies under real market conditions.

L4 is intentionally conservative:
- Paper trading only (no real execution).
- Learning adjusts *allocation weights* across known strategies (no strategy invention).
- Strong auditability and clean attribution (signal quality vs sizing vs execution).
- Multi-asset via a mix of Binance tradables + FRED-priced synthetic paper instruments.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                               L1: INGESTION                             │
│  GDELT │ FRED │ Binance │ CME COT │ Telegram                            │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
                      ┌─────────────────────┐
                      │ l1.signals.enriched │
                      └─────────┬───────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                               L2: REASONING                              │
│     Correlations │ Situations (regimes) │ Entity tracking               │
└───────────────┬──────────────────┬──────────────────────────────────────┘
                │                  │
                ▼                  ▼
         ┌────────────┐     ┌──────────────┐
         │ l2.insights │     │ l2.situations │
         └─────┬──────┘     └──────┬───────┘
               │                   │
               └──────────┬────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           L4: PAPER + LEARNING                           │
│ Strategy Library │ Portfolio Planner │ Execution Model │ Evaluator      │
│ Contextual Bandit │ Attribution │ Benchmarks │ Feedback                 │
└───────────────┬───────────────────────────────────────────┬─────────────┘
                │                                           │
                ▼                                           ▼
         ┌────────────┐                             ┌────────────────┐
         │  l4.plans   │                             │ l4.evaluations │
         └─────┬──────┘                             └──────┬─────────┘
               │                                           │
               ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         L3: DECISION / EXECUTION                         │
│ Guardrails │ Approvals │ Paper Executor │ Audit Trail                   │
└───────────────┬─────────────────────────────────────────────────────────┘
                ▼
       ┌─────────────────┐
       │ l3.actions/audit │  (paper fill results for L4 loop closure)
       └─────────────────┘
```

---

## 1) Purpose and Boundaries

### L4 Purpose
L4 exists to make portfolio decisions measurable and improvable:
- Convert L2 context + market data into **paper portfolio plans**.
- Simulate execution and maintain a **paper portfolio** with correct accounting.
- Evaluate outcomes using benchmark-relative metrics.
- Learn conservatively: allocate between known strategies based on context.

### In Scope
- Maintain one or more **paper portfolios** (positions, cash, P&L, exposures).
- Implement **All-Weather / risk parity** as the seed strategy.
- Maintain immutable **decision records** and strategy versioning.
- Provide **attribution** split into:
  - Signal/strategy quality (frictionless target-weight return)
  - Sizing/constraints impact (caps, volatility targeting, turnover limits)
  - Execution impact (fees/slippage/latency model)
- Emit evaluation + feedback topics for observability and downstream use.

### Out of Scope
- Real trading / broker connectivity.
- Strategy invention via LLMs, genetic programming, or unconstrained search.
- High-frequency alpha.
- Owning approvals/guardrails (remain L3 responsibilities).

### L4 vs L3 Responsibilities

| Responsibility | L3 | L4 |
|----------------|----|----|
| Guardrails (rate limits, kill switch) | ✓ | |
| Audit trail and action lifecycle | ✓ | |
| Execute approved actions (paper) | ✓ | |
| Decide allocations and portfolio targets | | ✓ |
| Evaluate, attribute, and learn | | ✓ |

L3 is the system "safety kernel". L4 is the portfolio "control plane" that treats L3 as an execution + audit substrate.

---

## 2) Multi-Asset Architecture (Binance + FRED + COT)

### Core Asset Abstraction
L4 uses a unified asset definition with two concrete categories:

| Category | Price Feed | Execution Model |
|----------|------------|-----------------|
| **Tradable (Binance)** | Binance trades/tickers | Fees + slippage + latency |
| **Synthetic (FRED-priced)** | FRED time series | No exchange fees (configurable) |

CME COT is treated as **context** (positioning/crowding), not as an instrument to execute.

### Canonical Asset ID

```
asset_id = "<venue>:<symbol_or_series>"
```

Examples:
- `binance:BTCUSDT`
- `binance:PAXGUSDT`
- `fred:SP500`
- `fred:DGS10_PRICE` (synthetic bond price index derived from yields)

### Market Data Normalization

L4 normalizes all pricing inputs into a canonical "bar" format:
- Default **decision bar**: `1d` (UTC day)
- Optional additional bars: `1h` for crypto-only portfolios

Normalization rules:
- Binance: build daily OHLC from trade/ticker events (coverage tracked)
- FRED: treat series points as daily closes (with publication lag handling)
- Synthetic instruments: derive a price index from a series (see bonds proxy)

### Publication Lags and "As-Of" Safety

L4 must not use data that wasn't available at decision time.

For every feature or price used in a plan, store:
- `effective_at`: the date/time the value refers to (e.g., observation date)
- `available_at`: when the system actually ingested/observed it

At decision time `t_decision`, L4 may only use values with `available_at <= t_decision`.

---

## 3) All-Weather Asset Universe (Within Current Sources)

All-Weather requires diversified exposures. L4 supports a pragmatic universe given:
- Binance for crypto tradables
- FRED for equities/bonds/commodities proxies
- COT for weekly positioning context

### A) Binance Tradables

| Role | Asset ID | Notes |
|------|----------|-------|
| Growth / risk-on proxy | `binance:BTCUSDT` | High beta; 24/7 |
| Growth / risk-on proxy | `binance:ETHUSDT` | Diversifies within crypto |
| Gold proxy | `binance:PAXGUSDT` | Tokenized gold proxy |
| Cash leg | `binance:USDT` | Base currency; no yield in MVP |

### B) FRED-Priced Synthetic Paper Instruments

| Asset Class | Asset ID | Example FRED Series | Notes |
|-------------|----------|---------------------|-------|
| Equities proxy | `fred:SP500` | `SP500` | Daily index level |
| Bonds proxy | `fred:DGS10_PRICE` | `DGS10` | Synthetic price from yields |
| Gold | `fred:GOLD_PRICE` | `GOLDAMGBD228NLBM` | Daily |
| Oil / commodities | `fred:WTI_OIL` | `DCOILWTICO` | Daily |
| USD strength (optional) | `fred:DXY_PROXY` | `DTWEXBGS` | Daily |

#### Bond Price Proxy Definition

FRED provides yields more reliably than total return indexes. L4 defines a synthetic bond price index:

```
P_t = P_{t-1} * exp(-D * (y_t - y_{t-1}))
```

Where:
- `y_t` is yield (decimal, e.g., 0.042)
- `D` is effective duration (configurable; default 7 for 10Y)

This is an approximation (no carry/roll), but sufficient for a defensive rates sleeve.

### C) CME COT Positioning (Context Only)

COT is used to estimate crowding:
- Map each COT market to an L4 sleeve (rates/equities/commodities/currencies)
- Derive crowding score (z-score of net positioning vs 3y history)
- Penalize allocations into crowded exposures or reduce portfolio risk target

---

## 4) All-Weather Strategy Implementation (Risk Parity)

### Strategy Identity

```yaml
strategy_id: all_weather_risk_parity
strategy_version: 1.0.0  # semver, immutable per version
```

### Objective

Create a diversified portfolio where each sleeve contributes roughly equal **risk**, not equal capital.

### Sleeves (Default Mapping)

| Sleeve | Assets | Role |
|--------|--------|------|
| Growth | `fred:SP500`, `binance:BTCUSDT`, `binance:ETHUSDT` | Risk-on |
| Deflation/Rates | `fred:DGS10_PRICE` | Duration hedge |
| Inflation | `fred:WTI_OIL`, `fred:GOLD_PRICE`, `binance:PAXGUSDT` | Inflation hedge |
| Cash | `binance:USDT` | Drawdown brake |

**Fallback (if FRED missing):**
- "Crypto risk parity" across `BTCUSDT`, `ETHUSDT`, optional `PAXGUSDT`, plus cash
- Must set `data_quality.fred_available=false` in evaluations

### Risk Estimation (Decision Bar = 1d)

- **Returns**: log returns for each asset
- **Volatility**: EWMA or rolling stdev (default 20 bars)
- **Covariance**: rolling covariance (default 60 bars), shrunk toward diagonal

### Weight Construction (MVP)

Two-stage risk parity:

**1) Within-sleeve inverse-vol:**
```
w_i ∝ 1 / max(vol_i, vol_floor)
```

**2) Sleeve risk budgets (default equal + cash floor):**
- Growth: 25%
- Deflation/rates: 25%
- Inflation: 25%
- Cash: 25%

**3) Portfolio volatility targeting:**
```
scale = target_vol / max(realized_portfolio_vol, vol_floor)
scale = min(scale, 1.0)  # no leverage in MVP
```

### Hard Constraints (Defaults)

| Constraint | Value |
|------------|-------|
| Long-only | Yes |
| Max weight per asset | 0.35 |
| Min weight cash | 0.10 |
| Max turnover per rebalance | 0.20 |

### Rebalancing Cadence

- **Weekly** (preferred for All-Weather, aligns with COT updates)
- **Daily** (optional for crypto-heavy / faster regime shifts)

---

## 5) Paper Trading Infrastructure

L4 is event-driven and replayable. It maintains separable subsystems.

### A) Market Data Normalizer

Responsibilities:
- Build daily bars for Binance tradables from `l1.signals.enriched`
- Convert FRED series points into daily "close" price-like values
- Maintain last-known price with coverage/quality metadata

### B) Portfolio + Accounting Engine

Responsibilities:
- Track positions, cash, average cost, realized/unrealized P&L
- Track fees and modeled slippage separately
- Support multiple portfolios (e.g., `paper-main`, `paper-shadow`)

Accounting rules (MVP):
- Base currency: USDT
- Long-only
- Mark-to-market at bar close
- Synthetic FRED instruments marked from FRED prices; no exchange fee by default

### C) Paper Execution Simulator

Goal: simulate realistic costs without pretending to be a full exchange.

**Execution Model:**
```
fill_price = ref_price * (1 + side * slippage_bps/10000)
fee = notional * fee_bps/10000
t_exec = t_decision + latency_ms
```

Where `side = +1` buy, `-1` sell.

**Default Parameters:**

| Parameter | Binance | FRED Synthetic |
|-----------|---------|----------------|
| fee_bps | 10 (0.10%) | 0 |
| slippage_bps | 5 (0.05%) | 0 |
| latency_ms | 250 | 0 |

---

## 6) Feedback Loop Design

### The Four Clocks

| Clock | When | Purpose |
|-------|------|---------|
| Decision (t0) | Targets computed | Freeze inputs, as-of safety |
| Execution (t0→t1) | Orders filled | Use prices at/after t_exec |
| Outcome (t1→t2) | Performance scored | Explicit horizons: 1d, 5d, 20d |
| Learning (t2) | Bandit updated | Only when outcomes mature |

### Attribution: Signal vs Sizing vs Execution

For each rebalance, compute three return streams:

| Stream | Definition |
|--------|------------|
| **Signal return** | Target weights, no fees/slippage |
| **Sizing return** | Constrained weights, no fees/slippage |
| **Realized return** | Filled trades with fees/slippage/latency |

Derived metrics:
```
sizing_delta = sizing_return - signal_return
execution_delta = realized_return - sizing_return
```

### Benchmarks (Required)

L4 evaluates every allocation against baselines:
- `benchmark:cash` (100% USDT)
- `benchmark:all_weather_static` (All-Weather without bandit)
- `benchmark:equal_weight_strategies` (static equal across strategies)

Rewards are benchmark-relative:
```
reward = utility(allocation) - utility(benchmark)
```

---

## 7) Conservative Learning Mechanism (Contextual Bandit)

L4 learns allocation weights across a small set of known strategies.

### Strategies (Arms)

| Strategy ID | Description |
|-------------|-------------|
| `all_weather_risk_parity@1.x` | Risk parity across sleeves |
| `cash_only@1.x` | Do-nothing baseline |
| `all_weather_defensive@1.x` | Higher cash, lower target vol (optional) |

### Context Features (Low-Dimensional)

From L2:
- `market_regime`: `risk_on|risk_off|mixed`
- `macro_risk_level`: `low|elevated|high`

From L4 portfolio state:
- `drawdown_bucket`: `0-5%|5-10%|10%+`
- `vol_bucket`: `low|medium|high`

### Reward / Utility

```
utility = avg_daily_return
        - lambda_dd * max_drawdown
        - lambda_turn * turnover
```

**MVP:** `utility = avg_daily_return - 2.0 * max_drawdown`

Reward is benchmark-relative (default: cash).

### Bandit Policy: Context-Bucketed UCB

Per (context bucket, arm), maintain: `n`, `mean_reward`, `reward_std`

Score:
```
score = mean_reward + ucb_c * sqrt(log(1 + total_n) / (1 + n))
```

Convert to weights via softmax, then apply:
- `min_weight_cash`: 20%
- `max_weight_any_arm`: 80%
- Shrink toward equal weights when sample counts are small

### Update Gating

- Updates only when outcomes mature (e.g., 5d horizon completed)
- Skip update or apply strong shrinkage if data quality degraded

---

## 8) Kafka Topics and Schemas

### Topic List

| Topic | Purpose | Key |
|-------|---------|-----|
| `l4.plans` | Portfolio target plans | `portfolio_id` |
| `l4.portfolio.snapshots` | Stateful portfolio state | `portfolio_id` |
| `l4.evaluations` | Performance + attribution | `portfolio_id` |
| `l4.bandit.state` | Compacted learner state | `bandit_id` |
| `l4.market.bars` | Normalized bars (optional) | `asset_id` |

### `l4.plans` Schema

```json
{
  "id": "uuid",
  "timestamp": "2026-02-03T00:00:30Z",
  "portfolio_id": "paper-main",
  "plan_type": "rebalance",
  "decision_time": "2026-02-03T00:00:30Z",
  "execution_time": "2026-02-03T00:01:00Z",
  "rebalance_bar": "1d",

  "context": {
    "market_regime": "risk_off",
    "macro_risk_level": "elevated",
    "drawdown_bucket": "0-5%",
    "vol_bucket": "medium",
    "asof": {
      "l2_situations_available_at": "2026-02-03T00:00:10Z",
      "market_prices_available_at": "2026-02-03T00:00:00Z",
      "fred_available_at": "2026-02-02T22:00:00Z",
      "cot_available_at": "2026-01-31T21:00:00Z"
    }
  },

  "allocation": {
    "strategy_set": [
      {"strategy_id": "all_weather_risk_parity", "strategy_version": "1.0.0"},
      {"strategy_id": "cash_only", "strategy_version": "1.0.0"}
    ],
    "strategy_weights": {
      "all_weather_risk_parity@1.0.0": 0.65,
      "cash_only@1.0.0": 0.35
    },
    "why": [
      "risk_off regime -> increase cash",
      "all_weather positive reward last 20d in this context"
    ]
  },

  "targets": {
    "base_currency": "USDT",
    "target_vol_annual": 0.10,
    "max_turnover": 0.20,
    "target_weights": {
      "binance:BTCUSDT": 0.12,
      "binance:ETHUSDT": 0.08,
      "binance:PAXGUSDT": 0.10,
      "fred:SP500": 0.20,
      "fred:DGS10_PRICE": 0.25,
      "fred:WTI_OIL": 0.10,
      "binance:USDT": 0.15
    }
  },

  "constraints": {
    "no_leverage": true,
    "long_only": true,
    "max_weight_per_asset": 0.35,
    "min_weight_cash": 0.10
  },

  "metadata": {
    "schema_version": "4.0.0",
    "l4_version": "1.0.0",
    "correlation_id": "trace-uuid",
    "dry_run": true
  }
}
```

### `l4.portfolio.snapshots` Schema

```json
{
  "id": "uuid",
  "timestamp": "2026-02-03T00:02:00Z",
  "portfolio_id": "paper-main",
  "asof": "2026-02-03T00:01:30Z",

  "positions": [
    {
      "asset_id": "binance:BTCUSDT",
      "quantity": 0.0123,
      "avg_cost": 42000.0,
      "mark_price": 42150.0,
      "market_value": 518.4
    }
  ],

  "cash": {
    "currency": "USDT",
    "balance": 10000.0
  },

  "pnl": {
    "unrealized": 12.5,
    "realized": -3.2,
    "fees": 1.8,
    "slippage": 0.9,
    "total": 6.6
  },

  "risk": {
    "gross_exposure": 0.85,
    "net_exposure": 0.85,
    "realized_vol_20d": 0.11,
    "max_drawdown_60d": 0.06
  },

  "metadata": {
    "schema_version": "4.0.0",
    "l4_version": "1.0.0"
  }
}
```

### `l4.evaluations` Schema

```json
{
  "id": "uuid",
  "timestamp": "2026-02-08T00:10:00Z",
  "portfolio_id": "paper-main",
  "plan_id": "plan-uuid",

  "horizon": "5d",
  "decision_time": "2026-02-03T00:00:30Z",
  "outcome_time": "2026-02-08T00:00:30Z",

  "benchmarks": {
    "cash": {"return": 0.0000},
    "all_weather_static": {"return": 0.0125}
  },

  "results": {
    "realized_return": 0.0108,
    "signal_return": 0.0132,
    "sizing_return": 0.0120,
    "sizing_delta": -0.0012,
    "execution_delta": -0.0012
  },

  "attribution": {
    "by_asset": [
      {"asset_id": "fred:DGS10_PRICE", "contribution": 0.0041},
      {"asset_id": "binance:BTCUSDT", "contribution": 0.0028}
    ],
    "by_sleeve": [
      {"sleeve": "deflation", "contribution": 0.0041},
      {"sleeve": "growth", "contribution": 0.0049}
    ]
  },

  "data_quality": {
    "fred_available": true,
    "cot_available": true,
    "binance_price_coverage": 0.999,
    "notes": []
  },

  "metadata": {
    "schema_version": "4.0.0",
    "l4_version": "1.0.0",
    "correlation_id": "trace-uuid"
  }
}
```

### `l4.bandit.state` Schema (Compacted)

```json
{
  "bandit_id": "paper-main:strategy_allocator",
  "timestamp": "2026-02-08T00:12:00Z",

  "context_schema": {
    "market_regime": ["risk_on", "risk_off", "mixed"],
    "macro_risk_level": ["low", "elevated", "high"],
    "drawdown_bucket": ["0-5%", "5-10%", "10%+"],
    "vol_bucket": ["low", "medium", "high"]
  },

  "arms": [
    "all_weather_risk_parity@1.0.0",
    "cash_only@1.0.0"
  ],

  "stats": [
    {
      "context": {
        "market_regime": "risk_off",
        "macro_risk_level": "elevated",
        "drawdown_bucket": "0-5%",
        "vol_bucket": "medium"
      },
      "arm": "all_weather_risk_parity@1.0.0",
      "n": 12,
      "mean_reward": 0.0031,
      "reward_std": 0.0075
    }
  ],

  "config": {
    "policy": "ucb_bucketed",
    "ucb_c": 0.8,
    "min_weight_cash": 0.20,
    "max_weight_any_arm": 0.80,
    "shrinkage_prior_strength": 10
  },

  "metadata": {
    "schema_version": "4.0.0",
    "l4_version": "1.0.0"
  }
}
```

### `l4.market.bars` Schema (Optional)

```json
{
  "id": "uuid",
  "timestamp": "2026-02-03T00:00:00Z",
  "bar": "1d",
  "asset_id": "binance:BTCUSDT",
  "open": 42000.0,
  "high": 43000.0,
  "low": 41800.0,
  "close": 42500.0,
  "volume": 1234.56,
  "metadata": {
    "schema_version": "4.0.0",
    "source": "l1.signals.enriched",
    "coverage": 0.995
  }
}
```

---

## 9) Integration With L2/L3

### Inputs L4 Consumes

| Source | Topic | Usage |
|--------|-------|-------|
| L2 | `l2.situations` | Regime + macro risk (bandit context) |
| L2 | `l2.insights` | Event-driven overlays (optional crisis mode) |
| L1 | `l1.signals.enriched` (binance) | Tradable pricing |
| L1 | `l1.signals.enriched` (fred) | Synthetic asset pricing |
| L1 | `l1.signals.enriched` (cme_cot) | Positioning features |
| L3 | `l3.actions` | Paper execution results (fills) |
| L3 | `l3.audit` | Guardrail triggers |

### Outputs L4 Produces

| Destination | Topic | Purpose |
|-------------|-------|---------|
| L3 | `l4.plans` | Portfolio rebalance plans |
| L2 (optional) | `l4.evaluations` | Calibrate regime confidence |

### L3 Paper Executor Contract

L3 consumes `l4.plans` and emits `l3.proposals` / `l3.actions`:

- **Proposal action type**: `execute_paper_trade`
- **Proposal parameters**:
  - `portfolio_id`
  - `plan_id`
  - `orders`: list of order intents (delta between current and target weights)
- **Action result output**:
  - `plan_id`
  - `fills`: per-asset fills (price, quantity, fee, slippage, timestamps)
  - `rejected_orders` with reason codes

### Correlation and Traceability

Every L4 plan propagates `correlation_id` into:
- L3 proposal metadata
- L3 action output
- L4 evaluation metadata

---

## 10) Kafka Topic Configuration

```yaml
topics:
  l4.plans:
    partitions: 3
    replication_factor: 1
    retention_ms: 2592000000  # 30 days
    cleanup_policy: delete

  l4.portfolio.snapshots:
    partitions: 3
    replication_factor: 1
    retention_ms: 2592000000  # 30 days
    cleanup_policy: compact

  l4.evaluations:
    partitions: 3
    replication_factor: 1
    retention_ms: 31536000000  # 365 days
    cleanup_policy: delete

  l4.bandit.state:
    partitions: 1
    replication_factor: 1
    retention_ms: -1
    cleanup_policy: compact

  l4.market.bars:
    partitions: 6
    replication_factor: 1
    retention_ms: 2592000000  # 30 days
    cleanup_policy: delete
```

---

## 11) Implementation Phases

### Phase 1: Deterministic Paper Portfolio Foundation
- Consume Binance pricing inputs and produce daily bars
- Implement portfolio accounting + snapshot emission
- Implement paper execution model for Binance assets
- Close loop by consuming `l3.actions` fill outputs

**Exit criteria:**
- Replayable: same inputs => same snapshots
- P&L decomposes correctly (realized/unrealized/fees/slippage)

### Phase 2: All-Weather (Risk Parity) + Benchmarks
- Add FRED synthetic assets (SP500, DGS10 price proxy, gold/oil)
- Implement All-Weather weights (inverse-vol + sleeve budgets + vol targeting)
- Emit evaluations and attribution

**Exit criteria:**
- Benchmark-relative performance computed
- Attribution fields consistently populated

### Phase 3: Contextual Bandit Allocation
- Add `cash_only` baseline strategy
- Implement bucketed UCB + shrinkage + caps
- Emit `l4.bandit.state` and embed weights in `l4.plans`

**Exit criteria:**
- Bandit updates only on matured outcomes
- Stable behavior in low-sample contexts

---

## 12) Directory Structure

```
python/
  l4_paper/
    __init__.py
    main.py                 # Orchestrates consumers + scheduling
    config.py
    schemas/
      plan.py
      snapshot.py
      evaluation.py
      bandit_state.py
      market_bar.py
    market/
      bars.py               # Daily bars from Binance inputs
      fred_synth.py         # Synthetic instruments from FRED
    portfolio/
      engine.py             # Positions, cash, P&L
      accounting.py
    execution/
      simulator.py          # Fees/slippage/latency model
    strategies/
      all_weather.py
      cash_only.py
    learning/
      bandit.py             # Bucketed UCB + shrinkage
    evaluation/
      evaluator.py          # Horizons, attribution, benchmarks
```

---

## 13) Environment Variables

```bash
# Kafka
L4_KAFKA_BROKERS=redpanda:9092
L4_KAFKA_CONSUMER_GROUP=l4-paper

# Input topics
L4_INPUT_SIGNALS_TOPIC=l1.signals.enriched
L4_INPUT_SITUATIONS_TOPIC=l2.situations
L4_INPUT_INSIGHTS_TOPIC=l2.insights
L4_INPUT_L3_ACTIONS_TOPIC=l3.actions
L4_INPUT_L3_AUDIT_TOPIC=l3.audit

# Output topics
L4_OUTPUT_PLANS_TOPIC=l4.plans
L4_OUTPUT_SNAPSHOTS_TOPIC=l4.portfolio.snapshots
L4_OUTPUT_EVALUATIONS_TOPIC=l4.evaluations
L4_OUTPUT_BANDIT_STATE_TOPIC=l4.bandit.state
L4_OUTPUT_MARKET_BARS_TOPIC=l4.market.bars

# Portfolio / timing
L4_PORTFOLIO_ID=paper-main
L4_DECISION_BAR=1d
L4_REBALANCE_CADENCE=weekly
L4_DECISION_TIME_UTC=00:00:30
L4_EXECUTION_TIME_UTC=00:01:00

# Execution model (Binance)
L4_BINANCE_FEE_BPS=10
L4_BINANCE_SLIPPAGE_BPS=5
L4_BINANCE_LATENCY_MS=250

# Risk model
L4_VOL_WINDOW_BARS=20
L4_COV_WINDOW_BARS=60
L4_VOL_FLOOR=0.0005
L4_TARGET_VOL_ANNUAL=0.10
L4_MAX_WEIGHT_PER_ASSET=0.35
L4_MAX_TURNOVER=0.20
L4_MIN_WEIGHT_CASH=0.10

# Bandit
L4_BANDIT_POLICY=ucb_bucketed
L4_BANDIT_UCB_C=0.8
L4_BANDIT_MIN_WEIGHT_CASH=0.20
L4_BANDIT_MAX_WEIGHT_ANY_ARM=0.80
L4_BANDIT_SHRINKAGE_PRIOR_STRENGTH=10

L4_LOG_LEVEL=INFO
```

---

## 14) Design Principles / Non-Negotiables

1. **Fewer strategies, better evaluation** - Prefer quality over quantity
2. **Data quality is first-class** - Missing/late macro data propagates into evaluation and learning
3. **Learner never bypasses constraints** - Caps, cash minimum, no leverage enforced after allocation
4. **Horizon-gated updates** - No peeking; benchmark-relative rewards
5. **Auditability** - Every decision traceable via correlation_id
6. **Replayability** - Same inputs must produce same outputs
