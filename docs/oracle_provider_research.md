## Low-Cost Provider Candidates (Oracle Research)

### Overview
Oracle identified free or inexpensive third-party data sources that can complement the existing ACC L1 ingestion stack (GDELT, Binance, COT, FRED, Telegram) without incurring large licensing costs. The list is grouped by asset class and highlights coverage, access, cost/licensing notes, rate limits/authentication quirks, and MVP-specific pros/cons.

### Crypto Market Data (beyond Binance)
1. **Coinbase Exchange Market Data**
   - **Coverage:** Spot trades/order books/candles for Coinbase-listed pairs.
   - **Access:** REST + WebSocket endpoints.
   - **Cost & Licensing:** Public market data; accept Coinbase market-data terms.
   - **Rate limits:** ~10 req/s per IP (REST); higher for authenticated/private endpoints; HTTP 429 on exceed.
   - **Pros:** Reliable liquidity venue, websocket stream available.
   - **Cons:** Exchange-specific view (no aggregated market coverage).

2. **CoinGecko Public API**
   - **Coverage:** Broad crypto asset metadata, prices, market caps, exchanges.
   - **Access:** REST.
   - **Cost & Licensing:** Free public access; paid tiers for higher throughput.
   - **Rate limits:** ~5–15 calls/min (public); “demo” registration cited for ~30 calls/min.
   - **Pros:** Quick way to enrich with market-wide context.
   - **Cons:** Tight free throughput; not suitable for high-frequency polling.

3. **CoinPaprika API**
   - **Coverage:** Crypto prices/markets/metadata across many assets.
   - **Access:** REST (WebSocket on higher tiers only).
   - **Cost & Licensing:** Free tier (~20k calls/month) explicitly for personal/non-commercial use; commercial usage requires a paid plan.
   - **Rate limits:** Monthly quota; slower refresh on free plan.
   - **Pros:** Simple quotas; wide asset coverage.
   - **Cons:** Licensing likely incompatible if ACC traffic is commercial—check intent.

4. **CryptoCompare / CCData**
   - **Coverage:** Exchange-aggregated crypto market data and optional news feed.
   - **Access:** REST (keyed).
   - **Cost & Licensing:** Free and paid tiers; redistribution restrictions apply.
   - **Rate limits:** Tier-dependent; handle quota exhaustion.
   - **Pros:** Single vendor covers both market data and crypto-news style events.
   - **Cons:** Must manage API key quotas and per-plan limits.

### Commodities (Gold, Silver, Oil)
1. **Alpha Vantage – Commodities Endpoints**
   - **Coverage:** Gold/silver spot, WTI/Brent oil, natural gas, some ag/industrial series.
   - **Access:** REST (JSON/CSV) with API key.
   - **Cost & Licensing:** Free plan limited to ~25 requests/day; higher tiers are paid.
   - **Rate limits:** Very tight daily quota—batch requests and low polling frequency.
   - **Pros:** Single provider also offers equities, FX, crypto, and news endpoints.
   - **Cons:** Free quota is tiny; only practical for daily signals.

2. **U.S. EIA (Energy Information Administration) Open Data**
   - **Coverage:** Official U.S. energy stats (petroleum inventories, production, prices).
   - **Access:** REST API v2 + bulk downloads; API key required.
   - **Cost & Licensing:** Free.
   - **Rate limits:** Not formally published but aggressive polling can suspend keys; result sets capped (e.g., 5k rows) requiring pagination.
   - **Pros:** High-quality macro signals for oil/gas markets.
   - **Cons:** Weekly/daily cadence; not a precious-metals source.

3. **Nasdaq Data Link (formerly Quandl) – e.g., LBMA datasets**
   - **Coverage:** Numerous time-series datasets including LBMA gold/silver fixings.
   - **Access:** REST with API key.
   - **Cost & Licensing:** Many datasets free; premium data available but optional.
   - **Rate limits:** Documented default ~50,000 calls/day with concurrency limit of 1 request at a time.
   - **Pros:** Reliable daily commodity fixings with generous daily caps.
   - **Cons:** Dataset-by-dataset licensing; no real-time updates.

### Equities / Market Breadth / Risk Sentiment
1. **Stooq CSV Endpoints**
   - **Coverage:** EOD/delayed quotes for many equities, ETFs, and global indices.
   - **Access:** Direct CSV downloads via HTTP.
   - **Cost & Licensing:** Free; verify redistribution rights if displaying raw data.
   - **Rate limits:** None published—cache responses to be polite.
   - **Pros:** Extremely simple ingestion for breadth-style signals (advance/decline, index levels).
   - **Cons:** Not an official exchange feed; data quality not guaranteed.

2. **Cboe VIX Historical CSV**
   - **Coverage:** Daily VIX index levels (volatility risk proxy).
   - **Access:** Static CSV download.
   - **Cost & Licensing:** Free.
   - **Rate limits:** None, but should cache to avoid repeated downloads.
   - **Pros:** Adds a strong risk-sentiment signal with zero integration complexity.
   - **Cons:** Single indicator rather than broad equity coverage.

3. **Financial Modeling Prep (FMP)**
   - **Coverage:** Equities (quotes, fundamentals, sector performance, gainers/losers), commodities, crypto, and market news on higher tiers.
   - **Access:** REST API (keyed).
   - **Cost & Licensing:** Free tier ~250 calls/day; inexpensive paid tiers. Redistribution/display requires agreement—emit derived metrics only.
   - **Rate limits:** Keyed quotas per tier; monitor 429s.
   - **Pros:** Provides breadth-style endpoints suitable for MVP; single vendor for multiple asset classes.
   - **Cons:** Strict redistribution terms; limited free throughput.

### Analyst / News / Qualitative Signals
1. **NewsAPI**
   - **Coverage:** Aggregated news headlines (general + business categories).
   - **Access:** REST with API key.
   - **Cost & Licensing:** Free developer plan (~100 requests/day) with ~24h delay; **not permitted for production**—requires paid plan for real-time use.
   - **Rate limits:** 100 calls/day (dev); paid tiers vary.
   - **Pros:** Fast prototyping for headline-based signals.
   - **Cons:** Free plan delayed + dev-only; production pricing climbs quickly.

2. **GNews API**
   - **Coverage:** News search/headlines, finance sources included in catalog.
   - **Access:** REST with API key.
   - **Cost & Licensing:** Free tier (100 requests/day, ~12h delay) for non-commercial/testing; “Essential” paid tier (~€40–50/mo) for production.
   - **Rate limits:** 100 calls/day (free) with 403 after limit; more generous on paid plan.
   - **Pros:** Clear upgrade path from free to inexpensive production tier.
   - **Cons:** Free tier non-commercial and delayed; limited source filtering.

3. **Alpha Vantage – News & Sentiment Endpoint**
   - **Coverage:** Aggregated market news headlines + sentiment scoring.
   - **Access:** REST (same API key as other AV endpoints).
   - **Cost & Licensing:** Free tier shares the overall AV quota (~25 req/day) so budget carefully.
   - **Rate limits:** Same as other AV endpoints.
   - **Pros:** Integrates easily if Alpha Vantage is already in use.
   - **Cons:** Severely limited free throughput; not suitable for broad news coverage.

4. **SEC EDGAR (Company Filings)**
   - **Coverage:** Official U.S. company filings (8-K, 10-Q/K, etc.)
   - **Access:** JSON submissions API + document downloads; no key, but SEC requests a descriptive User-Agent.
   - **Cost & Licensing:** Free/public.
   - **Rate limits:** Informal—conservative throttling advised; HTTP 429 or IP bans possible if abused.
   - **Pros:** High-signal catalysts (earnings, guidance, risk factors) without analyst paywalls.
   - **Cons:** Requires parsing filings; not strictly “news”.

### Notes & Recommendations
- Focus on derived signals: capture deltas, anomalies, or metadata + URLs rather than redistributing raw articles/data (avoids licensing issues).
- Most free tiers are designed for low-frequency polling; implement caching and backoff to stay within quotas.
- Start with 1–2 providers per asset class for MVP (e.g., Coinbase + CoinGecko; Alpha Vantage + Nasdaq Data Link; FMP + Stooq; GNews + EDGAR) and expand once requirements solidify.
