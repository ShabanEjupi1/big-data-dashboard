**Summary**

- **Project:** `Big Data Financial Analytics Dashboard` — real-time financial analytics using Apache Spark on a 10-VM cluster to collect crypto + stock data, produce ML price predictions, investment recommendations, and portfolio optimization. Key scripts: data_collector.py / data_collector_v2.py, ml_predictions.py, investment_recommender.py, dashboard.py.

- **Data collection:** Collectors ingest from CoinGecko, Binance, Yahoo Finance / yfinance, Alpha Vantage, Finnhub, Polygon. Collected records are converted to Spark DataFrames and written as partitioned Parquet under crypto and stocks using folder partitions `date=YYYY-MM-DD/hour=HH`.

- **Typical row fields (examples from attachments):**
  - Crypto (CoinGecko): `change_24h`, `id`, `market_cap`, `name`, `price`, `rank`, `source`, `symbol`, `timestamp`, `volume`, `date`, `hour`
  - Stocks (yfinance): `change`, `change_percent`, `high`, `low`, `market_cap`, `previous_close`, `price`, `source`, `symbol`, `timestamp`, `volume`, `date`, `hour`

- **What the ML actually uses:**
  - Required: **`price`**, **`timestamp`**, **`symbol`** — used to build per-symbol time series.
  - Used for selection/reporting: **`market_cap`**, **`volume`**, **`change_24h`** (if present).
  - Not used as regression features in current code: `high`, `low`, `previous_close`, `change_percent` (unless mapped).

- **ML processing (exact algorithm & parameters):**
  - Loads all parquet files under crypto for the last `days_back` (default 8).
  - For each symbol:
    - Sort by `timestamp`, take up to the last 100 `price` points.
    - Compute SMA(5), SMA(10), SMA(20), SMA(50) (falls back to current price if not enough points).
    - Compute linear trend slope via `np.polyfit(x, trend_prices, 1)` on up-to-100 points.
      - Stable-price guard: if price_range < current_price * 0.001, treat trend = 0.
      - Trend scaling: `time_scale = n_prices / 168`; `adjusted_trend = trend / max(1, time_scale)`.
    - Predictions:
      - `prediction_1h = current_price + adjusted_trend * 1`
      - `prediction_24h = current_price + adjusted_trend * 24`
      - `prediction_7d = current_price + adjusted_trend * 168`
    - Volatility: `std(diff(prices)/prices[:-1]) * 100`
    - Confidence: 30% data-based + 70% volatility-based; clamped 0–100.
    - Minimum 5 data points required; otherwise returns None.

- **End-to-end data flow:**
  1. APIs → collector dicts (fields shown above)
  2. Spark DataFrame creation → parquet files appended under `data/{type}/date=.../hour=.../`
  3. ml_predictions.py reads parquet recursively, concatenates to pandas DF → group by `symbol` → compute predictions per symbol → output JSON-like dicts.

- **Caveats / important details:**
  - No deduplication, resampling, or timezone normalization performed — duplicates and mixed timestamps are retained.
  - Partition folders represent write time; `timestamp` fields are ISO strings set by collectors.
  - The ML is univariate (price series only); volume and market_cap are not used as features for fitting.
  - Stock columns like `change`/`change_percent` are not recognized as `change_24h` by ML unless renamed or code patched.

- **Recommended small improvements (optional):**
  - Add explicit `date` and `hour` columns before writing Parquet.
  - Deduplicate and normalize timestamps (UTC) when writing or when loading in ML.
  - Map `change`/`change_percent` → `change_24h` or make ML read alternative column names.
  - If you want richer models, build a supervised feature matrix (lagged prices, high/low, volume, change_percent) and train a regressor.

- **Next actions I can take (pick one):**
  - Create a small mapping script to convert a stock Parquet/CSV to the crypto-style schema and run `calculate_simple_prediction()` on a symbol.
  - Patch ml_predictions.py to accept `change` / `change_percent` when `change_24h` is missing.
  - Add deduplication and `date`/`hour` injection in `save_to_parquet()`.

**Data inputs / preprocessing**
- Both modules load the last 8 days of Parquet data from `data/crypto/date=.../` and do NOT deduplicate or resample; timestamps are parsed to datetimes.
- Required per-row fields: `symbol`, `timestamp`, `price`, `volume`, `market_cap`, and (where present) `change_24h`. If a symbol has too few points (module-specific thresholds) it's skipped or penalized.

**Portfolio Optimizer (portfolio_optimizer.py)**
- Goal: Provide three strategy portfolios (Conservative, Balanced, Aggressive) using MPT concepts and simple heuristics.
- Key metrics per asset:
  - Expected return: computed from price returns and annualized (uses price diffs, attempts to scale to annual).
  - Volatility: std of returns, annualized.
  - Sharpe ratio: (expected_return - risk_free_rate) / volatility (risk_free_rate default 4%).
- Asset selection and computation:
  - Loads recent data (up to last ~240 points per symbol), filters symbols by market cap (top N), computes metrics in parallel.
  - Skips assets with too few data points.
- Strategy construction:
  - Conservative: choose 8 lowest-volatility assets, allocate by inverse volatility (lower vol → higher weight).
  - Balanced: pick top 10 by Sharpe ratio, assign declining weights (15%,13%,11%,...); then normalize to 100%.
  - Aggressive: pick top 10 by expected return and weight proportional to expected return.
- Portfolio outputs:
  - For each strategy: list of allocations (symbol, name, allocation %, expected_return, risk), aggregated expected_return and volatility (risk).
  - A small recommendations block (human-readable).
- Simplifications / approximations:
  - Portfolio variance/covariance is not computed from cross-asset correlations; risk of portfolio is simplified (mean of component volatilities) rather than full mean-variance optimization.
  - Expected returns and volatility calculations have heuristic scaling (periods_per_year, caps).
  - No explicit constraints optimization (no quadratic programming): MPT is described in comments, but implementation uses heuristics rather than solving the Markowitz optimization problem.
  - Parallel metric computation via ThreadPoolExecutor for speed.

**Investment Recommender (investment_recommender.py)**
- Goal: Produce per-asset scores (Stability, Growth Potential, Liquidity Risk), an overall Investment Score per risk profile, category, human-readable advice and suggested allocation.
- Scores:
  - Stability (0–100): weighted composite of price volatility (35%), volume consistency (25%), trend consistency (20%), and market cap stability (20%).
  - Growth Potential (0–100): combines recent 7-day change, volume trend, and market-cap growth with weights (40/30/30).
  - Liquidity Risk (0–100): based on avg volume bands (<$1M → high risk), volume CV, and slippage proxy from price volatility.
- Investment Score:
  - Weighted combination depending on `risk_profile`:
    - Conservative: stability*0.6 + growth*0.2 - liquidity_risk*0.2
    - Balanced: stability*0.4 + growth*0.4 - liquidity_risk*0.2
    - Aggressive: stability*0.2 + growth*0.6 - liquidity_risk*0.2
  - Clamped to 0–100.
- Categorization rules (examples):
  - Safe Investment: stability >= 70 AND liquidity_risk <= 30
  - Balanced Growth: stability >= 50 AND growth >= 60 AND risk <= 50
  - Growth Opportunity: growth >= 70 AND risk <= 60
  - High Risk / Highly Volatile: other thresholds as coded
- Recommendations:
  - Human-readable advice per category, recommended allocation ranges by category + risk profile (strings like "10-20%").
  - `get_portfolio_recommendations` returns a sorted list of recommendations and a diversification strategy helper (`get_diversification_strategy`) that splits a hypothetical total investment into safe/balanced/growth buckets.
- Simplifications / design choices:
  - All scores are heuristic, rule-based and tuned by fixed weights and thresholds in code.
  - Requires at least minimal data points (varies by function) or returns conservative defaults (e.g., high liquidity risk if insufficient data).
  - No machine learning model — pure deterministic scoring.

**Assumptions & limitations (important)**
- No deduplication, timezone normalization, or resampling — duplicates and irregular timestamps are preserved and affect volatility/return calculations.
- Portfolio risk is approximated (mean of volatilities) — no covariance matrix / true MPT optimization (no quadratic programming solver used).
- Expected returns are computed from recent price diffs and aggressively annualized; can be noisy and unstable for short horizons.
- Scores depend on `change_24h`, `volume`, `market_cap` being present and sane; some stock data columns (e.g., `change`, `change_percent`) are not mapped automatically.
- Liquidity risk thresholds are absolute (USD), yet volumes may be in different units or missing for some symbols.
- No transaction costs, slippage modeling beyond a crude proxy, or portfolio rebalancing simulations.
- No risk budgeting or constraints (max weight per asset, sector caps, etc.).

**Practical outputs produced by these modules**
- `portfolio_optimizer.create_simple_portfolio(top_n=15)` → dict with timestamp, `portfolios` (conservative/balanced/aggressive), `assets` (top metrics), `recommendations`.
- `investment_recommender.get_portfolio_recommendations(risk_profile, top_n=30)` → list of recommendations (symbol, scores, category, advice, recommended_allocation), plus helpers `get_diversification_strategy` to allocate a notional amount.