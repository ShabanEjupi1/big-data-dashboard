# Big Data Financial Analytics Dashboard

Real-time financial data analytics system using Apache Spark on a 10-VM cluster for cryptocurrency and stock market analysis.

## Source Code

The source code is available on VM5 at `/home/krenuser/big-data-dashboard`.

## Features

- **Distributed Processing** - Apache Spark cluster with 10 VMs (36 cores, 72GB RAM)
- **Real-time Data Collection** - Continuous data ingestion from CoinGecko, Binance, Yahoo Finance
- **ML Price Predictions** - Linear Regression with Moving Averages for trend forecasting
- **Investment Recommendations** - Multi-factor scoring system for risk-based suggestions
- **Portfolio Optimization** - Modern Portfolio Theory (Markowitz) implementation
- **Web Dashboard** - Flask-based interactive interface with Chart.js visualizations
- **Parquet Storage** - Efficient columnar data storage with date/hour partitioning

## Quick Start

If data is already loaded:

```bash
# Start the web dashboard
python3 dashboard.py

# Access at: http://localhost:5003
```

## Prerequisites

```bash
# Install dependencies
pip install -r requirements.txt

# Ensure Apache Spark is installed at /opt/spark/
# Configure cluster IPs in cluster_manager.py
```

## Start Data Collection

```bash
# Start Spark cluster
python3 cluster_manager.py

# Start data collector (runs continuously)
python3 data_collector.py
```

## Dashboard Usage

The web dashboard provides real-time analytics across multiple views.

**Access the Dashboard:**

```bash
python3 dashboard.py
# Access at: http://localhost:5003 or http://10.0.0.8:5003
```

**Available Pages:**

| Route              | Description                          |
|--------------------|--------------------------------------|
| `/`                | Home - System overview and stats     |
| `/crypto`          | Cryptocurrency analysis              |
| `/stocks`          | Stock market analysis                |
| `/predictions`     | ML price predictions                 |
| `/recommendations` | Investment recommendations           |
| `/portfolio`       | Portfolio optimizer                  |

## Project Structure

```
big-data-dashboard/
├── data_collector.py       # Real-time data collection from APIs
├── data_collector_v2.py    # Enhanced data collector
├── cluster_manager.py      # Spark cluster management
├── dashboard.py            # Flask web server
├── ml_predictions.py       # ML prediction engine
├── investment_recommender.py # Investment scoring system
├── portfolio_optimizer.py  # MPT portfolio optimization
├── templates/              # HTML templates
│   ├── index.html
│   ├── crypto.html
│   ├── stocks.html
│   ├── predictions.html
│   ├── recommendations.html
│   └── portfolio.html
├── data/                   # Parquet data storage
│   ├── crypto/
│   │   └── date=YYYY-MM-DD/hour=HH/
│   └── stocks/
│       └── date=YYYY-MM-DD/hour=HH/
├── checkpoints/            # Spark streaming checkpoints
├── logs/                   # Application logs
└── requirements.txt        # Python dependencies
```

## Data Sources

Collect data from multiple financial APIs:

```bash
# CoinGecko - 250 cryptocurrencies every 5 seconds
# Binance - 100 USDT pairs every 5 seconds
# Yahoo Finance - 20 S&P 500 stocks every 25 seconds
```

**Monitored Stocks:** AAPL, MSFT, GOOGL, AMZN, TSLA, META, NVDA, JPM, V, JNJ, XOM, WMT, PG, MA, CVX, HD, LLY, ABBV, KO, PFE

## Configuration

Edit `cluster_manager.py` to customize:

- Spark Master/Worker IPs
- Executor memory and cores
- VM SSH credentials

Edit `data_collector.py` to customize:

- API endpoints and rate limits
- Data collection intervals
- Storage paths

## Cluster Architecture

| VM   | IP Address | Role           |
|------|------------|----------------|
| VM5  | 10.0.0.8   | Spark Master   |
| VM1  | 10.0.0.4   | Spark Worker   |
| VM2  | 10.0.0.5   | Spark Worker   |
| VM3  | 10.0.0.6   | Spark Worker   |
| VM4  | 10.0.0.7   | Spark Worker   |
| VM6  | 10.0.0.9   | Spark Worker   |
| VM7  | 10.0.0.10  | Spark Worker   |
| VM8  | 10.0.0.11  | Spark Worker   |
| VM9  | 10.0.0.12  | Spark Worker   |
| VM10 | 10.0.0.13  | Spark Worker   |
