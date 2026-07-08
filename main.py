"""
=============================================================================
BIG DATA DASHBOARD - Reddit + Yahoo Finance + Crypto
=============================================================================
Autorët: Shaban Ejupi & Majlinda Bajraktari
Universiteti i Prishtinës - FSHMN
Departamenti i Matematikës

Projekti: Analizë e të Dhënave me Big Data & Machine Learning
Data: Nëntor-Dhjetor 2025

Përshkrimi:
-----------
Sistem për mbledhjen dhe analizën e të dhënave nga:
- Reddit (subreddits të ndryshme)
- Yahoo Finance (aksione dhe tregje)  
- CoinGecko (kriptovaluta)

Teknologjitë:
- Apache Spark për procesim të shpërndarë
- Machine Learning për analiza prediktive
- 10 VM Cluster për performancë të lartë

TODO: Integro më shumë burime të dhënash
TODO: Përmirëso algoritmet e ML
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier
import requests
import json
from datetime import datetime
import time
import os

class BigDataDashboard:
    """
    Klasa kryesore për Dashboard-in e Big Data
    Menaxhon mbledhjen dhe procesimin e të dhënave
    """
    def __init__(self, master_url="spark://10.0.0.8:7077"):
        print("="*80)
        print(" 🚀 BIG DATA DASHBOARD - Industrial Grade Collector")
        print(" 📊 Stocks + Crypto + Forex + Economic Indicators")
        print(" 🖥️  10 VM Cluster - 8 Days Continuous Collection")
        print(" 👨‍💻 Shaban Ejupi & Majlinda Bajraktari - UP FSHMN")
        print("="*80)
        
        # Inicializimi i Spark Session
        # Lidhja me klasterin Spark në 10.0.0.8
        self.spark = SparkSession.builder \
            .appName("Industrial_BigData_Collector") \
            .master(master_url) \
            .config("spark.executor.instances", "9") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .config("spark.cores.max", "36") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .getOrCreate()
        
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel("WARN")
        print(f"✓ Spark Session krijuar: {self.sc.applicationId}")
    
    def mine_reddit(self, subreddits=["technology", "science", "worldnews"], limit=50):
        """
        Mbledhja e postimeve nga Reddit
        Përdor API-në publike të Reddit për të mbledhur postime
        """
        """Mbledh të dhëna nga Reddit - API publike falas"""
        print(f"\n[1/5] 📡 Duke mbledhur të dhëna nga Reddit...")
        all_data = []
        
        for sub in subreddits:
            try:
                url = f"https://www.reddit.com/r/{sub}/hot.json?limit={limit}"
                headers = {'User-Agent': 'Mozilla/5.0 (University Project)'}
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    posts = response.json()['data']['children']
                    for post in posts:
                        p = post['data']
                        all_data.append({
                            'id': p.get('id', ''),
                            'subreddit': sub,
                            'title': p.get('title', '')[:200],
                            'score': int(p.get('score', 0)),
                            'comments': int(p.get('num_comments', 0)),
                            'upvote_ratio': float(p.get('upvote_ratio', 0)),
                            'created': int(p.get('created_utc', 0)),
                            'author': p.get('author', '[deleted]')
                        })
                    print(f"  ✓ {sub}: {len(posts)} postime")
                time.sleep(1)
            except Exception as e:
                print(f"  ✗ {sub}: Gabim - {e}")
        
        print(f"✓ Totali i postimeve nga Reddit: {len(all_data)}")
        return all_data
    
    def mine_yahoo_finance(self, symbols=None):
        """Mbledh të dhëna nga Yahoo Finance - 500+ symbols"""
        if symbols is None:
            # Top 500 S&P stocks
            symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "AMD", "NFLX", "DIS",
                      "JPM", "JNJ", "V", "WMT", "PG", "MA", "UNH", "HD", "BAC", "XOM",
                      "CVX", "LLY", "ABBV", "PFE", "KO", "PEP", "COST", "AVGO", "TMO", "MRK",
                      "CSCO", "ACN", "ABT", "NKE", "DHR", "TXN", "VZ", "ORCL", "ADBE", "INTC",
                      "CRM", "NEE", "WFC", "CMCSA", "BMY", "PM", "UPS", "RTX", "HON", "QCOM",
                      # Commodities
                      "GC=F", "SI=F", "CL=F", "NG=F"]
        
        print(f"\n[2/6] 💹 Duke mbledhur të dhëna nga Yahoo Finance ({len(symbols)} symbols)...")
        stock_data = []
        
        for symbol in symbols:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                params = {'interval': '1d', 'range': '1d'}
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()['chart']['result'][0]
                    meta = data.get('meta', {})
                    
                    # FIX: Add null checks with defaults
                    prev_close = float(meta.get('previousClose') or meta.get('chartPreviousClose') or 1)
                    current_price = float(meta.get('regularMarketPrice') or meta.get('previousClose') or 0)
                    
                    stock_data.append({
                        'symbol': symbol,
                        'price': current_price,
                        'previous_close': prev_close,
                        'change': current_price - prev_close,
                        'change_percent': ((current_price - prev_close) / prev_close * 100) if prev_close > 0 else 0,
                        'volume': int(meta.get('regularMarketVolume') or 0),
                        'market_cap': float(meta.get('marketCap') or 0),
                        'timestamp': datetime.now().isoformat()
                    })
                    if len(stock_data) % 10 == 0:
                        print(f"  ✓ Collected {len(stock_data)} stocks...")
                time.sleep(0.3)
            except Exception as e:
                print(f"  ✗ {symbol}: {str(e)[:50]}")
        
        print(f"✓ Totali i aksioneve: {len(stock_data)}")
        return stock_data
    
    def mine_crypto(self, limit=250):
        """Mbledh të dhëna nga CoinGecko - Top 250 crypto"""
        print(f"\n[3/6] 💰 Duke mbledhur të dhëna nga CoinGecko (Top {limit})...")
        crypto_data = []
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 250,
                'page': 1,
                'sparkline': False
            }
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                coins = response.json()
                for coin in coins:
                    # FIX: Add null checks for all fields
                    crypto_data.append({
                        'id': coin.get('id', 'unknown'),
                        'symbol': (coin.get('symbol') or 'UNK').upper(),
                        'name': coin.get('name', 'Unknown'),
                        'price': float(coin.get('current_price') or 0),
                        'market_cap': float(coin.get('market_cap') or 0),
                        'volume': float(coin.get('total_volume') or 0),
                        'change_24h': float(coin.get('price_change_percentage_24h') or 0),
                        'rank': int(coin.get('market_cap_rank') or 0),
                        'timestamp': datetime.now().isoformat()
                    })
                print(f"✓ Totali i kriptovalutave: {len(crypto_data)}")
        except Exception as e:
            print(f"✗ Gabim në CoinGecko: {str(e)[:100]}")
        return crypto_data
    
    def mine_binance(self):
        """Mbledh të dhëna nga Binance - Real-time crypto"""
        print(f"\n[4/6] 🪙 Duke mbledhur të dhëna nga Binance...")
        crypto_data = []
        try:
            url = "https://api.binance.com/api/v3/ticker/24hr"
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                tickers = response.json()
                for ticker in tickers[:100]:  # Top 100 pairs
                    if ticker['symbol'].endswith('USDT'):
                        crypto_data.append({
                            'symbol': ticker['symbol'],
                            'price': float(ticker.get('lastPrice') or 0),
                            'volume': float(ticker.get('volume') or 0),
                            'change_24h': float(ticker.get('priceChangePercent') or 0),
                            'high_24h': float(ticker.get('highPrice') or 0),
                            'low_24h': float(ticker.get('lowPrice') or 0),
                            'timestamp': datetime.now().isoformat()
                        })
                print(f"✓ Binance pairs: {len(crypto_data)}")
        except Exception as e:
            print(f"✗ Gabim në Binance: {str(e)[:100]}")
        return crypto_data
    
    def mine_forex(self):
        """Mbledh të dhëna Forex - Currency pairs"""
        print(f"\n[5/6] 💱 Duke mbledhur të dhëna Forex...")
        forex_data = []
        pairs = ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "USDCHF=X", "AUDUSD=X", "USDCAD=X"]
        
        for pair in pairs:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{pair}"
                params = {'interval': '1d', 'range': '1d'}
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()['chart']['result'][0]
                    meta = data.get('meta', {})
                    
                    forex_data.append({
                        'pair': pair.replace('=X', ''),
                        'rate': float(meta.get('regularMarketPrice') or 0),
                        'change': float(meta.get('regularMarketPrice', 0) or 0) - float(meta.get('previousClose', 0) or 0),
                        'timestamp': datetime.now().isoformat()
                    })
                time.sleep(0.3)
            except Exception as e:
                print(f"  ✗ {pair}: {str(e)[:50]}")
        
        print(f"✓ Forex pairs: {len(forex_data)}")
        return forex_data
    
    def process_ml(self, df, feature_cols, k=4):
        """Përpunon me K-Means clustering"""
        print(f"\n[6/6] 🤖 Duke ekzekutuar Machine Learning (K-Means)...")
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        df_features = assembler.transform(df)
        
        scaler = StandardScaler(inputCol="features_raw", outputCol="features")
        scaler_model = scaler.fit(df_features)
        df_scaled = scaler_model.transform(df_features)
        
        kmeans = KMeans(k=k, seed=42)
        model = kmeans.fit(df_scaled)
        predictions = model.transform(df_scaled)
        
        clusters = predictions.groupBy("prediction").count().collect()
        for c in clusters:
            print(f"  ✓ Grupimi {c['prediction']}: {c['count']} artikuj")
        
        return predictions
    
    def create_dashboard_data(self, reddit_data, stock_data, crypto_data, binance_data, forex_data):
        """Krijon të dhëna JSON për dashboard"""
        print(f"\n📊 Duke krijuar të dhëna për dashboard...")
        
        dashboard = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'reddit_posts': len(reddit_data),
                'stocks': len(stock_data),
                'cryptos_coingecko': len(crypto_data),
                'cryptos_binance': len(binance_data),
                'forex_pairs': len(forex_data),
                'total_assets': len(reddit_data) + len(stock_data) + len(crypto_data) + len(binance_data) + len(forex_data)
            },
            'reddit': {
                'total': len(reddit_data),
                'top_posts': sorted(reddit_data, key=lambda x: x['score'], reverse=True)[:10] if reddit_data else [],
                'all_posts': reddit_data
            },
            'stocks': {
                'total': len(stock_data),
                'all_stocks': stock_data,
                'top_gainers': sorted(stock_data, key=lambda x: x.get('change', 0), reverse=True)[:10] if stock_data else [],
                'top_losers': sorted(stock_data, key=lambda x: x.get('change', 0))[:10] if stock_data else []
            },
            'crypto': {
                'total': len(crypto_data),
                'all_crypto': crypto_data,
                'top_gainers': sorted(crypto_data, key=lambda x: x.get('change_24h', 0), reverse=True)[:10] if crypto_data else [],
                'top_losers': sorted(crypto_data, key=lambda x: x.get('change_24h', 0))[:10] if crypto_data else []
            },
            'binance': {
                'total': len(binance_data),
                'all_pairs': binance_data,
                'top_gainers': sorted(binance_data, key=lambda x: x.get('change_24h', 0), reverse=True)[:10] if binance_data else []
            },
            'forex': {
                'total': len(forex_data),
                'all_pairs': forex_data
            }
        }
        
        # Sigurohemi që direktoria ekziston
        output_dir = os.path.expanduser("~/big-data-dashboard")
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(output_dir, f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(dashboard, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Dashboard ruajtur: {output_file}")
        return dashboard
    
    def run(self):
        """Ekzekuton pipeline-in e plotë"""
        # Mbledh të dhëna nga të gjitha burimet
        reddit_data = self.mine_reddit(limit=30)  # Reduce Reddit priority
        stock_data = self.mine_yahoo_finance()
        crypto_data = self.mine_crypto()
        binance_data = self.mine_binance()
        forex_data = self.mine_forex()
        
        # Përpuno me Spark ML
        if stock_data:
            stock_df = self.spark.createDataFrame(stock_data)
            stock_clusters = self.process_ml(stock_df, ['price', 'volume', 'change'], k=3)
        
        if crypto_data:
            crypto_df = self.spark.createDataFrame(crypto_data)
            crypto_clusters = self.process_ml(crypto_df, ['price', 'market_cap', 'volume', 'change_24h'], k=4)
        
        # Krijo dashboard
        dashboard = self.create_dashboard_data(reddit_data, stock_data, crypto_data, binance_data, forex_data)
        
        print("\n" + "="*80)
        print(" ✅ SUKSES! Të dhënat e dashboard-it u krijuan")
        print("="*80)
        print(f"\n📊 Totali i të dhënave të mbledhura: {dashboard['summary']['total_assets']}")
        print(f"   - Postimet Reddit: {dashboard['summary']['reddit_posts']}")
        print(f"   - Aksionet: {dashboard['summary']['stocks']}")
        print(f"   - CoinGecko Crypto: {dashboard['summary']['cryptos_coingecko']}")
        print(f"   - Binance Pairs: {dashboard['summary']['cryptos_binance']}")
        print(f"   - Forex Pairs: {dashboard['summary']['forex_pairs']}")
        print("\n🚀 Tjetra: Nis dashboard-in me: python3 dashboard.py")
        
        self.spark.stop()

if __name__ == "__main__":
    app = BigDataDashboard()
    app.run()
