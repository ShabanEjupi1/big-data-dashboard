"""
CONTINUOUS DATA COLLECTOR - 8 Days Non-Stop
Collects 5,000 rows every 5 seconds from multiple APIs
Implements API rotation, retry logic, and checkpointing
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import requests
import json
import time
from datetime import datetime, timedelta
import os
import logging

# Setup logging
logging.basicConfig(
    filename='logs/collector.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ContinuousCollector:
    def __init__(self, master_url="spark://10.0.0.8:7077"):
        print("="*80)
        print(" 🚀 CONTINUOUS DATA COLLECTOR")
        print(" 📊 Target: 700M+ rows over 8 days")
        print("="*80)
        
        try:
            self.spark = SparkSession.builder \
                .appName("Continuous_Data_Collector") \
                .master(master_url) \
                .config("spark.executor.instances", "9") \
                .config("spark.executor.cores", "4") \
                .config("spark.executor.memory", "8g") \
                .config("spark.driver.memory", "8g") \
                .config("spark.streaming.backpressure.enabled", "true") \
                .getOrCreate()
            
            print(f"✓ Connected to Spark cluster: {master_url}")
            logging.info(f"Connected to Spark cluster: {master_url}")
        except Exception as e:
            print(f"❌ Failed to connect to Spark cluster: {e}")
            logging.error(f"Failed to connect to Spark cluster: {e}")
            raise
        
        self.checkpoint_dir = "/home/krenuser/big-data-dashboard/checkpoints/"
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        os.makedirs("data/stocks", exist_ok=True)
        os.makedirs("data/crypto", exist_ok=True)
        os.makedirs("data/forex", exist_ok=True)
        
        self.spark.sparkContext.setCheckpointDir(self.checkpoint_dir)
        print(f"✓ Directories created and checkpoint set")
        logging.info("Initialization complete")
        
        # API limits and rotation
        self.api_stats = {
            'yahoo': {'calls': 0, 'errors': 0, 'last_call': None},
            'coingecko': {'calls': 0, 'errors': 0, 'last_call': None, 'limit_per_min': 50},
            'binance': {'calls': 0, 'errors': 0, 'last_call': None},
        }
        
        # Top 500 S&P symbols
        self.stock_symbols = self._load_sp500_symbols()
        self.crypto_batch_size = 100
        self.collection_count = 0
        
    def _load_sp500_symbols(self):
        """Load S&P 500 stock symbols"""
        # Reduced to 20 top symbols to avoid rate limiting
        return [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "V", "JNJ",
            "XOM", "WMT", "PG", "MA", "CVX", "HD", "LLY", "ABBV", "KO", "PFE"
        ]
    
    def fetch_with_retry(self, url, params=None, max_retries=3, timeout=10):
        """Fetch data with retry logic"""
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=timeout)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limited
                    time.sleep(5 * (attempt + 1))
                    continue
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"Failed after {max_retries} attempts: {url} - {e}")
                    return None
                time.sleep(2 * (attempt + 1))
        return None
    
    def collect_stocks(self, batch_size=10):
        """Collect stock data from Yahoo Finance with rate limiting protection"""
        import random
        stock_data = []
        symbols_batch = self.stock_symbols[:batch_size]
        
        for symbol in symbols_batch:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                params = {'interval': '1d', 'range': '1d'}
                data = self.fetch_with_retry(url, params)
                
                if data and 'chart' in data and 'result' in data['chart'] and len(data['chart']['result']) > 0:
                    result = data['chart']['result'][0]
                    meta = result.get('meta', {})
                    
                    price = float(meta.get('regularMarketPrice') or meta.get('previousClose') or 0)
                    prev_close = float(meta.get('previousClose') or 1)
                    
                    if price > 0:  # Only add valid data
                        stock_data.append({
                            'symbol': symbol,
                            'price': price,
                            'previous_close': prev_close,
                            'change': price - prev_close,
                            'change_percent': ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0,
                            'volume': int(meta.get('regularMarketVolume') or 0),
                            'market_cap': float(meta.get('marketCap') or 0),
                            'timestamp': datetime.now().isoformat(),
                            'source': 'yahoo'
                        })
                        self.api_stats['yahoo']['calls'] += 1
                # Longer delay to avoid rate limiting (2-4 seconds)
                time.sleep(2 + random.random() * 2)
            except Exception as e:
                self.api_stats['yahoo']['errors'] += 1
                logging.error(f"Stock {symbol} error: {e}")
                time.sleep(3)  # Extra delay on error
        
        return stock_data
    
    def collect_crypto_coingecko(self):
        """Collect crypto from CoinGecko"""
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 250,
                'page': 1,
                'sparkline': False
            }
            data = self.fetch_with_retry(url, params, timeout=30)
            
            if data:
                crypto_data = []
                for coin in data:
                    crypto_data.append({
                        'id': coin.get('id', 'unknown'),
                        'symbol': (coin.get('symbol') or 'UNK').upper(),
                        'name': coin.get('name', 'Unknown'),
                        'price': float(coin.get('current_price') or 0),
                        'market_cap': float(coin.get('market_cap') or 0),
                        'volume': float(coin.get('total_volume') or 0),
                        'change_24h': float(coin.get('price_change_percentage_24h') or 0),
                        'rank': int(coin.get('market_cap_rank') or 0),
                        'timestamp': datetime.now().isoformat(),
                        'source': 'coingecko'
                    })
                self.api_stats['coingecko']['calls'] += 1
                return crypto_data
        except Exception as e:
            self.api_stats['coingecko']['errors'] += 1
            logging.error(f"CoinGecko error: {e}")
        return []
    
    def collect_crypto_binance(self):
        """Collect crypto from Binance"""
        try:
            url = "https://api.binance.com/api/v3/ticker/24hr"
            data = self.fetch_with_retry(url, timeout=15)
            
            if data:
                crypto_data = []
                for ticker in data[:100]:  # Top 100 pairs
                    if ticker['symbol'].endswith('USDT'):
                        crypto_data.append({
                            'symbol': ticker['symbol'],
                            'price': float(ticker.get('lastPrice') or 0),
                            'volume': float(ticker.get('volume') or 0),
                            'change_24h': float(ticker.get('priceChangePercent') or 0),
                            'high_24h': float(ticker.get('highPrice') or 0),
                            'low_24h': float(ticker.get('lowPrice') or 0),
                            'timestamp': datetime.now().isoformat(),
                            'source': 'binance'
                        })
                self.api_stats['binance']['calls'] += 1
                return crypto_data
        except Exception as e:
            self.api_stats['binance']['errors'] += 1
            logging.error(f"Binance error: {e}")
        return []
    
    def collect_forex(self):
        """Collect Forex data with rate limiting protection"""
        import random
        forex_data = []
        pairs = ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "USDCHF=X"]
        
        for pair in pairs:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{pair}"
                params = {'interval': '1d', 'range': '1d'}
                data = self.fetch_with_retry(url, params)
                
                if data and 'chart' in data and 'result' in data['chart'] and len(data['chart']['result']) > 0:
                    result = data['chart']['result'][0]
                    meta = result.get('meta', {})
                    
                    rate = float(meta.get('regularMarketPrice') or 0)
                    if rate > 0:  # Only add valid data
                        forex_data.append({
                            'pair': pair.replace('=X', ''),
                            'rate': rate,
                            'change': rate - float(meta.get('previousClose', 0) or 0),
                            'timestamp': datetime.now().isoformat(),
                            'source': 'yahoo'
                        })
                # Longer delay to avoid rate limiting (1-2 seconds)
                time.sleep(1 + random.random())
            except Exception as e:
                logging.error(f"Forex {pair} error: {e}")
                time.sleep(2)  # Extra delay on error
        
        return forex_data
    
    def save_to_parquet(self, data, data_type):
        """Save data to partitioned Parquet files"""
        if not data:
            return
        
        try:
            df = self.spark.createDataFrame(data)
            
            # Partition by date and hour
            now = datetime.now()
            date_str = now.strftime("%Y-%m-%d")
            hour_str = now.strftime("%H")
            
            path = f"data/{data_type}/date={date_str}/hour={hour_str}/"
            df.write.mode("append").parquet(path)
            
            logging.info(f"Saved {len(data)} rows to {path}")
        except Exception as e:
            logging.error(f"Error saving {data_type}: {e}")
    
    def collect_batch(self):
        """Collect one batch of data from all sources"""
        start_time = time.time()
        
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Collecting batch #{self.collection_count + 1}...")
        
        # Collect from all sources with crypto priority (no rate limits)
        crypto_cg = self.collect_crypto_coingecko()
        crypto_bn = self.collect_crypto_binance()
        
        # Stocks and forex with careful rate limiting (only every 5th batch to avoid 429)
        stocks = []
        forex = []
        if self.collection_count % 5 == 0:  # Only collect stocks/forex every 25 seconds
            stocks = self.collect_stocks(batch_size=10)
            forex = self.collect_forex()
        
        # Save to Parquet
        self.save_to_parquet(stocks, 'stocks')
        self.save_to_parquet(crypto_cg, 'crypto')
        self.save_to_parquet(crypto_bn, 'crypto')
        self.save_to_parquet(forex, 'forex')
        
        total_rows = len(stocks) + len(crypto_cg) + len(crypto_bn) + len(forex)
        elapsed = time.time() - start_time
        
        self.collection_count += 1
        
        print(f"  ✓ Collected {total_rows} rows in {elapsed:.1f}s")
        print(f"    - Stocks: {len(stocks)}")
        print(f"    - CoinGecko: {len(crypto_cg)}")
        print(f"    - Binance: {len(crypto_bn)}")
        print(f"    - Forex: {len(forex)}")
        
        # Log stats every 100 batches
        if self.collection_count % 100 == 0:
            self.print_stats()
        
        return total_rows
    
    def print_stats(self):
        """Print collection statistics"""
        print("\n" + "="*60)
        print(f" COLLECTION STATS (Batch #{self.collection_count})")
        print("="*60)
        for api, stats in self.api_stats.items():
            print(f"  {api:12} - Calls: {stats['calls']:6d} | Errors: {stats['errors']:4d}")
        print("="*60 + "\n")
    
    def run_continuous(self, duration_days=8):
        """Run continuous collection for specified days"""
        print(f"\n🚀 Starting {duration_days}-day continuous collection")
        print(f"Target: ~{duration_days * 24 * 60 * 12:,} batches (5-sec intervals)")
        print(f"Expected rows: ~{duration_days * 24 * 60 * 12 * 400:,}\n")
        
        end_time = datetime.now() + timedelta(days=duration_days)
        total_rows = 0
        
        try:
            while datetime.now() < end_time:
                rows = self.collect_batch()
                total_rows += rows
                
                # Sleep to maintain 5-second intervals
                time.sleep(5)
                
                # Checkpoint every 5 minutes (60 batches)
                if self.collection_count % 60 == 0:
                    logging.info(f"Checkpoint: {self.collection_count} batches, {total_rows:,} rows")
                    print(f"\n💾 Checkpoint: {total_rows:,} rows collected so far")
        
        except KeyboardInterrupt:
            print("\n\n⚠️  Collection interrupted by user")
        except Exception as e:
            logging.error(f"Fatal error: {e}")
            print(f"\n❌ Error: {e}")
        finally:
            self.print_stats()
            print(f"\n✅ Collection complete: {total_rows:,} total rows")
            self.spark.stop()

if __name__ == "__main__":
    print(f"\n{'='*80}")
    print(f" STARTING CONTINUOUS COLLECTION")
    print(f" Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    try:
        collector = ContinuousCollector()
        collector.run_continuous(duration_days=8)
    except KeyboardInterrupt:
        print("\n\n⚠️  Collection stopped by user (Ctrl+C)")
        logging.info("Collection stopped by user")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        logging.error(f"Fatal error in main: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        raise
