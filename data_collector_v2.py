"""
=============================================================================
ENHANCED DATA COLLECTOR - Mbledhësi i Përmirësuar i të Dhënave
=============================================================================
Autorët: Shaban Ejupi & Majlinda Bajraktari
Universiteti i Prishtinës - FSHMN   
Data: Nëntor-Dhjetor 2025

Përshkrimi:
-----------
Ky modul është përgjegjës për mbledhjen e të dhënave në kohë reale nga
API-të e ndryshme financiare duke përdorur Apache Spark për procesim
të shpërndarë në 10 VM.

Burimi kryesor:
- CoinGecko API: 350+ kriptovaluta
- Binance API: Të dhëna shtesë për kripto
- Alpha Vantage: Aksione (kur është i disponueshëm)

TODO: Shto më shumë burime për aksione
TODO: Përmirëso error handling për API failures
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import requests
import json
import time
from datetime import datetime, timedelta
import os
import logging
import random

# Konfigurimi i logging sistemit
logging.basicConfig(
    filename='logs/collector.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class EnhancedCollector:
    """
    Klasa kryesore për mbledhjen e të dhënave financiare
    Përdor Apache Spark për procesim të shpërndarë
    """
    def __init__(self, master_url="spark://10.0.0.8:7077"):
        print("="*80)
        print(" 🚀 ENHANCED DATA COLLECTOR - CRYPTO FOCUSED")
        print(" 📊 Primary: 350+ Cryptos | Secondary: Stocks when available")
        print(" 👨‍💻 Zhvilluar nga: Shaban Ejupi & Majlinda Bajraktari")
        print("="*80)
        
        try:
            # Inicializimi i Spark Session - Lidhja me klasterin
            self.spark = SparkSession.builder \
                .appName("Enhanced_Crypto_Collector") \
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
        
        # API Keys (Free tiers - replace with your keys if needed)
        # To get free API keys:
        # - Finnhub: https://finnhub.io/register (60 calls/minute free)
        # - Alpha Vantage: https://www.alphavantage.co/support/#api-key (500 calls/day free)
        # - Polygon.io: https://polygon.io/ (5 calls/minute free)
        self.api_keys = {
            'alpha_vantage': os.getenv('ALPHA_VANTAGE_KEY', 'demo'),
            'finnhub': os.getenv('FINNHUB_KEY', 'demo'),
            'polygon': os.getenv('POLYGON_KEY', 'demo'),
        }
        
        # API stats
        self.api_stats = {
            'coingecko': {'calls': 0, 'errors': 0, 'last_success': None},
            'binance': {'calls': 0, 'errors': 0, 'last_success': None},
            'finnhub': {'calls': 0, 'errors': 0, 'last_success': None},
            'alpha_vantage': {'calls': 0, 'errors': 0, 'last_success': None},
            'polygon': {'calls': 0, 'errors': 0, 'last_success': None},
            'yfinance_lib': {'calls': 0, 'errors': 0, 'last_success': None},
        }
        
        self.collection_count = 0
        
    def fetch_with_retry(self, url, params=None, max_retries=3, timeout=10):
        """
        Mërr të dhëna nga API me logjikë përsëritjeje
        Nëse API-ja dështon, provon disa herë para se të japimë dorazi
        
        TODO: Shto caching për kërkesa të përsëritura
        """
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=timeout)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limited
                    logging.warning(f"Rate limited: {url}")
                    time.sleep(10 * (attempt + 1))
                    continue
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"Failed after {max_retries} attempts: {url} - {e}")
                    return None
                time.sleep(2 * (attempt + 1))
        return None
    
    def collect_crypto_coingecko(self, per_page=250):
        """
        Mbledh të dhënat e kriptovalutave nga CoinGecko API
        Ky është burimi kryesor për kripto - merr deri 250 monedha
        
        Parametrat:
            per_page: Numri i monedhave për faqe (max 250)
        """
        try:
            # URL e API-së CoinGecko
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': per_page,
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
                self.api_stats['coingecko']['last_success'] = datetime.now()
                return crypto_data
        except Exception as e:
            self.api_stats['coingecko']['errors'] += 1
            logging.error(f"CoinGecko error: {e}")
        return []
    
    def collect_crypto_binance(self, limit=100):
        """Collect crypto from Binance - SECONDARY CRYPTO SOURCE"""
        try:
            url = "https://api.binance.com/api/v3/ticker/24hr"
            data = self.fetch_with_retry(url, timeout=15)
            
            if data:
                crypto_data = []
                usdt_pairs = [t for t in data if t['symbol'].endswith('USDT')][:limit]
                
                for ticker in usdt_pairs:
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
                self.api_stats['binance']['last_success'] = datetime.now()
                return crypto_data
        except Exception as e:
            self.api_stats['binance']['errors'] += 1
            logging.error(f"Binance error: {e}")
        return []
    
    def collect_stocks_yfinance_library(self):
        """
        Përdor librari yfinance - MË I BESUESHËM se thirrjet direkte API
        Mbledhim të dhënat për aksionet më të populluara
        """
        try:
            import yfinance as yf
        except ImportError:
            logging.warning("yfinance not installed. Run: pip install yfinance")
            return []
        
        stock_data = []
        # Lista e simboleve të aksioneve - mund të shtohen më shumë
        # TODO: Lexo simbolet nga një skedar konfigurimi
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "AMD", 
                   "NFLX", "DIS", "JPM", "V", "JNJ", "WMT", "PG"]
        
        # Provuam fillimisht me API tjetër por yfinance është më i mirë
        # symbols_OLD = ["AAPL", "MSFT", "GOOGL"]  # Versioni i vjetër - vetëm 3
        
        # Procesimi në batch për efikasitet më të mirë
        try:
            tickers = yf.Tickers(" ".join(symbols))
            
            for symbol in symbols:
                try:
                    ticker = tickers.tickers[symbol]
                    info = ticker.info
                    hist = ticker.history(period="1d")
                    
                    if not hist.empty and info:
                        current_price = float(hist['Close'].iloc[-1])
                        prev_close = float(info.get('previousClose', current_price))
                        
                        stock_data.append({
                            'symbol': symbol,
                            'price': current_price,
                            'previous_close': prev_close,
                            'change': current_price - prev_close,
                            'change_percent': ((current_price - prev_close) / prev_close * 100) if prev_close > 0 else 0,
                            'volume': int(info.get('volume', 0)),
                            'market_cap': float(info.get('marketCap', 0)),
                            'high': float(hist['High'].iloc[-1]) if 'High' in hist else 0,
                            'low': float(hist['Low'].iloc[-1]) if 'Low' in hist else 0,
                            'timestamp': datetime.now().isoformat(),
                            'source': 'yfinance_lib'
                        })
                        self.api_stats['yfinance_lib']['calls'] += 1
                        self.api_stats['yfinance_lib']['last_success'] = datetime.now()
                    
                    time.sleep(0.5)  # Small delay between stocks
                except Exception as e:
                    logging.error(f"yfinance error for {symbol}: {e}")
                    self.api_stats['yfinance_lib']['errors'] += 1
        except Exception as e:
            logging.error(f"yfinance batch error: {e}")
            self.api_stats['yfinance_lib']['errors'] += 1
        
        return stock_data
    
    def collect_stocks_polygon(self):
        """Collect stocks from Polygon.io - FREE: 5 calls/minute"""
        stock_data = []
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        
        for symbol in symbols:
            try:
                # Get previous close
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{yesterday}/{yesterday}"
                params = {'apiKey': self.api_keys['polygon']}
                
                data = self.fetch_with_retry(url, params, timeout=10)
                
                if data and 'results' in data and len(data['results']) > 0:
                    result = data['results'][0]
                    price = float(result.get('c', 0))  # Close price
                    prev_close = float(result.get('o', price))  # Open as previous close approximation
                    
                    stock_data.append({
                        'symbol': symbol,
                        'price': price,
                        'previous_close': prev_close,
                        'change': price - prev_close,
                        'change_percent': ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0,
                        'volume': int(result.get('v', 0)),
                        'high': float(result.get('h', 0)),
                        'low': float(result.get('l', 0)),
                        'timestamp': datetime.now().isoformat(),
                        'source': 'polygon'
                    })
                    self.api_stats['polygon']['calls'] += 1
                    self.api_stats['polygon']['last_success'] = datetime.now()
                
                time.sleep(12)  # 5 calls per minute = 12 seconds between calls
            except Exception as e:
                self.api_stats['polygon']['errors'] += 1
                logging.error(f"Polygon {symbol} error: {e}")
        
        return stock_data
    
    def collect_stocks_alpha_vantage(self):
        """Collect stocks from Alpha Vantage - FREE: 500 calls/day, 5 calls/minute"""
        stock_data = []
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "AMD", "NFLX", "DIS"]
        
        # Only try 5 stocks per batch to stay within rate limits
        symbols_subset = symbols[:5]
        
        for symbol in symbols_subset:
            try:
                url = "https://www.alphavantage.co/query"
                params = {
                    'function': 'GLOBAL_QUOTE',
                    'symbol': symbol,
                    'apikey': self.api_keys['alpha_vantage']
                }
                data = self.fetch_with_retry(url, params, timeout=10)
                
                if data and 'Global Quote' in data:
                    quote = data['Global Quote']
                    if quote and '05. price' in quote:
                        price = float(quote.get('05. price', 0))
                        prev_close = float(quote.get('08. previous close', price))
                        
                        stock_data.append({
                            'symbol': symbol,
                            'price': price,
                            'previous_close': prev_close,
                            'change': float(quote.get('09. change', 0)),
                            'change_percent': float(quote.get('10. change percent', '0').replace('%', '')),
                            'volume': int(quote.get('06. volume', 0)),
                            'high': float(quote.get('03. high', 0)),
                            'low': float(quote.get('04. low', 0)),
                            'timestamp': datetime.now().isoformat(),
                            'source': 'alpha_vantage'
                        })
                        self.api_stats['alpha_vantage']['calls'] += 1
                        self.api_stats['alpha_vantage']['last_success'] = datetime.now()
                
                # Rate limit: 5 calls per minute
                time.sleep(12)  # 60/5 = 12 seconds between calls
            except Exception as e:
                self.api_stats['alpha_vantage']['errors'] += 1
                logging.error(f"Alpha Vantage {symbol} error: {e}")
        
        return stock_data
    
    def collect_stocks_finnhub(self):
        """Collect stocks from Finnhub - FREE: 60 calls/minute"""
        stock_data = []
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "AMD", "NFLX", "DIS",
                   "JPM", "V", "JNJ", "WMT", "PG", "MA", "UNH", "HD", "BAC", "XOM"]
        
        # Try 20 stocks per batch (well within 60/min limit)
        for symbol in symbols[:20]:
            try:
                url = f"https://finnhub.io/api/v1/quote"
                params = {
                    'symbol': symbol,
                    'token': self.api_keys['finnhub']
                }
                data = self.fetch_with_retry(url, params, timeout=10)
                
                if data and 'c' in data:
                    current_price = float(data.get('c', 0))
                    prev_close = float(data.get('pc', current_price))
                    
                    if current_price > 0:
                        stock_data.append({
                            'symbol': symbol,
                            'price': current_price,
                            'previous_close': prev_close,
                            'change': current_price - prev_close,
                            'change_percent': ((current_price - prev_close) / prev_close * 100) if prev_close > 0 else 0,
                            'high': float(data.get('h', 0)),
                            'low': float(data.get('l', 0)),
                            'open': float(data.get('o', 0)),
                            'timestamp': datetime.now().isoformat(),
                            'source': 'finnhub'
                        })
                        self.api_stats['finnhub']['calls'] += 1
                        self.api_stats['finnhub']['last_success'] = datetime.now()
                
                # Rate limit: 60 calls per minute = 1 call per second
                time.sleep(1)
            except Exception as e:
                self.api_stats['finnhub']['errors'] += 1
                logging.error(f"Finnhub {symbol} error: {e}")
        
        return stock_data
    
    def collect_stocks_with_fallback(self):
        """Collect stocks with fallback mechanism - try multiple working APIs"""
        all_stocks = []
        
        # Try APIs in order of preference and reliability
        api_functions = [
            ('yfinance_lib', self.collect_stocks_yfinance_library),   # BEST - Most reliable
            ('finnhub', self.collect_stocks_finnhub),                 # 60 calls/min
            ('alpha_vantage', self.collect_stocks_alpha_vantage),     # 500 calls/day
            ('polygon', self.collect_stocks_polygon),                  # 5 calls/min
        ]
        
        # Rotate through APIs based on collection count
        api_index = self.collection_count % len(api_functions)
        api_name, api_function = api_functions[api_index]
        
        try:
            stocks = api_function()
            if stocks:
                all_stocks.extend(stocks)
                logging.info(f"Successfully collected {len(stocks)} stocks from {api_name}")
                print(f"    📈 Using {api_name}: {len(stocks)} stocks")
            else:
                # Try yfinance library as primary fallback (most reliable)
                if api_name != 'yfinance_lib':
                    print(f"    ⚠️  {api_name} failed, trying yfinance library...")
                    stocks = self.collect_stocks_yfinance_library()
                    if stocks:
                        all_stocks.extend(stocks)
                        logging.info(f"Fallback: collected {len(stocks)} stocks from yfinance_lib")
                        print(f"    ✓ yfinance_lib fallback: {len(stocks)} stocks")
        except Exception as e:
            logging.error(f"Stock collection failed: {e}")
            # Last resort: try yfinance library
            try:
                stocks = self.collect_stocks_yfinance_library()
                if stocks:
                    all_stocks.extend(stocks)
                    print(f"    ✓ Emergency fallback to yfinance_lib: {len(stocks)} stocks")
            except:
                pass
        
        return all_stocks
    
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
            
            logging.info(f"Saved {len(data)} {data_type} rows to {path}")
        except Exception as e:
            logging.error(f"Error saving {data_type}: {e}")
    
    def collect_batch(self):
        """Collect one batch - CRYPTO FOCUSED with multi-API stock fallback"""
        start_time = time.time()
        
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Batch #{self.collection_count + 1}...")
        
        # PRIMARY: Collect crypto (no rate limits, always works)
        crypto_cg = self.collect_crypto_coingecko(per_page=250)  # Top 250 coins
        crypto_bn = self.collect_crypto_binance(limit=100)        # Top 100 USDT pairs
        
        # SECONDARY: Collect stocks with fallback mechanism
        stocks = self.collect_stocks_with_fallback()
        
        # Save all data
        self.save_to_parquet(stocks, 'stocks')
        self.save_to_parquet(crypto_cg, 'crypto')
        self.save_to_parquet(crypto_bn, 'crypto')
        
        total_rows = len(stocks) + len(crypto_cg) + len(crypto_bn)
        elapsed = time.time() - start_time
        
        self.collection_count += 1
        
        print(f"  ✓ Collected {total_rows} rows in {elapsed:.1f}s")
        print(f"    - CoinGecko: {len(crypto_cg)} coins")
        print(f"    - Binance: {len(crypto_bn)} pairs")
        print(f"    - Stocks (Multi-API): {len(stocks)} stocks")
        
        # Print stats every 50 batches
        if self.collection_count % 50 == 0:
            self.print_stats()
        
        return total_rows
    
    def print_stats(self):
        """Print collection statistics"""
        print("\\n" + "="*60)
        print(f" COLLECTION STATS (Batch #{self.collection_count})")
        print("="*60)
        for api, stats in self.api_stats.items():
            success_time = stats.get('last_success')
            success_str = success_time.strftime('%H:%M:%S') if success_time else 'Never'
            print(f"  {api:12} - Calls: {stats['calls']:6d} | Errors: {stats['errors']:4d} | Last: {success_str}")
        print("="*60 + "\\n")
    
    def run_continuous(self, duration_days=8):
        """Run continuous collection"""
        print(f"\\n🚀 Starting {duration_days}-day crypto-focused collection")
        print(f"Target: ~{duration_days * 24 * 60 * 12:,} batches (5-sec intervals)")
        print(f"Expected crypto rows: ~{duration_days * 24 * 60 * 12 * 350:,}\\n")
        
        end_time = datetime.now() + timedelta(days=duration_days)
        total_rows = 0
        
        try:
            while datetime.now() < end_time:
                rows = self.collect_batch()
                total_rows += rows
                
                # Sleep to maintain 5-second intervals
                time.sleep(5)
                
                # Checkpoint every 5 minutes
                if self.collection_count % 60 == 0:
                    logging.info(f"Checkpoint: {self.collection_count} batches, {total_rows:,} rows")
                    print(f"\\n💾 Checkpoint: {total_rows:,} rows collected")
        
        except KeyboardInterrupt:
            print("\\n\\n⚠️  Collection interrupted by user")
        except Exception as e:
            logging.error(f"Fatal error: {e}")
            print(f"\\n❌ Error: {e}")
        finally:
            self.print_stats()
            print(f"\\n✅ Collection complete: {total_rows:,} total rows")
            self.spark.stop()

if __name__ == "__main__":
    print(f"\\n{'='*80}")
    print(f" STARTING ENHANCED CRYPTO-FOCUSED COLLECTION")
    print(f" Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\\n")
    
    try:
        collector = EnhancedCollector()
        collector.run_continuous(duration_days=8)
    except KeyboardInterrupt:
        print("\\n\\n⚠️  Collection stopped by user")
        logging.info("Collection stopped by user")
    except Exception as e:
        print(f"\\n❌ FATAL ERROR: {e}")
        logging.error(f"Fatal error in main: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        raise
