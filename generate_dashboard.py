"""
Generate Dashboard JSON from collected Parquet data
Reads all Parquet files and creates a comprehensive dashboard
"""

from pyspark.sql import SparkSession
from datetime import datetime
import json
import os
import glob

def generate_dashboard():
    print("="*80)
    print(" 📊 DASHBOARD DATA GENERATOR")
    print(" Generating dashboard from collected Parquet files")
    print("="*80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Dashboard_Generator") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    dashboard = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'reddit_posts': 0,
            'stocks': 0,
            'cryptos': 0,
            'forex_pairs': 0,
            'total_assets': 0
        },
        'reddit': {'total': 0, 'top_posts': [], 'all_posts': []},
        'stocks': {'total': 0, 'all_stocks': [], 'top_gainers': [], 'top_losers': []},
        'crypto': {'total': 0, 'all_crypto': [], 'top_gainers': [], 'top_losers': []},
        'forex': {'total': 0, 'all_pairs': []}
    }
    
    # Read Stocks
    stock_paths = glob.glob("data/stocks/date=*/hour=*/*.parquet")
    if stock_paths:
        print(f"\\n📈 Reading stock data from {len(stock_paths)} files...")
        try:
            df = spark.read.parquet("data/stocks/")
            # Get latest data for each symbol
            df_latest = df.orderBy("timestamp", ascending=False) \
                         .dropDuplicates(["symbol"]) \
                         .limit(100)
            
            stock_data = []
            for row in df_latest.collect():
                stock_data.append({
                    'symbol': row.symbol,
                    'price': float(row.price),
                    'previous_close': float(row.previous_close),
                    'change': float(row.change),
                    'change_percent': float(row.change_percent),
                    'volume': int(row.volume),
                    'market_cap': float(row.market_cap),
                    'timestamp': row.timestamp,
                    'source': row.source
                })
            
            dashboard['stocks']['all_stocks'] = stock_data
            dashboard['stocks']['total'] = len(stock_data)
            dashboard['stocks']['top_gainers'] = sorted(stock_data, key=lambda x: x['change'], reverse=True)[:10]
            dashboard['stocks']['top_losers'] = sorted(stock_data, key=lambda x: x['change'])[:10]
            dashboard['summary']['stocks'] = len(stock_data)
            print(f"✓ Loaded {len(stock_data)} stocks")
        except Exception as e:
            print(f"✗ Error reading stocks: {e}")
    else:
        print("⚠ No stock data found")
    
    # Read Crypto
    crypto_paths = glob.glob("data/crypto/date=*/hour=*/*.parquet")
    if crypto_paths:
        print(f"\\n💰 Reading crypto data from {len(crypto_paths)} files...")
        try:
            df = spark.read.parquet("data/crypto/")
            # Get latest data for each crypto
            df_latest = df.orderBy("timestamp", ascending=False)
            
            # For coins with 'id' field (CoinGecko)
            if 'id' in df.columns:
                df_latest = df_latest.dropDuplicates(["id"]).limit(250)
            else:
                df_latest = df_latest.dropDuplicates(["symbol"]).limit(250)
            
            crypto_data = []
            for row in df_latest.collect():
                crypto_dict = {
                    'symbol': row.symbol,
                    'price': float(row.price),
                    'timestamp': row.timestamp,
                    'source': row.source
                }
                
                # Add optional fields if they exist (with None checks)
                if hasattr(row, 'id') and row.id is not None:
                    crypto_dict['id'] = row.id
                if hasattr(row, 'name') and row.name is not None:
                    crypto_dict['name'] = row.name
                if hasattr(row, 'market_cap') and row.market_cap is not None:
                    crypto_dict['market_cap'] = float(row.market_cap)
                if hasattr(row, 'volume') and row.volume is not None:
                    crypto_dict['volume'] = float(row.volume)
                if hasattr(row, 'change_24h') and row.change_24h is not None:
                    crypto_dict['change_24h'] = float(row.change_24h)
                if hasattr(row, 'rank') and row.rank is not None:
                    crypto_dict['rank'] = int(row.rank)
                if hasattr(row, 'high_24h') and row.high_24h is not None:
                    crypto_dict['high_24h'] = float(row.high_24h)
                if hasattr(row, 'low_24h') and row.low_24h is not None:
                    crypto_dict['low_24h'] = float(row.low_24h)
                
                crypto_data.append(crypto_dict)
            
            dashboard['crypto']['all_crypto'] = crypto_data
            dashboard['crypto']['total'] = len(crypto_data)
            
            # Top gainers/losers (only if change_24h exists)
            crypto_with_change = [c for c in crypto_data if 'change_24h' in c]
            if crypto_with_change:
                dashboard['crypto']['top_gainers'] = sorted(crypto_with_change, key=lambda x: x['change_24h'], reverse=True)[:10]
                dashboard['crypto']['top_losers'] = sorted(crypto_with_change, key=lambda x: x['change_24h'])[:10]
            
            dashboard['summary']['cryptos'] = len(crypto_data)
            print(f"✓ Loaded {len(crypto_data)} cryptos")
        except Exception as e:
            print(f"✗ Error reading crypto: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("⚠ No crypto data found")
    
    # Read Forex
    forex_paths = glob.glob("data/forex/date=*/hour=*/*.parquet")
    if forex_paths:
        print(f"\\n💱 Reading forex data from {len(forex_paths)} files...")
        try:
            df = spark.read.parquet("data/forex/")
            df_latest = df.orderBy("timestamp", ascending=False) \
                         .dropDuplicates(["pair"]) \
                         .limit(20)
            
            forex_data = []
            for row in df_latest.collect():
                forex_data.append({
                    'pair': row.pair,
                    'rate': float(row.rate),
                    'change': float(row.change),
                    'timestamp': row.timestamp,
                    'source': row.source
                })
            
            dashboard['forex']['all_pairs'] = forex_data
            dashboard['forex']['total'] = len(forex_data)
            dashboard['summary']['forex_pairs'] = len(forex_data)
            print(f"✓ Loaded {len(forex_data)} forex pairs")
        except Exception as e:
            print(f"✗ Error reading forex: {e}")
    else:
        print("⚠ No forex data found")
    
    # Calculate total
    dashboard['summary']['total_assets'] = (
        dashboard['summary']['stocks'] + 
        dashboard['summary']['cryptos'] + 
        dashboard['summary']['forex_pairs']
    )
    
    # Save dashboard
    output_file = f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(dashboard, f, indent=2, ensure_ascii=False)
    
    print("\\n" + "="*80)
    print(" ✅ DASHBOARD GENERATED")
    print("="*80)
    print(f"\\n📊 Summary:")
    print(f"   - Stocks: {dashboard['summary']['stocks']}")
    print(f"   - Cryptos: {dashboard['summary']['cryptos']}")
    print(f"   - Forex Pairs: {dashboard['summary']['forex_pairs']}")
    print(f"   - Total Assets: {dashboard['summary']['total_assets']}")
    print(f"\\n💾 Saved to: {output_file}")
    print(f"\\n🚀 View dashboard: python3 dashboard.py")
    
    spark.stop()
    return dashboard

if __name__ == "__main__":
    generate_dashboard()
