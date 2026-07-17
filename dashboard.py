"""
=============================================================================
FLASK DASHBOARD - Web UI për Big Data Analytics
=============================================================================
Autorët: Shaban Ejupi & Majlinda Bajraktari
Universiteti i Prishtinës - FSHMN
Departamenti i Matematikës
Data e zhvillimit: Nëntor-Dhjetor 2025

Përshkrimi:
-----------
Ky sistem ofron analizë në kohë reale të të dhënave financiare duke përdorur
Apache Spark në një klaster me 10 VM. Sistemi mbledh dhe analizon të dhëna
për kriptovaluta, aksione, dhe postime Reddit.

Teknologjitë e përdorura:
- Apache Spark për procesimin e shpërndarë
- Flask për web server
- Pandas & PyArrow për analizën e të dhënave
- Chart.js për vizualizime interaktive

REAL-TIME Data from Parquet Files - 10 VM Cluster
Enhanced with live updates, multiple stock APIs, and modern UI
ML Predictions, Investment Recommendations, and Portfolio Optimization
=============================================================================
"""

# Importimi i moduleve të nevojshme
from flask import Flask, render_template, jsonify, send_file, Response, request, stream_with_context
import json
import os
import glob
import csv
import io
import zlib
import gzip
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import atexit

# Singleton Spark session reference
_spark_session = None

def get_spark_session():
    """Return a singleton SparkSession. Register atexit stop handler.

    This avoids repeatedly creating/stopping SparkSession objects which
    can lead to threadpool and TaskScheduler errors when served from Flask.
    """
    global _spark_session
    if _spark_session is not None:
        return _spark_session

    try:
        from pyspark.sql import SparkSession
        # Tuned for moderate-memory local use; adjust for your cluster
        builder = SparkSession.builder.appName("BigDataDashboard")
        try:
            builder = builder.config("spark.driver.memory", "4g")
            builder = builder.config("spark.sql.shuffle.partitions", "8")
            builder = builder.config("spark.driver.maxResultSize", "1g")
            # Prevent executor shutdown errors
            builder = builder.config("spark.executor.heartbeatInterval", "30s")
            builder = builder.config("spark.network.timeout", "300s")
            builder = builder.config("spark.executor.allowSparkContext", "true")
        except Exception:
            pass

        _spark_session = builder.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")  # Reduce noise

        def _stop_spark():
            try:
                global _spark_session
                if _spark_session is not None:
                    logging.info("Stopping Spark session gracefully...")
                    # Give time for running jobs to complete
                    import time
                    time.sleep(2)
                    _spark_session.stop()
                    _spark_session = None
            except Exception as e:
                logging.warning(f"Error stopping Spark: {e}")

        atexit.register(_stop_spark)
        return _spark_session
    except Exception as e:
        print(f"SparkSession not available: {e}")
        return None

# Lazy-load ML and Investment modules to avoid import-time failures
def load_ml_modules():
    """Attempt to import ML modules on demand. Returns tuple:
       (available, MLPredictionEngine, InvestmentRecommender, PortfolioOptimizer)
    """
    # ML price predictions were removed (forecasting the market — dropped by request).
    # MLPredictionEngine no longer exists; only the recommender and optimizer remain, and
    # the import stays independent so deleting ml_predictions.py does not disable them too.
    try:
        from investment_recommender import InvestmentRecommender
        from portfolio_optimizer import PortfolioOptimizer
        return True, None, InvestmentRecommender, PortfolioOptimizer
    except Exception as e:
        logging.warning(f"ML modules not available: {e}")
        return False, None, None, None

# Inicializimi i Flask aplikacionit
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# Global cache for ML predictions and recommendations
prediction_cache = {'data': None, 'timestamp': None, 'lock': threading.Lock()}
recommendation_cache = {'data': None, 'timestamp': None, 'lock': threading.Lock()}

# Global cache for total record counts
total_counts_cache = {'crypto': 0, 'stocks': 0, 'timestamp': None}

def get_total_record_counts():
    """Get total record count for all data using fast Spark scan"""
    global total_counts_cache
    
    # Cache for 5 minutes
    if total_counts_cache['timestamp']:
        age = (datetime.now() - total_counts_cache['timestamp']).total_seconds()
        if age < 300:  # 5 minutes
            return total_counts_cache['crypto'], total_counts_cache['stocks']
    
    try:
        # Use a singleton SparkSession helper (safer than creating/stopping
        # a SparkSession on every request which can cause threadpool crashes).
        spark = get_spark_session()

        crypto_path = '/home/krenuser/big-data-dashboard/data/crypto'
        stocks_path = '/home/krenuser/big-data-dashboard/data/stocks'

        crypto_count = 0
        stocks_count = 0

        if os.path.exists(crypto_path):
            try:
                parquet_files = glob.glob(f"{crypto_path}/**/*.parquet", recursive=True)
                if parquet_files and spark is not None:
                    crypto_count = spark.read.parquet(*parquet_files).count()
                elif parquet_files:
                    # no spark available, count via filesystem
                    crypto_count = len(parquet_files)
                else:
                    crypto_count = 0
            except Exception:
                crypto_count = 0

        if os.path.exists(stocks_path):
            try:
                parquet_files = glob.glob(f"{stocks_path}/**/*.parquet", recursive=True)
                if parquet_files and spark is not None:
                    stocks_count = spark.read.parquet(*parquet_files).count()
                elif parquet_files:
                    stocks_count = len(parquet_files)
                else:
                    stocks_count = 0
            except Exception:
                stocks_count = 0

        # Update cache
        total_counts_cache['crypto'] = crypto_count
        total_counts_cache['stocks'] = stocks_count
        total_counts_cache['timestamp'] = datetime.now()

        return crypto_count, stocks_count
    except Exception as e:
        print(f"Error counting total records: {e}")
        return 0, 0

def read_parquet_data(data_type, hours_back=2, all_files=False, max_workers=6, max_files=50):
    """
    Leximi i të dhënave nga skedarët Parquet - REAL-TIME nga VM-t
    OPTIMIZED: Only reads last N hours for faster loading
    
    Parametrat:
        data_type: Lloji i të dhënave (crypto, stocks, forex)
        hours_back: Numri i orëve prapa (default 2)
        max_files: Maximum number of files to read (default 50 to prevent broken pipe)
    
    Kthehet: Listë me të dhënat e lexuara
    """
    data_dir = f"/home/krenuser/big-data-dashboard/data/{data_type}/"
    
    if not os.path.exists(data_dir):
        print(f"⚠️  Directory not found: {data_dir}")
        return []
    
    all_data = []
    
    try:
        # Get current date and recent hours for faster loading
        now = datetime.now()
        
        # Only read specific hours to speed up loading unless all_files is True
        parquet_files_all = []
        if not all_files and hours_back and hours_back > 0:
            target_hours = []
            for h in range(hours_back):
                target_time = now - timedelta(hours=h)
                target_hours.append({
                    'date': target_time.strftime("%Y-%m-%d"),
                    'hour': target_time.strftime("%H")
                })

            for target in target_hours:
                hour_dir = f"{data_dir}date={target['date']}/hour={target['hour']}/"
                if not os.path.exists(hour_dir):
                    continue
                found = sorted(glob.glob(f"{hour_dir}*.parquet"), key=os.path.getmtime, reverse=True)
                if found:
                    parquet_files_all.extend(found)

        # If collecting all files or no recent files found, fall back to scanning the tree
        if all_files or not parquet_files_all:
            fallback = sorted(glob.glob(f"{data_dir}/**/*.parquet", recursive=True), key=os.path.getmtime, reverse=True)
            if fallback:
                # If max_files provided, limit; otherwise potentially use a reasonable cap if not all_files
                if max_files:
                    parquet_files_all = fallback[:max_files]
                elif all_files:
                    parquet_files_all = fallback
                else:
                    parquet_files_all = fallback[:200]
                    print(f"⚠️  No recent hour files found for {data_type}; falling back to {len(parquet_files_all)} recent parquet files")
            else:
                print(f"✓ Loaded 0 {data_type} records from 0 parquet files")
                return []

        # Try to read with Spark (faster and more robust for many small files)
        spark = get_spark_session()
        if spark is not None and not all_files:
            try:
                sdf = spark.read.parquet(*parquet_files_all)
                # Convert to pandas for existing processing
                if sdf.rdd.isEmpty():
                    print(f"✓ Loaded 0 {data_type} records from 0 parquet files (spark)")
                else:
                    df = sdf.toPandas()
                    # Ensure numeric types for key fields
                    if 'market_cap' in df.columns:
                        df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce').fillna(0)
                    if 'volume' in df.columns:
                        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
                    if 'price' in df.columns:
                        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
                    if 'change_24h' in df.columns:
                        df['change_24h'] = pd.to_numeric(df['change_24h'], errors='coerce').fillna(0)
                    all_data.extend(df.to_dict('records'))
                    files_read = len(parquet_files_all)
            except Exception as e:
                print(f"Spark read failed, falling back to pandas per-file: {e}")
                # fall through to pandas per-file reader

        # If Spark not available or we requested all_files, fallback to pandas per-file
        files_skipped = 0  # Initialize before conditional
        if not all_data:
            files_read = 0

            # Always limit total files for safety (prevent broken pipe)
            if isinstance(parquet_files_all, list):
                if max_files:
                    parquet_files = parquet_files_all[:max_files]
                else:
                    parquet_files = parquet_files_all[:50]  # Default limit
            else:
                parquet_files = parquet_files_all

            def _read_file(path):
                try:
                    if not path.lower().endswith('.parquet'):
                        return None, 'skipped'
                    df = pd.read_parquet(path)
                    if df.empty:
                        return [], None
                    # Normalize numeric fields
                    if 'market_cap' in df.columns:
                        df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce').fillna(0)
                    if 'volume' in df.columns:
                        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
                    if 'price' in df.columns:
                        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
                    if 'change_24h' in df.columns:
                        df['change_24h'] = pd.to_numeric(df['change_24h'], errors='coerce').fillna(0)
                    return df.to_dict('records'), None
                except Exception as e:
                    return None, str(e)

            # Read files in parallel to speed up IO-bound reads
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {ex.submit(_read_file, p): p for p in parquet_files}
                for fut in as_completed(futures):
                    path = futures[fut]
                    records, err = fut.result()
                    if err == 'skipped':
                        files_skipped += 1
                        continue
                    if err:
                        files_skipped += 1
                        print(f"Error reading {path}: {err}")
                        continue
                    if records:
                        all_data.extend(records)
                        files_read += 1

        if files_skipped:
            print(f"⚠️  Skipped {files_skipped} non-parquet or unreadable files while loading {data_type}")

        print(f"✓ Loaded {len(all_data)} {data_type} records from {files_read} parquet files")
        
    except Exception as e:
        print(f"Error reading parquet data for {data_type}: {e}")
    
    return all_data


def _list_parquet_files_for_export(data_dir, hours_back=None, max_files=None):
    """Return a list of parquet files to export.

    If `hours_back` is provided, prefer files from the last N hours (matching
    the project's `date=YYYY-MM-DD/hour=HH` layout). Otherwise fallback to
    scanning the tree. Results are sorted by modification time (newest first).
    """
    files = []
    try:
        if hours_back and hours_back > 0:
            now = datetime.now()
            candidate_files = []
            for h in range(hours_back):
                target_time = now - timedelta(hours=h)
                hour_dir = f"{data_dir}/date={target_time.strftime('%Y-%m-%d')}/hour={target_time.strftime('%H')}/"
                if os.path.exists(hour_dir):
                    found = sorted(glob.glob(f"{hour_dir}*.parquet"), key=os.path.getmtime, reverse=True)
                    candidate_files.extend(found)

            if candidate_files:
                files = candidate_files
        # Fallback to scanning entire tree
        if not files:
            files = sorted(glob.glob(f"{data_dir}/**/*.parquet", recursive=True), key=os.path.getmtime, reverse=True)

        if max_files:
            files = files[:max_files]
    except Exception:
        files = []

    return files


def _gzip_bytes_generator(text_iterable, encoding='utf-8'):
    """Compress text chunks yielded from `text_iterable` into gzip bytes.

    Accepts an iterable that yields str (or bytes). Returns an iterator of
    compressed bytes which can be returned directly from Flask `Response`.
    Uses zlib.compressobj with wbits=31 to produce a gzip stream.
    """
    comp = zlib.compressobj(wbits=31)
    for chunk in text_iterable:
        if chunk is None:
            continue
        if isinstance(chunk, str):
            chunk_bytes = chunk.encode(encoding)
        else:
            chunk_bytes = chunk
        out = comp.compress(chunk_bytes)
        if out:
            yield out
    tail = comp.flush()
    if tail:
        yield tail

def aggregate_crypto_data(crypto_records):
    """Aggregate crypto data - get latest price for each symbol"""
    if not crypto_records:
        return []
    
    # Convert to DataFrame for easier aggregation
    df = pd.DataFrame(crypto_records)
    
    # Ensure numeric types for critical fields
    if 'market_cap' in df.columns:
        df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce').fillna(0)
    if 'volume' in df.columns:
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    if 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    if 'change_24h' in df.columns:
        df['change_24h'] = pd.to_numeric(df['change_24h'], errors='coerce').fillna(0)
    if 'rank' in df.columns:
        df['rank'] = pd.to_numeric(df['rank'], errors='coerce').fillna(999)
    
    # Sort by timestamp and get most recent for each symbol
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp', ascending=False)
    
    # Group by symbol/id and get latest
    if 'symbol' in df.columns:
        latest_crypto = df.drop_duplicates(subset=['symbol'], keep='first')
    elif 'id' in df.columns:
        latest_crypto = df.drop_duplicates(subset=['id'], keep='first')
    else:
        latest_crypto = df
    
    # Sort by market cap for proper ranking
    if 'market_cap' in latest_crypto.columns:
        latest_crypto = latest_crypto.sort_values('market_cap', ascending=False)
    
    return latest_crypto.to_dict('records')

def aggregate_stock_data(stock_records):
    """Aggregate stock data - get latest price for each symbol"""
    if not stock_records:
        return []
    
    df = pd.DataFrame(stock_records)
    
    # Sort by timestamp and get most recent for each symbol
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp', ascending=False)
    
    if 'symbol' in df.columns:
        latest_stocks = df.drop_duplicates(subset=['symbol'], keep='first')
        return latest_stocks.to_dict('records')
    
    return stock_records

# Global cache for dashboard data
dashboard_cache = {'data': None, 'timestamp': None, 'lock': threading.Lock()}

def load_dashboard_data(use_cache=True):
    """Load REAL-TIME data from Parquet files written by VMs (with caching)"""
    
    # Check cache (refresh every 60 seconds for fast response)
    if use_cache:
        with dashboard_cache['lock']:
            if dashboard_cache['data'] and dashboard_cache['timestamp']:
                age = (datetime.now() - dashboard_cache['timestamp']).total_seconds()
                if age < 60:  # 60 seconds cache
                    print(f"✓ Using cached data (age: {int(age)}s)")
                    return dashboard_cache['data']
    
    print("\n" + "="*60)
    print(" 📊 LOADING REAL-TIME DATA FROM PARQUET FILES")
    print("="*60)
    
    # Get total record counts from ALL data
    crypto_total_count, stocks_total_count = get_total_record_counts()
    
    # Read raw data from parquet (recent data for display)
    crypto_records = read_parquet_data('crypto', hours_back=24)
    stock_records = read_parquet_data('stocks', hours_back=24)
    
    # Aggregate to get latest data
    crypto_data = aggregate_crypto_data(crypto_records)
    stock_data = aggregate_stock_data(stock_records)
    
    print(f"\n📈 Data Summary:")
    print(f"  - Crypto records (recent 24h): {len(crypto_records)}")
    print(f"  - Crypto records (TOTAL ALL TIME): {crypto_total_count:,}")
    print(f"  - Crypto (unique latest): {len(crypto_data)}")
    print(f"  - Stock records (recent 24h): {len(stock_records)}")
    print(f"  - Stock records (TOTAL ALL TIME): {stocks_total_count:,}")
    print(f"  - Stocks (unique latest): {len(stock_data)}")
    print("="*60 + "\n")
    
    # Prepare stock analysis
    stocks_with_change = [s for s in stock_data if 'change_percent' in s]
    top_gainers = sorted(stocks_with_change, key=lambda x: float(x.get('change_percent', 0)), reverse=True)[:10]
    top_losers = sorted(stocks_with_change, key=lambda x: float(x.get('change_percent', 0)))[:10]
    
    # Prepare crypto analysis
    crypto_with_change = [c for c in crypto_data if 'change_24h' in c]
    crypto_gainers = sorted(crypto_with_change, key=lambda x: float(x.get('change_24h', 0)), reverse=True)[:10]
    crypto_losers = sorted(crypto_with_change, key=lambda x: float(x.get('change_24h', 0)))[:10]
    
    result = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'reddit_posts': 0,
            'stocks': len(stock_data),
            'cryptos': len(crypto_data),
            'total_records': crypto_total_count + stocks_total_count,
            'total_assets': len(stock_data) + len(crypto_data),
            'crypto_total_records': crypto_total_count,
            'crypto_unique_assets': len(crypto_data),
            'stock_total_records': stocks_total_count,
            'stock_unique_assets': len(stock_data)
        },
        'reddit': {
            'total': 0,
            'top_posts': [],
            'all_posts': []
        },
        'stocks': {
            'total': len(stock_data),
            'total_records': stocks_total_count,
            'unique_symbols': len(stock_data),
            'all_stocks': stock_data,
            'top_gainers': top_gainers,
            'top_losers': top_losers
        },
        'crypto': {
            'total': len(crypto_data),
            'total_records': crypto_total_count,
            'unique_coins': len(crypto_data),
            'all_crypto': crypto_data,
            'top_gainers': crypto_gainers,
            'top_losers': crypto_losers
        }
    }
    
    # Cache the result
    with dashboard_cache['lock']:
        dashboard_cache['data'] = result
        dashboard_cache['timestamp'] = datetime.now()
    
    return result

@app.route('/')
def index():
    """Faqja kryesore e dashboard"""
    try:
        data = load_dashboard_data()
        return render_template('index.html', data=data)
    except Exception as e:
        print(f"Error in index route: {e}")
        return f"Error loading dashboard: {e}", 500

@app.route('/reddit')
def reddit():
    """Faqja e analizës së Reddit"""
    try:
        data = load_dashboard_data()
        return render_template('reddit.html', data=data)
    except Exception as e:
        print(f"Error in reddit route: {e}")
        return f"Error loading reddit data: {e}", 500

@app.route('/stocks')
def stocks():
    """Faqja e analizës së aksioneve - REAL-TIME from Parquet"""
    try:
        data = load_dashboard_data()
        return render_template('stocks.html', data=data)
    except Exception as e:
        print(f"Error in stocks route: {e}")
        return f"Error loading stocks: {e}", 500

@app.route('/crypto')
def crypto():
    """Faqja e analizës së kriptovalutave - REAL-TIME from Parquet"""
    try:
        data = load_dashboard_data()
        return render_template('crypto.html', data=data)
    except Exception as e:
        print(f"Error in crypto route: {e}")
        return f"Error loading crypto: {e}", 500

@app.route('/api/data')
def api_data():
    """API endpoint për të dhënat e dashboard - REAL-TIME"""
    try:
        data = load_dashboard_data()
        return jsonify(data)
    except Exception as e:
        print(f"Error in API data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/refresh')
def api_refresh():
    """Refresh të dhënat - Always fresh from Parquet files"""
    try:
        data = load_dashboard_data()
        return jsonify({
            'success': True, 
            'timestamp': data.get('timestamp', ''),
            'stocks': data.get('summary', {}).get('stocks', 0),
            'cryptos': data.get('summary', {}).get('cryptos', 0),
            'total_records': data.get('summary', {}).get('total_records', 0)
        })
    except Exception as e:
        print(f"Error in API refresh: {e}")
        return jsonify({'error': str(e), 'success': False}), 500

@app.route('/api/cluster_status')
def cluster_status():
    """Get Spark cluster status"""
    try:
        import subprocess
        # Check if Spark processes are running
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        spark_workers = result.stdout.count('Worker')
        spark_master = result.stdout.count('Master')
        collector_running = 'data_collector_v2' in result.stdout
        
        # Count parquet files to show data collection activity
        crypto_files = len(glob.glob('/home/krenuser/big-data-dashboard/data/crypto/**/*.parquet', recursive=True))
        stock_files = len(glob.glob('/home/krenuser/big-data-dashboard/data/stocks/**/*.parquet', recursive=True))
        
        return jsonify({
            'success': True,
            'cluster_active': spark_master > 0,
            'workers': spark_workers,
            'collector_running': collector_running,
            'data_files': {
                'crypto': crypto_files,
                'stocks': stock_files
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/collection_stats')
def collection_stats():
    """Get data collection statistics"""
    try:
        # Get file counts by hour
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        crypto_dir = f"/home/krenuser/big-data-dashboard/data/crypto/date={date_str}/"
        
        hourly_stats = {}
        if os.path.exists(crypto_dir):
            for hour_dir in glob.glob(f"{crypto_dir}hour=*/"):
                hour = hour_dir.split('hour=')[1].rstrip('/')
                files = glob.glob(f"{hour_dir}*.parquet")
                hourly_stats[hour] = len(files)
        
        return jsonify({
            'success': True,
            'hourly_stats': hourly_stats,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/export/json')
def export_json():
    """Export data as JSON"""
    data = load_dashboard_data()
    if not data:
        return jsonify({'error': 'Nuk u gjetën të dhëna'}), 404
    
    # Create JSON file in memory
    json_str = json.dumps(data, indent=2, ensure_ascii=False)
    
    return Response(
        json_str,
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment;filename=dashboard_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'}
    )

@app.route('/api/export/crypto/csv')
def export_crypto_csv():
    """Export crypto data as CSV"""
    data = load_dashboard_data()
    if not data or 'crypto' not in data:
        return jsonify({'error': 'Nuk u gjetën të dhëna crypto'}), 404
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(['Rank', 'Name', 'Symbol', 'Price', 'Change 24h %', 'Market Cap', 'Volume 24h', 'Source'])
    
    # Write data
    for crypto in data['crypto'].get('all_crypto', []):
        writer.writerow([
            crypto.get('rank', 'N/A'),
            crypto.get('name', crypto.get('symbol', 'Unknown')),
            crypto.get('symbol', 'N/A'),
            crypto.get('price', 0),
            crypto.get('change_24h', 0),
            crypto.get('market_cap', 0),
            crypto.get('volume', 0),
            crypto.get('source', 'unknown')
        ])
    
    output.seek(0)
    # Optionally gzip the CSV for faster network transfer
    gzip_param = request.args.get('gzip', '0')
    filename = f'crypto_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    if gzip_param in ('1', 'true', 'yes'):
        compressed = gzip.compress(output.getvalue().encode('utf-8'))
        headers = {
            'Content-Disposition': f'attachment;filename={filename}.gz',
            'Content-Encoding': 'gzip'
        }
        return Response(compressed, mimetype='text/csv', headers=headers)

    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment;filename={filename}'}
    )


@app.route('/api/export/crypto/all/csv')
def export_crypto_all_csv():
    """Export ALL crypto parquet records as CSV (streaming)"""
    data_dir = '/home/krenuser/big-data-dashboard/data/crypto'
    if not os.path.exists(data_dir):
        return jsonify({'error': 'Data directory not found'}), 404
    # Query params to control selection and compression
    hours_back = request.args.get('hours_back')
    max_files = request.args.get('max_files')
    gzip_param = request.args.get('gzip', '1')  # default to gzip for large exports

    try:
        hours_back = int(hours_back) if hours_back is not None else None
    except Exception:
        hours_back = None
    try:
        max_files = int(max_files) if max_files is not None else None
    except Exception:
        max_files = None

    def generate_text_rows():
        # header
        yield 'Rank,Name,Symbol,Price,Change 24h %,Market Cap,Volume 24h,Source\n'

        files = _list_parquet_files_for_export(data_dir, hours_back=hours_back, max_files=max_files)
        for path in files:
            try:
                df = pd.read_parquet(path)
                if df.empty:
                    continue
                # normalize and yield rows
                for _, row in df.iterrows():
                    rank = row.get('rank', '')
                    name = str(row.get('name', '')).replace(',', ' ')
                    symbol = row.get('symbol', '')
                    price = row.get('price', 0)
                    change = row.get('change_24h', 0)
                    market = row.get('market_cap', 0)
                    vol = row.get('volume', 0)
                    source = row.get('source', 'parquet')
                    yield f'{rank},{name},{symbol},{price},{change},{market},{vol},{source}\n'
            except Exception as e:
                print(f"Error reading {path}: {e}")
                continue

    filename = f'crypto_export_all_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    if gzip_param in ('1', 'true', 'yes'):
        # Stream gzipped CSV bytes
        headers = {
            'Content-Disposition': f'attachment;filename={filename}.gz',
            'Content-Encoding': 'gzip'
        }
        return Response(stream_with_context(_gzip_bytes_generator(generate_text_rows())), mimetype='text/csv', headers=headers)

    headers = {'Content-Disposition': f'attachment;filename={filename}'}
    return Response(stream_with_context(generate_text_rows()), mimetype='text/csv', headers=headers)

@app.route('/api/export/stocks/csv')
def export_stocks_csv():
    """Export stocks data as CSV"""
    data = load_dashboard_data()
    if not data or 'stocks' not in data:
        return jsonify({'error': 'Nuk u gjetën të dhëna stocks'}), 404
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(['Symbol', 'Price', 'Change', 'Change %', 'Volume', 'Market Cap', 'Source'])
    
    # Write data
    for stock in data['stocks'].get('all_stocks', []):
        writer.writerow([
            stock.get('symbol', 'N/A'),
            stock.get('price', 0),
            stock.get('change', 0),
            stock.get('change_percent', 0),
            stock.get('volume', 0),
            stock.get('market_cap', 0),
            stock.get('source', 'unknown')
        ])
    
    output.seek(0)
    gzip_param = request.args.get('gzip', '0')
    filename = f'stocks_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    if gzip_param in ('1', 'true', 'yes'):
        compressed = gzip.compress(output.getvalue().encode('utf-8'))
        headers = {
            'Content-Disposition': f'attachment;filename={filename}.gz',
            'Content-Encoding': 'gzip'
        }
        return Response(compressed, mimetype='text/csv', headers=headers)

    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment;filename={filename}'}
    )


@app.route('/api/export/stocks/all/csv')
def export_stocks_all_csv():
    """Export ALL stock parquet records as CSV (streaming)"""
    data_dir = '/home/krenuser/big-data-dashboard/data/stocks'
    if not os.path.exists(data_dir):
        return jsonify({'error': 'Data directory not found'}), 404

    # Query params to control selection and compression
    hours_back = request.args.get('hours_back')
    max_files = request.args.get('max_files')
    gzip_param = request.args.get('gzip', '1')

    try:
        hours_back = int(hours_back) if hours_back is not None else None
    except Exception:
        hours_back = None
    try:
        max_files = int(max_files) if max_files is not None else None
    except Exception:
        max_files = None

    def generate_text_rows():
        # header
        yield 'Symbol,Price,Change,Change %,Volume,Market Cap,Source\n'
        files = _list_parquet_files_for_export(data_dir, hours_back=hours_back, max_files=max_files)
        for path in files:
            try:
                df = pd.read_parquet(path)
                if df.empty:
                    continue
                for _, row in df.iterrows():
                    symbol = row.get('symbol', '')
                    price = row.get('price', 0)
                    change = row.get('change', 0)
                    change_pct = row.get('change_percent', 0)
                    vol = row.get('volume', 0)
                    market = row.get('market_cap', 0)
                    source = row.get('source', 'parquet')
                    yield f'{symbol},{price},{change},{change_pct},{vol},{market},{source}\n'
            except Exception as e:
                print(f"Error reading {path}: {e}")
                continue

    filename = f'stocks_export_all_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    if gzip_param in ('1', 'true', 'yes'):
        headers = {
            'Content-Disposition': f'attachment;filename={filename}.gz',
            'Content-Encoding': 'gzip'
        }
        return Response(stream_with_context(_gzip_bytes_generator(generate_text_rows())), mimetype='text/csv', headers=headers)

    headers = {'Content-Disposition': f'attachment;filename={filename}'}
    return Response(stream_with_context(generate_text_rows()), mimetype='text/csv', headers=headers)

# The /predictions page and /api/ml/predictions endpoint were removed: forecasting future
# prices was dropped by request. The recommender and portfolio pages (analysis of historical
# data) remain below.

@app.route('/recommendations')
def recommendations_page():
    """Investment Recommendations page"""
    try:
        data = load_dashboard_data()
        return render_template('recommendations.html', data=data)
    except Exception as e:
        print(f"Error in recommendations route: {e}")
        return f"Error loading recommendations: {e}", 500

@app.route('/portfolio')
def portfolio_page():
    """Portfolio Optimizer page"""
    try:
        data = load_dashboard_data()
        return render_template('portfolio.html', data=data)
    except Exception as e:
        print(f"Error in portfolio route: {e}")
        return f"Error loading portfolio: {e}", 500

@app.route('/api/investment/recommendations')
def get_recommendations():
    """Get investment recommendations"""
    try:
        risk_profile = request.args.get('risk_profile', 'balanced')
        
        # Check cache (refresh every 5 minutes for fresh recommendations)
        cache_key = f"{risk_profile}"
        with recommendation_cache['lock']:
            if recommendation_cache['data'] and recommendation_cache['timestamp']:
                age = (datetime.now() - recommendation_cache['timestamp']).total_seconds()
                if age < 300:  # 5 minutes (was 30 minutes)
                    return jsonify({
                        'success': True,
                        'recommendations': recommendation_cache['data'],
                        'cached': True,
                        'cache_age': int(age)
                    })
        
        # Lazy import recommender
        available, _, InvestmentRecommender, _ = load_ml_modules()
        if not available or InvestmentRecommender is None:
            return jsonify({
                'success': False, 
                'error': 'Investment recommender module not available',
                'recommendations': [],
                'note': 'Ensure investment_recommender.py is available and dependencies are installed'
            }), 200

        try:
            recommender = InvestmentRecommender()
            recommendations = recommender.get_portfolio_recommendations(risk_profile)
        except Exception as rec_error:
            logging.error(f"Recommendation generation error: {rec_error}")
            return jsonify({
                'success': False,
                'error': f'Could not generate recommendations: {str(rec_error)}',
                'recommendations': [],
                'note': 'Ensure sufficient crypto data is available'
            }), 200
        
        # Cache results
        with recommendation_cache['lock']:
            recommendation_cache['data'] = recommendations
            recommendation_cache['timestamp'] = datetime.now()
        response = {
            'success': True,
            'recommendations': recommendations if recommendations else [],
            'timestamp': datetime.now().isoformat()
        }
        if not recommendations:
            response['note'] = 'No recommendations generated. Need more historical data (at least 24 hours of collection).'
            response['suggestion'] = 'Continue running the data collector to build up sufficient data.'

        return jsonify(response)
    
    except Exception as e:
        logging.error(f"Error getting recommendations: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False, 
            'error': str(e),
            'recommendations': [],
            'note': 'An unexpected error occurred. Check server logs for details.'
        }), 200

@app.route('/api/portfolio/optimize')
def optimize_portfolio():
    """Get optimized portfolio allocation"""
    try:
        top_n = int(request.args.get('top_n', 15))
        
        # Lazy import optimizer
        available, _, _, PortfolioOptimizer = load_ml_modules()
        if not available or PortfolioOptimizer is None:
            return jsonify({
                'success': False, 
                'error': 'Portfolio optimizer module not available',
                'portfolio': None,
                'note': 'Ensure portfolio_optimizer.py is available and dependencies are installed'
            }), 200

        try:
            optimizer = PortfolioOptimizer()
            portfolio = optimizer.create_simple_portfolio(top_n=top_n)
        except Exception as opt_error:
            logging.error(f"Portfolio optimization error: {opt_error}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': f'Could not create portfolio: {str(opt_error)}',
                'portfolio': None,
                'note': 'Ensure sufficient crypto data with price history is available',
                'suggestion': 'The system needs at least 24-48 hours of collected data for meaningful portfolio optimization.'
            }), 200
        
        if not portfolio:
            return jsonify({
                'success': False, 
                'error': 'Unable to create portfolio (insufficient data)',
                'portfolio': None,
                'note': 'Need more cryptocurrencies with sufficient price history',
                'suggestion': 'Ensure data collector has been running for at least 24 hours'
            }), 200

        # Return in format expected by the HTML page
        return jsonify({
            'success': True,
            'portfolio': portfolio,
            'strategy': portfolio,  # Also include as 'strategy' for backward compatibility
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logging.error(f"Error optimizing portfolio: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False, 
            'error': str(e),
            'portfolio': None,
            'note': 'An unexpected error occurred. Check server logs for details.'
        }), 200

@app.route('/api/market/insights')
def get_market_insights():
    """Get overall market insights and trends"""
    try:
        # Get recent data
        data = load_dashboard_data()
        # The dashboard stores crypto records under data['crypto']['all_crypto']
        crypto_list = data.get('crypto', {}).get('all_crypto', [])

        if not crypto_list:
            return jsonify({'success': False, 'error': 'No market data available'}), 404

        # Calculate simple market insights
        total_cryptos = len(crypto_list)
        avg_change = sum(float(c.get('change_24h', 0) or 0) for c in crypto_list) / total_cryptos if total_cryptos > 0 else 0

        # Get top gainers and losers
        sorted_crypto = sorted(crypto_list, key=lambda x: float(x.get('change_24h', 0) or 0), reverse=True)
        top_gainers = sorted_crypto[:5]
        top_losers = sorted_crypto[-5:]
        
        insights = {
            'timestamp': datetime.now().isoformat(),
            'market_summary': {
                'total_cryptocurrencies': total_cryptos,
                'average_24h_change': round(avg_change, 2),
                'market_sentiment': 'Bullish' if avg_change > 2 else 'Bearish' if avg_change < -2 else 'Neutral'
            },
            'top_gainers': top_gainers,
            'top_losers': top_losers
        }
        
        return jsonify({
            'success': True,
            'insights': insights,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logging.error(f"Error getting market insights: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/documentation')
def get_documentation():
    """Serve the project documentation JSON"""
    try:
        doc_path = '/home/krenuser/big-data-dashboard/project_documentation.json'
        with open(doc_path, 'r', encoding='utf-8') as f:
            documentation = json.load(f)
        return jsonify({
            'success': True,
            'documentation': documentation
        })
    except Exception as e:
        logging.error(f"Error loading documentation: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cluster/info')
def get_cluster_info():
    """Get cluster configuration info (without credentials)"""
    cluster_info = {
        'success': True,
        'cluster': {
            'architecture': 'Master-Worker (Standalone Mode)',
            'total_vms': 10,
            'master': {
                'vm_name': 'VM5',
                'ip': '10.0.0.8',
                'ports': {
                    'spark': 7077,
                    'web_ui': 8080,
                    'dashboard': 5003
                }
            },
            'workers': [
                {'vm': 'VM1', 'ip': '10.0.0.4'},
                {'vm': 'VM2', 'ip': '10.0.0.5'},
                {'vm': 'VM3', 'ip': '10.0.0.6'},
                {'vm': 'VM4', 'ip': '10.0.0.7'},
                {'vm': 'VM6', 'ip': '10.0.0.9'},
                {'vm': 'VM7', 'ip': '10.0.0.10'},
                {'vm': 'VM8', 'ip': '10.0.0.11'},
                {'vm': 'VM9', 'ip': '10.0.0.12'},
                {'vm': 'VM10', 'ip': '10.0.0.13'}
            ],
            'configuration': {
                'executor_instances': 9,
                'executor_cores': 4,
                'executor_memory': '8GB',
                'driver_memory': '8GB',
                'total_cores': 36,
                'total_memory': '72GB'
            }
        },
        'data_stats': {
            'crypto_files': len(glob.glob('/home/krenuser/big-data-dashboard/data/crypto/**/*.parquet', recursive=True)),
            'stock_files': len(glob.glob('/home/krenuser/big-data-dashboard/data/stocks/**/*.parquet', recursive=True)),
            'collection_period': '8 days (Nov 30 - Dec 8, 2025)'
        },
        'security_note': 'VM credentials are not exposed in this API for security reasons.'
    }
    return jsonify(cluster_info)

if __name__ == '__main__':
    print("="*80)
    print(" 🚀 BIG DATA DASHBOARD - REAL-TIME PARQUET READER")
    print("="*80)
    print("\n📊 Dashboard i disponueshëm në:")
    print("   - http://localhost:5000")
    print("   - http://10.0.0.8:5000")
    print("\n📄 Faqet:")
    print("   / ................ Ballina (Overview)")
    print("   /reddit .......... Analiza e Reddit")
    print("   /stocks .......... Analiza e Aksioneve (REAL-TIME)")
    print("   /crypto .......... Analiza e Kriptovalutave (REAL-TIME)")
    print("   /api/data ........ JSON API (REAL-TIME)")
    print("   /api/refresh ..... Refresh Status")
    print("\n🔄 Data Source: Live Parquet files from 10 VMs")
    print("   - Auto-refreshes every request")
    print("   - Shows ALL collected data from VMs")
    print("\n⚠️  Shtyp CTRL+C për të ndaluar serverin")
    print("="*80 + "\n")
    
    # Test data loading
    try:
        print("🔍 Testing data availability...")
        # Don't actually load data at startup, just check if directories exist
        crypto_dir = "/home/krenuser/big-data-dashboard/databaza.csv"
        stocks_dir = "/home/krenuser/big-data-dashboard/databaza-stocks.csv"
        
        crypto_exists = os.path.exists(crypto_dir)
        stocks_exists = os.path.exists(stocks_dir)
        
        print(f"✓ Data directories checked:")
        print(f"  - Crypto data: {'✓ Available' if crypto_exists else '✗ Not found'}")
        print(f"  - Stock data: {'✓ Available' if stocks_exists else '✗ Not found'}")
        print(f"  - Data will be loaded on first request\n")
    except Exception as e:
        print(f"⚠️  Error checking data: {e}")
        print("   Dashboard will attempt to load data on each request.\n")
    
    try:
        # Nis Flask server
        app.run(host='0.0.0.0', port=5003, debug=False, threaded=True)
    except KeyboardInterrupt:
        print("\n\n⚠️  Dashboard stopped by user")
    except Exception as e:
        print(f"\n❌ Dashboard error: {e}")
        raise
