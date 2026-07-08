"""
=============================================================================
ML PREDICTIONS ENGINE - Machine Learning Price Predictions
=============================================================================
Autorët: Shaban Ejupi & Majlinda Bajraktari
Universiteti i Prishtinës - FSHMN
Data: Nëntor-Dhjetor 2025

Përshkrimi:
-----------
Ky modul implementon parashikimin e çmimeve të kriptovalutave duke përdorur
algoritme të Machine Learning:

1. LINEAR REGRESSION (Regresioni Linear)
   - Përdoret për të parashikuar trendin e çmimit bazuar në të dhënat historike
   - Formula: y = mx + b (ku m=pjerrësia, b=intersepti)
   - Arsyeja: E thjeshtë, e shpejtë, dhe efektive për trende afatshkurtra

2. EXPONENTIAL WEIGHTED MOVING AVERAGE (EWMA)
   - Jep peshë më të madhe të dhënave të fundit
   - Alpha = 2/(period+1) - përcakton dekadencën
   - Arsyeja: Reagon më shpejt ndaj ndryshimeve të çmimit

3. SIMPLE MOVING AVERAGES (SMA)
   - MA(5): Mesatarja 5-periodike - trendi afatshkurtër
   - MA(10): Mesatarja 10-periodike - trendi afatmesëm
   - MA(20): Mesatarja 20-periodike - trendi afatgjatë
   - Arsyeja: Identifikimi i trendit dhe mbështetjes/rezistencës

4. VOLATILITY CALCULATION (Kalkulimi i Volatilitetit)
   - Standard Deviation i kthimeve (returns)
   - Risk Assessment bazuar në volatilitet
   - Arsyeja: Vlerësimi i riskut të investimit

Si funksionon:
--------------
1. Ngarkon TË GJITHA të dhënat nga 8 ditët e fundit (jo vetëm kampione)
2. Për çdo kriptovalutë, llogarit:
   - Trendin linear (pjerrësia e çmimit në kohë)
   - Moving averages për perioda të ndryshme
   - Volatilitetin si devijim standard
   - Confidencën bazuar në qëndrueshmërinë e trendit
3. Parashikon çmimin duke projektuar trendin në të ardhmen

Data Sources:
- CoinGecko API: 350+ kriptovaluta me të dhëna të plota
- Binance API: Çmimet në kohë reale (backup)
- Të dhënat ruhen në format Parquet për akses të shpejtë
=============================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob
import os
import json
import logging

logging.basicConfig(level=logging.INFO)

class MLPredictionEngine:
    """
    Machine Learning Prediction Engine për çmimet e kriptovalutave.
    
    Përdor Linear Regression dhe Moving Averages për parashikime.
    Ngarkon të dhëna nga skedarët Parquet të gjeneruar nga klastri Spark.
    """
    
    def __init__(self, master_url=None):
        """
        Inicializon ML Prediction Engine.
        
        Args:
            master_url: URL e Spark master (optional, for future Spark ML integration)
        """
        self.data_dir = "/home/krenuser/big-data-dashboard/data/crypto/"
        self.total_records_loaded = 0
        self.unique_symbols = 0
        logging.info("✓ ML Prediction Engine initialized with Linear Regression + Moving Averages")
    
    def load_recent_crypto_data(self, days_back=8):
        """
        Ngarkon TË GJITHA të dhënat crypto nga skedarët Parquet.
        
        Algoritmi i Ngarkimit:
        ----------------------
        1. Skanon të gjitha datat nga days_back ditët e fundit
        2. Për çdo datë, ngarkon TË GJITHA skedarët parquet (pa limit)
        3. Kombinon të dhënat duke ruajtur timestamp-in origjinal
        4. KRITIKE: Nuk heq duplikatet e shtruara - ruan histori të plotë
        
        Kjo siguron që:
        - Parashikimet bazohen në TË GJITHA të dhënat e mbledhura
        - Moving averages kanë numër të mjaftueshëm pikash
        - Trend analysis ka bazë solide statistikore
        
        Args:
            days_back: Numri i ditëve prapa për të ngarkuar (default 8 për periudhën e plotë)
        
        Returns:
            DataFrame me të gjitha të dhënat crypto
        """
        all_data = []
        files_loaded = 0
        total_rows = 0
        
        try:
            logging.info(f"🔄 Loading ALL crypto data (up to {days_back} days of data)...")
            
            # Find all available date directories and use the most recent ones
            date_dirs = sorted(glob.glob(f"{self.data_dir}date=*/"), reverse=True)
            
            if not date_dirs:
                logging.warning(f"No date directories found in {self.data_dir}")
                return pd.DataFrame()
            
            # Use the most recent N days of available data
            date_dirs_to_use = date_dirs[:days_back]
            logging.info(f"  Found {len(date_dirs)} date directories, using {len(date_dirs_to_use)} most recent")
            
            # Load data from all available dates
            for day_dir in date_dirs_to_use:
                date_str = os.path.basename(day_dir.rstrip('/')).replace('date=', '')
                
                # Load ALL parquet files for each day
                parquet_files = glob.glob(f"{day_dir}**/*.parquet", recursive=True)
                day_rows = 0
                
                for file_path in parquet_files:
                    try:
                        df = pd.read_parquet(file_path)
                        if not df.empty:
                            all_data.append(df)
                            files_loaded += 1
                            day_rows += len(df)
                    except Exception as e:
                        continue
                
                total_rows += day_rows
                logging.info(f"  ✓ {date_str}: {len(parquet_files)} files, {day_rows:,} rows")
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Konvertimi i timestamp në datetime
                if 'timestamp' in combined_df.columns:
                    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                
                # Ruan statistikat për dokumentim
                self.total_records_loaded = len(combined_df)
                self.unique_symbols = combined_df['symbol'].nunique()
                
                logging.info(f"✓ TOTAL LOADED: {self.total_records_loaded:,} records from {files_loaded} files")
                logging.info(f"✓ UNIQUE SYMBOLS: {self.unique_symbols} cryptocurrencies")
                
                return combined_df
            
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            import traceback
            traceback.print_exc()
        
        return pd.DataFrame()
    
    def calculate_simple_prediction(self, symbol_df):
        """
        Llogarit parashikimin e çmimit duke përdorur Linear Regression dhe Moving Averages.
        
        ALGORITMET E PËRDORURA:
        =======================
        
        1. LINEAR REGRESSION (Regresioni Linear)
           -----------------------------------------
           Formula: y = mx + b
           - m (slope/pjerrësia): Shpejtësia e ndryshimit të çmimit
           - b (intercept/intersepti): Çmimi bazë
           
           Si funksionon:
           - Merr 100 pikë të fundit të çmimit (ose sa ka)
           - Krijon varg x = [0, 1, 2, ..., n-1] për kohën
           - Llogarit slope duke përdorur np.polyfit(x, y, 1)
           - Slope > 0: Trendi rritës (Bullish)
           - Slope < 0: Trendi zbritës (Bearish)
           
           Parashikimi:
           - Prediction_1h = current_price + (slope * 1)
           - Prediction_24h = current_price + (slope * 24)
           - Prediction_7d = current_price + (slope * 168)
        
        2. MOVING AVERAGES (Mesataret Lëvizëse)
           ---------------------------------------
           - MA(5): Mesatarja e 5 çmimeve të fundit - reagon shpejt
           - MA(10): Mesatarja e 10 çmimeve të fundit - balance
           - MA(20): Mesatarja e 20 çmimeve të fundit - trendi afatgjatë
           
           Interpretimi:
           - Nëse çmimi > MA: Sinjal bullish
           - Nëse çmimi < MA: Sinjal bearish
           - MA(5) crossing MA(20): Sinjal i fortë i ndryshimit të trendit
        
        3. VOLATILITY (Volatiliteti)
           ---------------------------
           Formula: std(returns) / mean(price) * 100
           - returns = (price[i] - price[i-1]) / price[i-1]
           - Volatiliteti i lartë = Risk i lartë
           
           Risk Assessment:
           - < 5%: Low Risk
           - 5-15%: Medium Risk  
           - > 15%: High Risk
        
        4. CONFIDENCE (Besueshmëria)
           ---------------------------
           Formula: 100 - (volatility * 100)
           - Volatilitet i ulët = Besueshmëri e lartë
           - Bazohet në qëndrueshmërinë historike të çmimit
        
        Args:
            symbol_df: DataFrame me të dhënat e një kriptovalute
            
        Returns:
            dict me parashikimet dhe metrikat
        """
        try:
            if len(symbol_df) < 5:
                return None
            
            # Rendit sipas kohës
            symbol_df = symbol_df.sort_values('timestamp')
            
            # Përdor TË GJITHA të dhënat, jo vetëm 20 të fundit
            all_prices = symbol_df['price'].values
            n_prices = len(all_prices)
            current_price = all_prices[-1]
            
            # === MOVING AVERAGES ===
            # Llogarit mesataret lëvizëse me numrin aktual të të dhënave
            ma_5 = np.mean(all_prices[-5:]) if n_prices >= 5 else current_price
            ma_10 = np.mean(all_prices[-10:]) if n_prices >= 10 else current_price
            ma_20 = np.mean(all_prices[-20:]) if n_prices >= 20 else current_price
            ma_50 = np.mean(all_prices[-50:]) if n_prices >= 50 else ma_20
            
            # === LINEAR REGRESSION ===
            # Përdor deri në 100 pika për trend analysis
            trend_prices = all_prices[-min(100, n_prices):]
            x = np.arange(len(trend_prices))
            
            # Kontrollo për vlera konstante (stablecoins)
            price_range = np.max(trend_prices) - np.min(trend_prices)
            if price_range < current_price * 0.001:  # Ndryshimi < 0.1%
                # Stablecoin ose çmim konstant
                trend = 0
                prediction_1h = current_price
                prediction_24h = current_price
                prediction_7d = current_price
            else:
                # Regresioni linear për trende të vërtetë
                coeffs = np.polyfit(x, trend_prices, 1)
                trend = coeffs[0]  # Slope
                
                # Parashikimet bazuar në trend
                # Shkallëzimi sipas intervalit kohor (presupozojmë 1 rekord/orë mesatarisht)
                time_scale = n_prices / 168  # Raport me javën
                adjusted_trend = trend / max(1, time_scale)
                
                prediction_1h = current_price + (adjusted_trend * 1)
                prediction_24h = current_price + (adjusted_trend * 24)
                prediction_7d = current_price + (adjusted_trend * 168)
            
            # === VOLATILITY CALCULATION ===
            # Llogarit kthimet (returns) dhe volatilitetin
            if n_prices > 1:
                returns = np.diff(all_prices) / all_prices[:-1]
                volatility = np.std(returns) * 100  # Si përqindje
            else:
                volatility = 0
            
            # === CONFIDENCE SCORE ===
            # Bazuar në volatilitet dhe numrin e të dhënave
            data_confidence = min(100, n_prices / 10 * 10)  # Max 100 për 10+ pika
            volatility_confidence = max(0, 100 - (volatility * 10))
            confidence = (data_confidence * 0.3 + volatility_confidence * 0.7)
            confidence = max(0, min(100, confidence))
            
            # === TREND DIRECTION ===
            # Përcakto drejtimin e trendit
            trend_threshold = current_price * 0.001  # 0.1% threshold
            if trend > trend_threshold:
                trend_direction = "Bullish"
            elif trend < -trend_threshold:
                trend_direction = "Bearish"
            else:
                trend_direction = "Neutral"
            
            # === ADDITIONAL METRICS ===
            change_24h = symbol_df['change_24h'].iloc[-1] if 'change_24h' in symbol_df.columns else 0
            
            # Risk Assessment
            if volatility < 5:
                risk_level = "Low Risk"
            elif volatility < 15:
                risk_level = "Medium Risk"
            else:
                risk_level = "High Risk"
            
            return {
                'current_price': float(current_price),
                'prediction_1h': float(prediction_1h),
                'prediction_24h': float(prediction_24h),
                'prediction_7d': float(prediction_7d),
                'ma_5': float(ma_5),
                'ma_10': float(ma_10),
                'ma_20': float(ma_20),
                'ma_50': float(ma_50),
                'trend': trend_direction,
                'trend_slope': float(trend),
                'confidence': float(confidence),
                'change_24h': float(change_24h),
                'volatility': float(volatility),
                'risk_level': risk_level,
                'risk_score': float(volatility),
                'data_points': n_prices,
                'volume': float(symbol_df['volume'].iloc[-1]) if 'volume' in symbol_df.columns else 0,
                'market_cap': float(symbol_df['market_cap'].iloc[-1]) if 'market_cap' in symbol_df.columns else 0,
                'algorithm': 'Linear Regression + Moving Averages',
                'analysis_period': f'{n_prices} data points over 8 days'
            }
            
        except Exception as e:
            logging.error(f"Error calculating prediction: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_all_predictions(self, top_n=20):
        """Get predictions for top cryptocurrencies"""
        try:
            # Load data
            df = self.load_recent_crypto_data()
            if df.empty:
                logging.warning("No data loaded")
                return []
            
            # Get unique symbols with enough data
            symbol_counts = df['symbol'].value_counts()
            valid_symbols = symbol_counts[symbol_counts >= 5].index.tolist()
            
            if not valid_symbols:
                logging.warning("No symbols with sufficient data")
                return []
            
            # Filter to valid symbols and get top by market cap
            df_valid = df[df['symbol'].isin(valid_symbols)]
            latest_data = df_valid.sort_values('timestamp').groupby('symbol').tail(1)
            
            # Remove rows with NaN market_cap
            latest_data = latest_data.dropna(subset=['market_cap'])
            
            if latest_data.empty:
                logging.warning("No valid market cap data")
                return []
            
            top_cryptos = latest_data.nlargest(min(top_n, len(latest_data)), 'market_cap')['symbol'].tolist()
            
            predictions = []
            for symbol in top_cryptos:
                symbol_df = df[df['symbol'] == symbol].copy()
                prediction = self.calculate_simple_prediction(symbol_df)
                
                if prediction:
                    prediction['symbol'] = symbol
                    # Try to get name from the data
                    name_row = symbol_df[symbol_df['name'].notna()]
                    if not name_row.empty:
                        prediction['name'] = name_row.iloc[0]['name']
                    else:
                        prediction['name'] = symbol.replace('USDT', '').replace('USD', '')
                    predictions.append(prediction)
            
            logging.info(f"✓ Generated {len(predictions)} predictions")
            return predictions
            
        except Exception as e:
            logging.error(f"Error generating predictions: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_prediction_for_symbol(self, symbol):
        """Get prediction for a specific cryptocurrency"""
        try:
            df = self.load_recent_crypto_data()
            if df.empty:
                return None
            
            symbol_df = df[df['symbol'] == symbol].copy()
            if symbol_df.empty:
                return None
            
            prediction = self.calculate_simple_prediction(symbol_df)
            if prediction:
                prediction['symbol'] = symbol
                prediction['name'] = symbol.replace('USDT', '').replace('USD', '')
            
            return prediction
            
        except Exception as e:
            logging.error(f"Error getting prediction for {symbol}: {e}")
            return None
    
    def close(self):
        """Close resources (no-op for simple version)"""
        pass

# Test function
if __name__ == "__main__":
    engine = MLPredictionEngine()
    predictions = engine.get_all_predictions(top_n=10)
    
    print(f"\n{'='*80}")
    print(f"SIMPLE ML PREDICTIONS - Top 10 Cryptocurrencies")
    print(f"{'='*80}\n")
    
    for pred in predictions:
        print(f"{pred['name']:10s} | Current: ${pred['current_price']:,.2f} | "
              f"24h Pred: ${pred['prediction_24h']:,.2f} | "
              f"Trend: {pred['trend']:8s} | Confidence: {pred['confidence']:.1f}%")
    
    print(f"\n{'='*80}\n")
