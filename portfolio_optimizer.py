"""
=============================================================================
PORTFOLIO OPTIMIZER - Modern Portfolio Theory Implementation
=============================================================================
Autorët: Shaban Ejupi & Majlinda Bajraktari
Universiteti i Prishtinës - FSHMN
Data: Nëntor-Dhjetor 2025

Përshkrimi:
-----------
Ky modul implementon optimizimin e portofolit bazuar në Modern Portfolio Theory
(MPT) të Harry Markowitz. Qëllimi është maksimizimi i kthimit të pritur për
një nivel të caktuar risku, ose minimizimi i riskut për një kthim të caktuar.

ALGORITMET E PËRDORURA:
=======================

1. MODERN PORTFOLIO THEORY (MPT) - Markowitz Mean-Variance
   -------------------------------------------------------
   Koncepti Bazë:
   - Diversifikimi zvogëlon riskun pa ulur kthimin e pritur
   - Kombinimi optimal i aseteve minimizon variancën e portofolit
   
   Formula për Kthimin e Pritur të Portofolit:
   E(Rp) = Σ wi * E(Ri)
   ku:
   - wi = pesha e asetit i në portofol
   - E(Ri) = kthimi i pritur i asetit i
   
   Formula për Riskun e Portofolit:
   σp² = Σ Σ wi * wj * σi * σj * ρij
   ku:
   - σi, σj = devijimet standarde
   - ρij = korrelacioni ndërmjet aseteve

2. SHARPE RATIO - Optimizimi i Kthimit të Rregulluar për Risk
   ----------------------------------------------------------
   Formula: (Rp - Rf) / σp
   ku:
   - Rp = kthimi i portofolit
   - Rf = norma pa risk (4% vjetore)
   - σp = volatiliteti i portofolit
   
   Interpretimi:
   - Sharpe > 1: Kthim i mirë për riskun e marrë
   - Sharpe > 2: Kthim shumë i mirë
   - Sharpe < 0: Kthim nën normën pa risk

3. RISK PARITY - Kontribut i Barabartë i Riskut
   ---------------------------------------------
   Koncept:
   - Çdo aset kontribuon barabar në riskun total
   - Asetet më të paqëndrueshme marrin peshë më të vogël
   
   Implementimi:
   - weight_i = 1/volatility_i / Σ(1/volatility_j)
   - Më pak volatil = Peshë më e madhe

4. STRATEGJITË E PORTOFOLIT
   ------------------------
   
   a) Conservative (Konservative):
      - Fokus: Ruajtja e kapitalit
      - Metodë: Inverse Volatility Weighting
      - Asetet: Top 8 me volatilitet më të ulët
      - Risk Level: Low
   
   b) Balanced (E Balancuar):
      - Fokus: Balancë rritje/stabilitet
      - Metodë: Sharpe Ratio ranking me pesha zbritëse
      - Asetet: Top 10 sipas Sharpe Ratio
      - Risk Level: Medium
   
   c) Aggressive (Agresive):
      - Fokus: Maksimizimi i kthimit
      - Metodë: Return-weighted allocation
      - Asetet: Top 10 sipas kthimit të pritur
      - Risk Level: High

5. METRIKAT E ASETEVE
   ------------------
   
   Expected Return (Kthimi i Pritur):
   - Formula: mean(daily_returns) * 365
   - Annualizimi i kthimeve ditore
   
   Volatility (Volatiliteti):
   - Formula: std(daily_returns) * sqrt(365)
   - Annualizimi i devijimit standard
   
   Daily Returns:
   - Formula: (price[t] - price[t-1]) / price[t-1]
   - Kthimet logaritmike ose aritmetike

Data Sources:
- Të dhënat historike nga 8 ditët e fundit
- 80,000+ rekorde nga 350+ kriptovaluta
- Format Parquet për lexim të shpejtë
=============================================================================
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import glob
import os
import logging
import time
import concurrent.futures

logging.basicConfig(level=logging.INFO)

class PortfolioOptimizer:
    """
    Optimizuesi i Portofolit bazuar në Modern Portfolio Theory.
    
    Implementon tre strategji: Conservative, Balanced, Aggressive.
    Përdor Sharpe Ratio dhe Inverse Volatility për alokimin e aseteve.
    """
    
    def __init__(self, risk_free_rate=0.04):
        """
        Inicializon Portfolio Optimizer.
        
        Args:
            risk_free_rate: Norma vjetore pa risk (default 4% - T-Bills)
        """
        self.risk_free_rate = risk_free_rate
        self.data_dir = "/home/krenuser/big-data-dashboard/data/crypto/"
        self.total_records = 0
        self.assets_analyzed = 0
        logging.info(f"✓ Portfolio Optimizer initialized (Risk-free rate: {risk_free_rate*100}%)")
    
    def load_crypto_data(self, days_back=8):
        """
        Ngarkon TË GJITHA të dhënat për optimizimin e portofolit.
        
        Algoritmi:
        ----------
        1. Lexon të gjitha skedarët Parquet nga 8 ditët e fundit
        2. Kombinon të dhënat duke ruajtur kronologjinë
        3. Nuk heq duplikate - nevojiten për kalkulimin e returns
        
        Modern Portfolio Theory kërkon:
        - Seria kohore të çmimeve për çdo aset
        - Mjaft pika për kalkulim statistikor të besueshëm
        - Të dhëna të pastra (pa NaN për çmimet)
        
        Returns:
            DataFrame me të gjitha të dhënat historike
        """
        all_data = []
        files_loaded = 0
        
        try:
            logging.info(f"🔄 Loading ALL portfolio data (up to {days_back} days of data)...")
            
            # Find all available date directories and use the most recent ones
            date_dirs = sorted(glob.glob(f"{self.data_dir}date=*/"), reverse=True)
            
            if not date_dirs:
                logging.warning(f"No date directories found in {self.data_dir}")
                return pd.DataFrame()
            
            # Use the most recent N days of available data
            date_dirs_to_use = date_dirs[:days_back]
            logging.info(f"  Found {len(date_dirs)} date directories, using {len(date_dirs_to_use)} most recent")
            
            for day_dir in date_dirs_to_use:
                date_str = os.path.basename(day_dir.rstrip('/')).replace('date=', '')
                
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
                
                logging.info(f"  ✓ {date_str}: {len(parquet_files)} files, {day_rows:,} rows")
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                if 'timestamp' in combined_df.columns:
                    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                
                self.total_records = len(combined_df)
                logging.info(f"✓ TOTAL LOADED: {self.total_records:,} records from {files_loaded} files")
                
                return combined_df
            
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            import traceback
            traceback.print_exc()
        
        return pd.DataFrame()
    
    def calculate_asset_metrics(self, symbol_df):
        """
        Llogarit metrikat e asetit për optimizimin e portofolit.
        
        Metrikat e Kalkuluara:
        ----------------------
        
        1. EXPECTED RETURN (Kthimi i Pritur)
           Formula: mean(daily_returns) * 365
           - daily_returns = (price[t] - price[t-1]) / price[t-1]
           - Annualizimi me faktorin 365
           - Rezultati në përqindje (e.g., 150% = 1.5x rritje vjetore)
        
        2. VOLATILITY (Volatiliteti)
           Formula: std(daily_returns) * sqrt(365)
           - Annualizimi i devijimit standard
           - Rezultati në përqindje
           - Volatilitet i lartë = Risk i lartë
        
        3. SHARPE RATIO
           Formula: (expected_return - risk_free_rate) / volatility
           - Matës i kthimit të rregulluar për risk
           - Sharpe > 1: Mirë
           - Sharpe > 2: Shumë mirë
           - Sharpe < 0: Kthim nën normën pa risk
        
        Args:
            symbol_df: DataFrame me të dhënat e një aseti
            
        Returns:
            dict me metrikat ose None nëse s'ka mjaft të dhëna
        """
        try:
            if len(symbol_df) < 5:
                return None
            
            symbol_df = symbol_df.sort_values('timestamp')

            # Keep only the most recent points to speed up calculations
            # (e.g. last ~240 points -> ~10 days hourly). This reduces
            # computation time while preserving recent behavior.
            MAX_POINTS = 240
            if len(symbol_df) > MAX_POINTS:
                symbol_df = symbol_df.tail(MAX_POINTS).copy()

            # Të dhënat bazë
            current_price = symbol_df['price'].iloc[-1]
            prices = symbol_df['price'].values
            n_prices = len(prices)
            
            # === DAILY RETURNS ===
            # Kalkulon kthimet (returns) nga seria e çmimeve
            returns = np.diff(prices) / prices[:-1]
            
            # Filtro vlerat ekstreme (outliers) që mund të shtrembërojnë kalkulimin
            returns = returns[np.isfinite(returns)]
            if len(returns) < 3:
                return None
            
            # === EXPECTED RETURN (Annualized) ===
            # Supozojmë se të dhënat janë afërsisht çdo orë
            # Kemi ~24 pika/ditë, prandaj shkallëzojmë
            periods_per_year = 365 * 24  # Nëse të dhënat janë çdo orë
            
            avg_return = np.mean(returns)
            # Normalizimi bazuar në numrin aktual të të dhënave
            time_span_hours = n_prices  # Numri i pikave ≈ orë
            time_span_days = time_span_hours / 24
            
            # Annualized return
            if time_span_days > 0:
                daily_return = avg_return * (24 / max(1, time_span_hours / time_span_days))
                expected_return = daily_return * 365
            else:
                expected_return = avg_return * 365
            
            # === VOLATILITY (Annualized) ===
            # Devijimi standard i kthimeve, annualized
            volatility = np.std(returns) * np.sqrt(periods_per_year / (time_span_hours / max(1, time_span_days)))
            
            # Cap volatility to reasonable values
            volatility = min(volatility, 500)  # Max 500% annual volatility
            
            # === SHARPE RATIO ===
            if volatility > 0.0001:  # Shmang ndarjen me zero
                sharpe = (expected_return - self.risk_free_rate) / volatility
            else:
                sharpe = 0
            
            # Cap Sharpe to reasonable values
            sharpe = max(-10, min(10, sharpe))
            
            # === ADDITIONAL METRICS ===
            market_cap = symbol_df['market_cap'].iloc[-1]
            volume = symbol_df['volume'].iloc[-1]
            change_24h = symbol_df['change_24h'].iloc[-1] if 'change_24h' in symbol_df.columns else 0
            
            return {
                'current_price': float(current_price),
                'expected_return': float(expected_return * 100),  # Si përqindje
                'volatility': float(volatility * 100),  # Si përqindje
                'sharpe_ratio': float(sharpe),
                'market_cap': float(market_cap),
                'volume': float(volume),
                'change_24h': float(change_24h),
                'data_points': n_prices,
                'time_span_days': round(time_span_days, 1)
            }
            
        except Exception as e:
            logging.error(f"Error calculating metrics: {e}")
            return None
    
    def create_simple_portfolio(self, top_n=15):
        """Create a simple diversified portfolio with basic allocation"""
        try:
            # Load data
            df = self.load_crypto_data()
            if df.empty:
                return None
            
            # Get top cryptos by market cap
            latest_data = df.sort_values('timestamp').groupby('symbol').tail(1)
            top_cryptos = latest_data.nlargest(top_n, 'market_cap')
            
            assets = []

            # Parallelize metric calculations to use multiple cores and
            # speed up processing when many symbols exist.
            symbols = top_cryptos['symbol'].tolist()
            max_workers = min(8, (os.cpu_count() or 4))

            def _compute(symbol):
                try:
                    symbol_df = df[df['symbol'] == symbol].copy()
                    metrics = self.calculate_asset_metrics(symbol_df)
                    if metrics:
                        metrics['symbol'] = symbol
                        metrics['name'] = symbol.replace('USDT', '').replace('USD', '')
                        return metrics
                except Exception:
                    return None
                return None

            start_time = time.time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for res in executor.map(_compute, symbols):
                    if res:
                        assets.append(res)
            elapsed = time.time() - start_time
            logging.info(f"✓ Computed metrics for {len(assets)} assets in {elapsed:.1f}s using {max_workers} workers")
            
            if not assets:
                return None
            
            # Sort by Sharpe ratio (risk-adjusted return)
            assets.sort(key=lambda x: x['sharpe_ratio'], reverse=True)
            
            # Create three portfolio strategies
            portfolios = {
                'conservative': self._create_conservative_portfolio(assets),
                'balanced': self._create_balanced_portfolio(assets),
                'aggressive': self._create_aggressive_portfolio(assets)
            }
            
            result = {
                'timestamp': datetime.now().isoformat(),
                'portfolios': portfolios,
                'assets': assets[:10],  # Top 10 assets
                'recommendations': self._generate_recommendations(portfolios)
            }
            
            logging.info(f"✓ Created portfolio with {len(assets)} assets")
            return result
            
        except Exception as e:
            logging.error(f"Error creating portfolio: {e}")
            return None
    
    def _create_conservative_portfolio(self, assets):
        """Conservative portfolio: Low risk, stable assets"""
        # Focus on low volatility assets
        low_vol_assets = sorted(assets, key=lambda x: x['volatility'])[:8]
        
        # Allocate based on inverse volatility (lower vol = higher allocation)
        total_inv_vol = sum(1/max(a['volatility'], 1) for a in low_vol_assets)
        allocations = []
        
        for asset in low_vol_assets:
            weight = (1/max(asset['volatility'], 1)) / total_inv_vol
            allocations.append({
                'symbol': asset['symbol'],
                'name': asset['name'],
                'allocation': round(weight * 100, 2),
                'expected_return': asset['expected_return'],
                'risk': asset['volatility']
            })
        
        # Calculate portfolio metrics
        portfolio_return = sum(a['allocation']/100 * a['expected_return'] for a in allocations)
        portfolio_risk = np.mean([a['risk'] for a in allocations])  # Simplified
        
        return {
            'name': 'Conservative Portfolio',
            'description': 'Low-risk portfolio focusing on stable assets',
            'allocations': allocations,
            'expected_return': round(portfolio_return, 2),
            'risk_level': 'Low',
            'volatility': round(portfolio_risk, 2)
        }
    
    def _create_balanced_portfolio(self, assets):
        """Balanced portfolio: Mix of stability and growth"""
        # Mix of good Sharpe ratio assets
        top_sharpe = sorted(assets, key=lambda x: x['sharpe_ratio'], reverse=True)[:10]
        
        # Simple equal weight with slight bias to better Sharpe ratios
        allocations = []
        for i, asset in enumerate(top_sharpe):
            # Decreasing weight: 15%, 13%, 11%, 10%, 9%, 8%, 7%, 6%, 6%, 5%
            weight = max(5, 15 - i * 1.5)
            allocations.append({
                'symbol': asset['symbol'],
                'name': asset['name'],
                'allocation': round(weight, 2),
                'expected_return': asset['expected_return'],
                'risk': asset['volatility']
            })
        
        # Normalize to 100%
        total = sum(a['allocation'] for a in allocations)
        for a in allocations:
            a['allocation'] = round(a['allocation'] / total * 100, 2)
        
        portfolio_return = sum(a['allocation']/100 * a['expected_return'] for a in allocations)
        portfolio_risk = np.mean([a['risk'] for a in allocations])
        
        return {
            'name': 'Balanced Portfolio',
            'description': 'Balanced mix of stable and growth assets',
            'allocations': allocations,
            'expected_return': round(portfolio_return, 2),
            'risk_level': 'Medium',
            'volatility': round(portfolio_risk, 2)
        }
    
    def _create_aggressive_portfolio(self, assets):
        """Aggressive portfolio: Higher risk, higher potential return"""
        # Focus on high return potential (even with high volatility)
        high_return = sorted(assets, key=lambda x: x['expected_return'], reverse=True)[:10]
        
        # Weight by expected return
        total_return = sum(max(asset['expected_return'], 0) for asset in high_return)
        allocations = []
        
        for asset in high_return:
            if total_return > 0:
                weight = max(asset['expected_return'], 0) / total_return
            else:
                weight = 1.0 / len(high_return)
            
            allocations.append({
                'symbol': asset['symbol'],
                'name': asset['name'],
                'allocation': round(weight * 100, 2),
                'expected_return': asset['expected_return'],
                'risk': asset['volatility']
            })
        
        portfolio_return = sum(a['allocation']/100 * a['expected_return'] for a in allocations)
        portfolio_risk = np.mean([a['risk'] for a in allocations])
        
        return {
            'name': 'Aggressive Portfolio',
            'description': 'High-risk, high-return growth portfolio',
            'allocations': allocations,
            'expected_return': round(portfolio_return, 2),
            'risk_level': 'High',
            'volatility': round(portfolio_risk, 2)
        }
    
    def _generate_recommendations(self, portfolios):
        """Generate investment recommendations"""
        recommendations = []
        
        recommendations.append({
            'type': 'Conservative',
            'advice': 'Best for risk-averse investors. Focus on capital preservation.',
            'ideal_for': 'Retirement savings, long-term stability',
            'timeframe': '5+ years'
        })
        
        recommendations.append({
            'type': 'Balanced',
            'advice': 'Suitable for most investors. Good balance of growth and stability.',
            'ideal_for': 'Building wealth, medium-term goals',
            'timeframe': '3-5 years'
        })
        
        recommendations.append({
            'type': 'Aggressive',
            'advice': 'For experienced investors comfortable with high volatility.',
            'ideal_for': 'Growth-focused, speculative investments',
            'timeframe': '1-3 years'
        })
        
        return recommendations

# Test function
if __name__ == "__main__":
    optimizer = PortfolioOptimizer()
    portfolio = optimizer.create_simple_portfolio(top_n=15)
    
    if portfolio:
        print(f"\n{'='*80}")
        print(f"SIMPLE PORTFOLIO OPTIMIZER - {portfolio['timestamp']}")
        print(f"{'='*80}\n")
        
        for strategy_name, strategy in portfolio['portfolios'].items():
            print(f"\n{strategy['name']} ({strategy['risk_level']} Risk)")
            print(f"Expected Return: {strategy['expected_return']:.2f}%")
            print(f"Volatility: {strategy['volatility']:.2f}%")
            print(f"\nTop 5 Holdings:")
            for alloc in strategy['allocations'][:5]:
                print(f"  {alloc['name']:10s}: {alloc['allocation']:6.2f}%")
        
        print(f"\n{'='*80}\n")
