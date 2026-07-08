"""
=============================================================================
SMART INVESTMENT RECOMMENDATION ENGINE
=============================================================================
Autorët: Shaban Ejupi & Majlinda Bajraktari
Universiteti i Prishtinës - FSHMN
Data: Nëntor-Dhjetor 2025

Përshkrimi:
-----------
Sistemi i Rekomandimeve të Investimeve - Fokusohet në investime të qëndrueshme
dhe afatgjata me menaxhim të riskut. Paralajmëron për kriptovalutat e paqëndrueshme.

ALGORITMET E PËRDORURA:
=======================

1. STABILITY SCORE (Pikët e Qëndrueshmërisë) - 0-100
   ------------------------------------------------
   Komponenta:
   a) Price Volatility (35% weight)
      - Formula: std(price_changes) * adjustment_factor
      - Volatilitet i ulët = Pikë të larta
   
   b) Volume Consistency (25% weight)
      - Formula: std(volume) / mean(volume) - Coefficient of Variation
      - CV i ulët = Më konsistent = Pikë të larta
   
   c) Trend Consistency (20% weight)
      - Formula: 100 - std(change_24h) * 2
      - Ndryshime të vogla ditore = Më të qëndrueshme
   
   d) Market Cap Stability (20% weight)
      - Formula: min(100, market_cap / 1 billion * 10)
      - Market cap më i madh = Më i qëndrueshëm

2. GROWTH POTENTIAL (Potenciali i Rritjes) - 0-100
   -----------------------------------------------
   Komponenta:
   a) Growth Momentum (40% weight)
      - Formula: (avg_7day_change + 20) * 2.5
      - Ndryshime pozitive = Momentum i lartë
   
   b) Volume Trend (30% weight)
      - Formula: (volume_recent / volume_old - 1) * 100
      - Volum në rritje = Interes më i madh
   
   c) Market Cap Growth (30% weight)
      - Formula: (current_mc / past_mc - 1) * 100
      - Rritje e market cap = Potencial më i madh

3. LIQUIDITY RISK (Risku i Likuiditetit) - 0-100
   -----------------------------------------------
   Komponenta:
   a) Volume Risk (50% weight)
      - < $1M daily: 80 points (risky)
      - $1-10M: 50 points (moderate)
      - > $10M: 20 points (safe)
   
   b) Volume Consistency (30% weight)
      - CV i volumit - variabilitet i lartë = risk
   
   c) Slippage Risk (20% weight)
      - Bazuar në volatilitetin e çmimit
      - Volatilitet i lartë = slippage i lartë

4. INVESTMENT SCORE (Pikët e Investimit) - 0-100
   -----------------------------------------------
   Balancon stability, growth, dhe risk sipas profilit:
   
   Conservative:
   - Score = stability*0.6 + growth*0.2 - liquidity_risk*0.2
   
   Balanced:
   - Score = stability*0.4 + growth*0.4 - liquidity_risk*0.2
   
   Aggressive:
   - Score = stability*0.2 + growth*0.6 - liquidity_risk*0.2

5. KATEGORAT E INVESTIMIT
   ----------------------
   - Safe Investment: stability >= 70 AND liquidity_risk <= 30
   - Balanced Growth: stability >= 50 AND growth >= 60 AND risk <= 50
   - Growth Opportunity: growth >= 70 AND risk <= 60
   - High Risk - Liquidative: liquidity_risk >= 70
   - Highly Volatile - Risky: stability < 30
   - Moderate Risk: Të tjerat

Data Sources:
- CoinGecko API: 350+ kriptovaluta
- Binance API: Të dhëna real-time
- Të dhënat Parquet nga 10 VM cluster
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

class InvestmentRecommender:
    """
    Sistemi i Rekomandimeve për Investime në Kriptovaluta.
    
    Analizon stabilitetin, potencialin e rritjes, dhe riskun e likuiditetit
    për të gjeneruar rekomandime të personalizuara sipas profilit të riskut.
    """
    
    def __init__(self):
        self.data_dir = "/home/krenuser/big-data-dashboard/data/crypto/"
        self.total_records_analyzed = 0
        self.risk_thresholds = {
            'very_low': 15,
            'low': 30,
            'medium': 50,
            'high': 70,
            'very_high': 100
        }
        logging.info("✓ Investment Recommender initialized")
    
    def load_crypto_data(self, days_back=8):
        """
        Ngarkon TË GJITHA të dhënat historike për analizë investimi.
        
        Algoritmi i Ngarkimit:
        ----------------------
        1. Skanon të gjitha datat nga periudha 8-ditore
        2. Ngarkon ÇDOKUSH skedar parquet (pa limit)
        3. Ruan të dhënat origjinale pa hequr duplikate të shtruara
        4. Kjo siguron analizë të saktë të volatilitetit dhe trendeve
        
        Investment Analysis Metrics:
        - Stability Score: Bazuar në volatilitet të çmimit dhe volumit
        - Growth Potential: Momentum, trend volumit, rritje market cap
        - Liquidity Risk: Adekuatësia e volumit, risku i slippage
        
        Returns:
            DataFrame me të gjitha të dhënat për analizë
        """
        all_data = []
        files_loaded = 0
        
        try:
            logging.info(f"🔄 Loading ALL investment data (up to {days_back} days of data)...")
            
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
                
                # Load ALL parquet files
                parquet_files = glob.glob(f"{day_dir}**/*.parquet", recursive=True)
                day_rows = 0
                
                for file_path in parquet_files:
                    try:
                        df = pd.read_parquet(file_path)
                        if not df.empty:
                            all_data.append(df)
                            files_loaded += 1
                            day_rows += len(df)
                    except:
                        continue
                
                logging.info(f"  ✓ {date_str}: {len(parquet_files)} files, {day_rows:,} rows")
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Konverto timestamp
                if 'timestamp' in combined_df.columns:
                    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                
                self.total_records_analyzed = len(combined_df)
                logging.info(f"✓ TOTAL LOADED: {self.total_records_analyzed:,} records from {files_loaded} files")
                logging.info(f"✓ UNIQUE ASSETS: {combined_df['symbol'].nunique()} cryptocurrencies")
                
                return combined_df
        
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            import traceback
            traceback.print_exc()
        
        return pd.DataFrame()
    
    def calculate_stability_score(self, symbol_df):
        """Calculate stability score (0-100, higher is more stable)"""
        try:
            if len(symbol_df) < 10:
                return 0
            
            # Price volatility (lower is better)
            price_volatility = symbol_df['price'].pct_change().std()
            volatility_score = max(0, 100 - (price_volatility * 1000))
            
            # Volume consistency (higher is better)
            volume_cv = symbol_df['volume'].std() / symbol_df['volume'].mean() if symbol_df['volume'].mean() > 0 else 1
            volume_score = max(0, 100 - (volume_cv * 50))
            
            # Price trend consistency
            changes = symbol_df['change_24h'].values
            trend_consistency = 100 - (np.std(changes) * 2)
            trend_consistency = max(0, min(100, trend_consistency))
            
            # Market cap stability (larger is more stable)
            avg_market_cap = symbol_df['market_cap'].mean()
            market_cap_score = min(100, (avg_market_cap / 1e9) * 10)  # Billion dollar scale
            
            # Weighted average
            stability = (
                volatility_score * 0.35 +
                volume_score * 0.25 +
                trend_consistency * 0.20 +
                market_cap_score * 0.20
            )
            
            return min(100, max(0, stability))
        
        except Exception as e:
            logging.error(f"Error calculating stability: {e}")
            return 0
    
    def calculate_growth_potential(self, symbol_df):
        """Calculate growth potential score (0-100)"""
        try:
            if len(symbol_df) < 7:
                return 0
            
            # Recent performance
            recent_changes = symbol_df['change_24h'].tail(7).values
            avg_change = np.mean(recent_changes)
            growth_momentum = max(0, min(100, (avg_change + 20) * 2.5))
            
            # Volume trend (increasing volume is positive)
            volumes = symbol_df['volume'].tail(14).values
            if len(volumes) >= 7:
                volume_trend = (volumes[-7:].mean() / volumes[:7].mean() - 1) * 100 if volumes[:7].mean() > 0 else 0
                volume_score = max(0, min(100, 50 + volume_trend))
            else:
                volume_score = 50
            
            # Market cap growth
            market_caps = symbol_df['market_cap'].values
            if len(market_caps) >= 7:
                mc_trend = (market_caps[-1] / market_caps[0] - 1) * 100 if market_caps[0] > 0 else 0
                mc_score = max(0, min(100, 50 + mc_trend))
            else:
                mc_score = 50
            
            growth_potential = (
                growth_momentum * 0.4 +
                volume_score * 0.3 +
                mc_score * 0.3
            )
            
            return min(100, max(0, growth_potential))
        
        except Exception as e:
            logging.error(f"Error calculating growth potential: {e}")
            return 0
    
    def calculate_liquidity_risk(self, symbol_df):
        """Calculate liquidity risk (0-100, lower is better)"""
        try:
            if len(symbol_df) < 5:
                return 100  # High risk for insufficient data
            
            # Volume consistency
            avg_volume = symbol_df['volume'].mean()
            volume_std = symbol_df['volume'].std()
            volume_cv = (volume_std / avg_volume * 100) if avg_volume > 0 else 100
            
            # Low volume is high risk
            if avg_volume < 1000000:  # Less than $1M daily volume
                volume_risk = 80
            elif avg_volume < 10000000:  # Less than $10M
                volume_risk = 50
            else:
                volume_risk = 20
            
            # Price slippage risk (high volatility = high slippage)
            price_volatility = symbol_df['price'].pct_change().std() * 100
            slippage_risk = min(100, price_volatility * 50)
            
            liquidity_risk = (volume_risk * 0.5 + volume_cv * 0.3 + slippage_risk * 0.2)
            
            return min(100, max(0, liquidity_risk))
        
        except Exception as e:
            return 100
    
    def categorize_investment(self, stability, growth, liquidity_risk):
        """Categorize investment based on scores"""
        if stability >= 70 and liquidity_risk <= 30:
            return "Safe Investment"
        elif stability >= 50 and growth >= 60 and liquidity_risk <= 50:
            return "Balanced Growth"
        elif growth >= 70 and liquidity_risk <= 60:
            return "Growth Opportunity"
        elif liquidity_risk >= 70:
            return "High Risk - Liquidative"
        elif stability < 30:
            return "Highly Volatile - Risky"
        else:
            return "Moderate Risk"
    
    def generate_investment_advice(self, category, stability, growth, liquidity_risk):
        """Generate human-readable investment advice"""
        advice = []
        
        if category == "Safe Investment":
            advice.append("✅ RECOMMENDED: This is a stable, long-term investment option.")
            advice.append("Low volatility and good liquidity make it suitable for conservative portfolios.")
            advice.append("Consider for: Core holdings, long-term wealth preservation")
        
        elif category == "Balanced Growth":
            advice.append("✅ GOOD OPTION: Balance of stability and growth potential.")
            advice.append("Moderate risk with reasonable upside potential.")
            advice.append("Consider for: Diversified portfolio, medium-term goals")
        
        elif category == "Growth Opportunity":
            advice.append("⚠️ MODERATE RISK: High growth potential but increased volatility.")
            advice.append("Suitable for growth-oriented investors with risk tolerance.")
            advice.append("Consider for: Small allocation (5-10%), growth portfolio")
        
        elif category == "High Risk - Liquidative":
            advice.append("⛔ HIGH RISK WARNING: Poor liquidity detected!")
            advice.append("Difficulty entering/exiting positions. High slippage risk.")
            advice.append("⚠️ NOT RECOMMENDED for most investors")
        
        elif category == "Highly Volatile - Risky":
            advice.append("⛔ VOLATILITY WARNING: Extremely unstable price action!")
            advice.append("High risk of significant losses. Speculative only.")
            advice.append("⚠️ NOT RECOMMENDED unless you can afford total loss")
        
        else:
            advice.append("⚠️ MODERATE RISK: Requires careful monitoring.")
            advice.append("Consider small position sizing and stop-loss protection.")
            advice.append("Best for: Experienced investors, tactical allocation")
        
        # Additional warnings
        if liquidity_risk > 70:
            advice.append("🚨 LIQUIDITY ALERT: Low trading volume may cause large price slippage!")
        
        if stability < 20:
            advice.append("🚨 VOLATILITY ALERT: Extreme price swings detected!")
        
        return advice
    
    def get_portfolio_recommendations(self, risk_profile='conservative', top_n=30):
        """Get personalized portfolio recommendations"""
        df = self.load_crypto_data(days_back=8)  # Use full 8 days of data
        
        if df.empty:
            return []
        
        # Get top cryptocurrencies by market cap to analyze
        latest_data = df.sort_values('timestamp').groupby('symbol').tail(1)
        top_cryptos = latest_data.nlargest(top_n, 'market_cap')['symbol'].tolist()
        
        # Analyze each cryptocurrency
        recommendations = []
        for symbol in top_cryptos:
            try:
                symbol_df = df[df['symbol'] == symbol].sort_values('timestamp')
                
                if len(symbol_df) < 10:
                    continue
                
                # Calculate scores
                stability = self.calculate_stability_score(symbol_df)
                growth = self.calculate_growth_potential(symbol_df)
                liquidity_risk = self.calculate_liquidity_risk(symbol_df)
                
                # Overall investment score
                if risk_profile == 'conservative':
                    investment_score = stability * 0.6 + growth * 0.2 - liquidity_risk * 0.2
                elif risk_profile == 'balanced':
                    investment_score = stability * 0.4 + growth * 0.4 - liquidity_risk * 0.2
                else:  # aggressive
                    investment_score = stability * 0.2 + growth * 0.6 - liquidity_risk * 0.2
                
                investment_score = max(0, min(100, investment_score))
                
                category = self.categorize_investment(stability, growth, liquidity_risk)
                advice = self.generate_investment_advice(category, stability, growth, liquidity_risk)
                
                # Get current data
                latest = symbol_df.iloc[-1]
                
                recommendations.append({
                    'symbol': symbol,
                    'name': latest.get('name', symbol.replace('USDT', '').replace('USD', '')),
                    'current_price': float(latest['price']),
                    'market_cap': float(latest['market_cap']),
                    'volume_24h': float(latest['volume']),
                    'change_24h': float(latest['change_24h']),
                    'scores': {
                        'stability': round(stability, 2),
                        'growth_potential': round(growth, 2),
                        'liquidity_risk': round(liquidity_risk, 2),
                        'investment_score': round(investment_score, 2)
                    },
                    'category': category,
                    'advice': advice,
                    'recommended_allocation': self.get_allocation_percentage(investment_score, category, risk_profile)
                })
            
            except Exception as e:
                logging.error(f"Error analyzing {symbol}: {e}")
                continue
        
        # Sort by investment score
        recommendations.sort(key=lambda x: x['scores']['investment_score'], reverse=True)
        
        logging.info(f"✓ Generated {len(recommendations)} recommendations")
        return recommendations
    
    def get_allocation_percentage(self, investment_score, category, risk_profile):
        """Suggest portfolio allocation percentage"""
        if category == "Safe Investment":
            if risk_profile == 'conservative':
                return "20-30%"
            elif risk_profile == 'balanced':
                return "15-25%"
            else:
                return "10-20%"
        
        elif category == "Balanced Growth":
            if risk_profile == 'conservative':
                return "10-15%"
            elif risk_profile == 'balanced':
                return "15-20%"
            else:
                return "15-25%"
        
        elif category == "Growth Opportunity":
            if risk_profile == 'conservative':
                return "0-5%"
            elif risk_profile == 'balanced':
                return "5-10%"
            else:
                return "10-20%"
        
        else:  # High risk categories
            return "0-2% (if any)"
    
    def get_diversification_strategy(self, recommendations, total_investment=10000):
        """Create diversified portfolio strategy"""
        # Filter for recommended investments
        safe = [r for r in recommendations if r['category'] == "Safe Investment"][:3]
        balanced = [r for r in recommendations if r['category'] == "Balanced Growth"][:5]
        growth = [r for r in recommendations if r['category'] == "Growth Opportunity"][:3]
        
        strategy = {
            'total_investment': total_investment,
            'allocation': {
                'safe_investments': {
                    'percentage': 50,
                    'amount': total_investment * 0.5,
                    'assets': safe
                },
                'balanced_growth': {
                    'percentage': 35,
                    'amount': total_investment * 0.35,
                    'assets': balanced
                },
                'growth_opportunities': {
                    'percentage': 15,
                    'amount': total_investment * 0.15,
                    'assets': growth
                }
            },
            'risk_level': 'Moderate',
            'expected_return': '8-15% annually',
            'rebalancing_frequency': 'Quarterly'
        }
        
        return strategy

if __name__ == "__main__":
    recommender = InvestmentRecommender()
    recommendations = recommender.get_portfolio_recommendations('balanced')
    
    print("\n" + "="*80)
    print(" 💼 SMART INVESTMENT RECOMMENDATIONS")
    print("="*80)
    
    print("\n🏆 TOP 10 RECOMMENDED INVESTMENTS:\n")
    for i, rec in enumerate(recommendations[:10], 1):
        print(f"{i}. {rec['symbol']} ({rec['name']})")
        print(f"   Category: {rec['category']}")
        print(f"   Investment Score: {rec['scores']['investment_score']:.1f}/100")
        print(f"   Stability: {rec['scores']['stability']:.1f} | Growth: {rec['scores']['growth_potential']:.1f} | Risk: {rec['scores']['liquidity_risk']:.1f}")
        print(f"   Allocation: {rec['recommended_allocation']}")
        print()
