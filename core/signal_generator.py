# signal_generator.py
from binance.client import Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from functools import lru_cache
from config import BINANCE_API_KEY, BINANCE_API_SECRET, DAILY_TOP_N
from core.supabase_manager import SupabaseManager
from statsmodels.tsa.stattools import coint
import warnings
warnings.filterwarnings('ignore')

# Khởi tạo Binance client với retry mechanism
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=False)
client.timeout = 30
supabase_manager = SupabaseManager()



def get_top_pairs_from_db():
    """Lấy top 10 pairs từ hourly_rankings (ranking mới nhất)"""
    try:
        # Lấy top pairs từ hourly_rankings (ranking mới nhất)
        hourly_rankings = supabase_manager.get_hourly_rankings()
        
        if hourly_rankings and len(hourly_rankings) > 0:
            # Sắp xếp theo current_rank và lấy top 10
            sorted_rankings = sorted(hourly_rankings, key=lambda x: x.get('current_rank', 999))
            top_10_rankings = sorted_rankings[:10]
            
            # Chuyển đổi thành format pairs
            top_pairs = []
            for ranking in top_10_rankings:
                pair_id = ranking.get('pair_id')
                if pair_id:
                    # Lấy thông tin pair từ pair_id
                    pair_info = supabase_manager.get_pair_by_id(pair_id)
                    if pair_info:
                        top_pairs.append({
                            'pair1': pair_info['pair1'],
                            'pair2': pair_info['pair2'],
                            'rank': ranking.get('current_rank'),
                            'correlation': ranking.get('current_correlation'),
                            'pair_id': pair_id
                        })
            
            print(f"📊 Lấy được {len(top_pairs)} top pairs từ hourly_rankings")
            return top_pairs
        else:
            print("⚠️  Không có hourly_rankings, thử lấy từ daily_pairs...")
            # Fallback: lấy từ daily_pairs
            top_pairs = supabase_manager.get_top_pairs()
            if top_pairs and len(top_pairs) > 0:
                print(f"📊 Lấy được {len(top_pairs)} top pairs từ daily_pairs")
                return top_pairs[:10]
            else:
                print("⚠️  Không có top pairs trong database, sử dụng fallback")
                return get_fallback_pairs()
            
    except Exception as e:
        print(f"❌ Error getting top pairs from DB: {e}")
        return get_fallback_pairs()

def get_klines_data(symbol, interval="15m", limit=168):
    """Lấy dữ liệu klines từ Binance API - sử dụng futures API"""
    try:
        # Sử dụng futures API thay vì spot API
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        
        if not klines:
            return None
        
        # Chuyển đổi thành DataFrame
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # Chuyển đổi kiểu dữ liệu
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Chuyển đổi timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        return df
        
    except Exception as e:
        print(f"❌ Error getting klines data for {symbol}: {e}")
        return None

def calculate_volatility_ratio(df1, df2, window=20):
    """Tính tỷ lệ biến động giữa 2 coins để chọn coin biến động mạnh hơn"""
    try:
        # Tính volatility cho từng coin (rolling standard deviation của returns)
        returns1 = df1['close'].pct_change()
        returns2 = df2['close'].pct_change()
        
        vol1 = returns1.rolling(window=window).std()
        vol2 = returns2.rolling(window=window).std()
        
        # Lấy giá trị hiện tại
        current_vol1 = vol1.iloc[-1]
        current_vol2 = vol2.iloc[-1]
        
        # Tính tỷ lệ biến động
        vol_ratio = current_vol1 / current_vol2 if current_vol2 != 0 else 1.0
        
        return vol_ratio, current_vol1, current_vol2
        
    except Exception as e:
        print(f"❌ Error calculating volatility ratio: {e}")
        return 1.0, 0.0, 0.0

def predict_market_trend(pair1, pair2, timeframe="1h"):
    """Dự đoán xu hướng thị trường dựa trên momentum và volume"""
    try:
        # Lấy dữ liệu cho cả hai pairs
        df1 = get_klines_data(pair1, interval=timeframe, limit=168)
        df2 = get_klines_data(pair2, interval=timeframe, limit=168)
        if df1 is None or df2 is None or len(df1) < 20 or len(df2) < 20:
            return None, None, None
        # Tính momentum cho từng coin (tỷ lệ thay đổi giá)
        momentum1 = (df1['close'].iloc[-1] - df1['close'].iloc[-5]) / df1['close'].iloc[-5]
        momentum2 = (df2['close'].iloc[-1] - df2['close'].iloc[-5]) / df2['close'].iloc[-5]
        # Tính volume ratio (coin nào có volume cao hơn)
        avg_volume1 = df1['volume'].tail(10).mean()
        avg_volume2 = df2['volume'].tail(10).mean()
        volume_ratio = avg_volume1 / avg_volume2 if avg_volume2 > 0 else 1.0
        # Tính RSI để xác định xu hướng
        def calculate_rsi(prices, window=14):
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
        rsi1 = calculate_rsi(df1['close']).iloc[-1]
        rsi2 = calculate_rsi(df2['close']).iloc[-1]
        # Dự đoán xu hướng:
        if momentum1 > 0.01 and momentum2 > 0.01:
            trend = "UP"
            trend_strength = min(momentum1, momentum2)
        elif momentum1 < -0.01 and momentum2 < -0.01:
            trend = "DOWN"
            trend_strength = max(abs(momentum1), abs(momentum2))
        else:
            if volume_ratio > 1.2:
                trend = "UP" if momentum1 > 0 else "DOWN"
                trend_strength = abs(momentum1)
            else:
                trend = "UP" if momentum2 > 0 else "DOWN"
                trend_strength = abs(momentum2)
        return trend, trend_strength, volume_ratio
    except Exception as e:
        print(f"❌ Error predicting market trend: {e}")
        return None, None, None

def calculate_pair_z_score(pair1, pair2, window=20, timeframe="1h"):
    """Tính z-score cho một cặp pairs và trả về thêm volatility info"""
    try:
        # Lấy dữ liệu cho cả hai pairs từ Binance API
        df1 = get_klines_data(pair1, interval=timeframe, limit=168)
        df2 = get_klines_data(pair2, interval=timeframe, limit=168)
        
        if df1 is None or df2 is None or len(df1) < window or len(df2) < window:
            return None, None, None, None, None, None, None
        
        # Tính spread giữa hai pairs
        spread = df1['close'] - df2['close']
        
        # Tính rolling mean và std của spread
        rolling_mean = spread.rolling(window=window).mean()
        rolling_std = spread.rolling(window=window).std()
        
        # Tính z-score của spread
        z_score = (spread - rolling_mean) / rolling_std
        
        # Lấy giá trị hiện tại
        current_spread = spread.iloc[-1]
        current_z_score = z_score.iloc[-1]
        current_mean = rolling_mean.iloc[-1]
        current_std = rolling_std.iloc[-1]
        
        # Tính volatility ratio để chọn coin biến động mạnh hơn
        vol_ratio, vol1, vol2 = calculate_volatility_ratio(df1, df2, window)
        
        return current_z_score, current_spread, current_mean, current_std, vol_ratio, vol1, vol2
        
    except Exception as e:
        print(f"❌ Error calculating pair z-score for {pair1}-{pair2}: {e}")
        return None, None, None, None, None, None, None

def calculate_pair_z_score_batch(pairs_batch, window=20, timeframe="1h"):
    """Tính z-score cho một batch pairs, chỉ lưu signal cho 1 symbol duy nhất trong mỗi pair theo xu hướng thị trường, kèm TP/SL/Entry."""
    results = []
    for pair in pairs_batch:
        pair1 = pair['pair1']
        pair2 = pair['pair2']
        z_score, spread, mean, std, vol_ratio, vol1, vol2 = calculate_pair_z_score(pair1, pair2, window, timeframe)
        if z_score is not None and not np.isnan(z_score):
            current_timestamp = datetime.now().isoformat()
            market_trend, trend_strength, volume_ratio = predict_market_trend(pair1, pair2, timeframe)
            if market_trend is None:
                print(f"⚠️ {pair1}-{pair2}: Không thể dự đoán xu hướng thị trường")
                continue
            close1 = None
            close2 = None
            try:
                df1 = get_klines_data(pair1, interval=timeframe, limit=1)
                if df1 is not None and len(df1) > 0:
                    close1 = float(df1['close'].iloc[-1])
                df2 = get_klines_data(pair2, interval=timeframe, limit=1)
                if df2 is not None and len(df2) > 0:
                    close2 = float(df2['close'].iloc[-1])
            except Exception as e:
                print(f"Lỗi lấy giá close mới nhất: {e}")
            if z_score > 2.0:
                if market_trend == "UP" and close1:
                    tp = round(close1 * 1.01, 4)
                    sl = round(close1 * 0.99, 4)
                    entry = round(close1, 4)
                    print(f"📈 {pair1}-{pair2}: z_score={z_score:.3f}, trend=UP → BUY {pair1} TP={tp} SL={sl} ENTRY={entry}")
                    results.append({
                        'pair1': pair1,
                        'pair2': pair2,
                        'symbol': pair1,
                        'signal_type': 'BUY',
                        'z_score': z_score,
                        'spread': spread,
                        'market_trend': market_trend,
                        'trend_strength': trend_strength,
                        'timestamp': current_timestamp,
                        'tp': tp,
                        'sl': sl,
                        'entry': entry
                    })
                elif market_trend == "DOWN" and close2:
                    tp = round(close2 * 0.99, 4)
                    sl = round(close2 * 1.01, 4)
                    entry = round(close2, 4)
                    print(f"📉 {pair1}-{pair2}: z_score={z_score:.3f}, trend=DOWN → SELL {pair2} TP={tp} SL={sl} ENTRY={entry}")
                    results.append({
                        'pair1': pair1,
                        'pair2': pair2,
                        'symbol': pair2,
                        'signal_type': 'SELL',
                        'z_score': z_score,
                        'spread': spread,
                        'market_trend': market_trend,
                        'trend_strength': trend_strength,
                        'timestamp': current_timestamp,
                        'tp': tp,
                        'sl': sl,
                        'entry': entry
                    })
            elif z_score < -2.0:
                if market_trend == "UP" and close2:
                    tp = round(close2 * 1.01, 4)
                    sl = round(close2 * 0.99, 4)
                    entry = round(close2, 4)
                    print(f"📈 {pair1}-{pair2}: z_score={z_score:.3f}, trend=UP → BUY {pair2} TP={tp} SL={sl} ENTRY={entry}")
                    results.append({
                        'pair1': pair1,
                        'pair2': pair2,
                        'symbol': pair2,
                        'signal_type': 'BUY',
                        'z_score': z_score,
                        'spread': spread,
                        'market_trend': market_trend,
                        'trend_strength': trend_strength,
                        'timestamp': current_timestamp,
                        'tp': tp,
                        'sl': sl,
                        'entry': entry
                    })
                elif market_trend == "DOWN" and close1:
                    tp = round(close1 * 0.99, 4)
                    sl = round(close1 * 1.01, 4)
                    entry = round(close1, 4)
                    print(f"📉 {pair1}-{pair2}: z_score={z_score:.3f}, trend=DOWN → SELL {pair1} TP={tp} SL={sl} ENTRY={entry}")
                    results.append({
                        'pair1': pair1,
                        'pair2': pair2,
                        'symbol': pair1,
                        'signal_type': 'SELL',
                        'z_score': z_score,
                        'spread': spread,
                        'market_trend': market_trend,
                        'trend_strength': trend_strength,
                        'timestamp': current_timestamp,
                        'tp': tp,
                        'sl': sl,
                        'entry': entry
                    })
    return results

def generate_signals_for_top_pairs(timeframe="1h"):
    """Tạo signals cho top 10 pairs từ database với timeframe tuỳ chọn, lọc trùng symbol."""
    print(f"🚀 GENERATING SIGNALS FOR TOP 10 PAIRS (timeframe={timeframe})")
    print("=" * 60)
    # Lấy top 10 pairs từ database
    top_pairs = get_top_pairs_from_db()
    if not top_pairs:
        print("❌ Không có top pairs để tạo signals")
        return []
    print(f"📊 Đang tạo signals cho {len(top_pairs)} top pairs...")
    # Chia pairs thành batches cho parallel processing
    batch_size = max(1, len(top_pairs) // 4)  # 4 workers
    batches = [top_pairs[i:i + batch_size] for i in range(0, len(top_pairs), batch_size)]
    # Parallel processing
    all_signals = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_batch = {executor.submit(calculate_pair_z_score_batch, batch, 20, timeframe): batch for batch in batches}
        completed = 0
        for future in as_completed(future_to_batch):
            batch_results = future.result()
            all_signals.extend(batch_results)
            completed += 1
            print(f"📊 Hoàn thành batch {completed}/{len(batches)} ({len(all_signals)} signals)")
    if not all_signals:
        print("❌ Không tạo được signals")
        return []
    # Lọc trùng: chỉ giữ signal có |z_score| lớn nhất cho mỗi symbol/signal_type
    signals_df = pd.DataFrame(all_signals)
    signals_df['abs_z'] = signals_df['z_score'].abs()
    signals_df = signals_df.sort_values('abs_z', ascending=False)
    signals_df = signals_df.drop_duplicates(subset=['symbol', 'signal_type'], keep='first')
    signals_df = signals_df.drop(columns=['abs_z'])
    print(f"\n📊 KẾT QUẢ SIGNAL GENERATION:")
    print(f"- Tổng signals: {len(signals_df)}")
    print(f"- Buy: {len(signals_df[signals_df['signal_type'] == 'BUY'])}")
    print(f"- Sell: {len(signals_df[signals_df['signal_type'] == 'SELL'])}")
    # Hiển thị top signals
    print(f"\n🏆 TOP SIGNALS:")
    print("=" * 100)
    print(f"{'Rank':<5} {'Symbol':<12} {'Z-Score':<10} {'Signal':<8} {'Spread':<12}")
    print("-" * 100)
    top_signals = signals_df.nlargest(10, 'z_score')
    for i, row in top_signals.iterrows():
        rank = i + 1
        symbol = row['symbol']
        z_score = f"{row['z_score']:.3f}"
        signal = row['signal_type']
        spread = f"{row['spread']:.2f}"
        print(f"{rank:<5} {symbol:<12} {z_score:<10} {signal:<8} {spread:<12}")
    return signals_df.to_dict('records')

def generate_and_save_signals():
    """Tạo và lưu signals cho pairs"""
    print("🚀 SIGNAL GENERATOR - TOP 10 PAIRS")
    print("=" * 60)
    
    # Bỏ logic check thời gian để tránh bỏ lỡ signals quan trọng
    # Chỉ dựa vào database check để filter trùng lặp
    
    # Tạo signals cho top pairs với timeframe 1h
    signals = generate_signals_for_top_pairs(timeframe="1h")
    
    if not signals:
        print("❌ Không tạo được signals")
        return []
    
    # Lưu signals vào database
    print(f"[DEBUG] Đang lưu {len(signals)} signals vào database...")
    success = supabase_manager.save_pair_signals(signals)
    
    if success:
        print("✅ Signal generation hoàn thành!")
        return signals  # Trả về list signals thay vì bool
    else:
        print("❌ Lỗi khi lưu signals")
        return []


def main():
    """Main function"""
    print("🚀 PAIR SIGNAL GENERATOR")
    print("=" * 60)
    
    # Tạo và lưu signals trực tiếp
    success = generate_and_save_signals()
    
    if success:
        print("🎉 Pair signal generation hoàn thành!")
    else:
        print("❌ Có lỗi trong pair signal generation")
