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

def get_klines_data(symbol, interval="1h", limit=168):
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

def calculate_pair_z_score(pair1, pair2, window=20, timeframe="1h"):
    """Tính z-score cho một cặp pairs"""
    try:
        # Lấy dữ liệu cho cả hai pairs từ Binance API
        df1 = get_klines_data(pair1, interval=timeframe, limit=168)
        df2 = get_klines_data(pair2, interval=timeframe, limit=168)
        
        if df1 is None or df2 is None or len(df1) < window or len(df2) < window:
            return None, None, None, None
        
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
        
        return current_z_score, current_spread, current_mean, current_std
        
    except Exception as e:
        print(f"❌ Error calculating pair z-score for {pair1}-{pair2}: {e}")
        return None, None, None, None

def calculate_pair_z_score_batch(pairs_batch, window=20, timeframe="1h"):
    """Tính z-score cho một batch pairs, luôn print kết quả, chỉ lưu BUY/SELL, không lưu NEUTRAL"""
    results = []
    for pair in pairs_batch:
        pair1 = pair['pair1']
        pair2 = pair['pair2']
        z_score, spread, mean, std = calculate_pair_z_score(pair1, pair2, window, timeframe)
        if z_score is not None and not np.isnan(z_score):
            if z_score > 2.0:
                print(f"{pair1}-{pair2}: z_score={z_score:.3f}, spread={spread:.3f}, {pair1}=SELL, {pair2}=BUY")
                # SELL pair1, BUY pair2
                results.append({
                    'pair1': pair1,
                    'pair2': pair2,
                    'symbol': pair1,
                    'signal_type': 'SELL',
                    'z_score': z_score,
                    'spread': spread,
                    'timestamp': datetime.now().isoformat()
                })
                results.append({
                    'pair1': pair1,
                    'pair2': pair2,
                    'symbol': pair2,
                    'signal_type': 'BUY',
                    'z_score': z_score,
                    'spread': spread,
                    'timestamp': datetime.now().isoformat()
                })
            elif z_score < -2.0:
                print(f"{pair1}-{pair2}: z_score={z_score:.3f}, spread={spread:.3f}, {pair1}=BUY, {pair2}=SELL")
                # BUY pair1, SELL pair2
                results.append({
                    'pair1': pair1,
                    'pair2': pair2,
                    'symbol': pair1,
                    'signal_type': 'BUY',
                    'z_score': z_score,
                    'spread': spread,
                    'timestamp': datetime.now().isoformat()
                })
                results.append({
                    'pair1': pair1,
                    'pair2': pair2,
                    'symbol': pair2,
                    'signal_type': 'SELL',
                    'z_score': z_score,
                    'spread': spread,
                    'timestamp': datetime.now().isoformat()
                })
            else:
                print(f"{pair1}-{pair2}: z_score={z_score:.3f}, spread={spread:.3f}, NEUTRAL")
                # Không lưu NEUTRAL
    return results

def generate_signals_for_top_pairs(timeframe="1h"):
    """Tạo signals cho top 10 pairs từ database với timeframe tuỳ chọn"""
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
    
    # Tạo DataFrame và phân tích
    signals_df = pd.DataFrame(all_signals)
    
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
    
    # Tạo signals cho top pairs
    signals = generate_signals_for_top_pairs()
    
    if not signals:
        print("❌ Không tạo được signals")
        return []
    
    # Lưu signals vào database
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

if __name__ == "__main__":
    main() 