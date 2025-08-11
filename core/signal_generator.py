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
from sklearn.linear_model import LinearRegression
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
                print("⚠️  Không có top pairs trong database")
                return []
            
    except Exception as e:
        print(f"❌ Error getting top pairs from DB: {e}")
        return []

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
        
        # Chuyển đổi timestamp và đảm bảo timezone consistency
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Thêm cột open_time để tương thích với code cũ
        df['open_time'] = df['timestamp']
        
        return df
        
    except Exception as e:
        print(f"❌ Error getting klines data for {symbol}: {e}")
        return None

def calculate_volatility_ratio(df1, df2, window=20):
    """Tính tỷ lệ biến động giữa 2 coins sử dụng log-returns để công bằng"""
    try:
        # Tính volatility cho từng coin (rolling standard deviation của log-returns)
        log_returns1 = np.log(df1['close']).diff()
        log_returns2 = np.log(df2['close']).diff()
        
        vol1 = log_returns1.rolling(window=window).std(ddof=1)
        vol2 = log_returns2.rolling(window=window).std(ddof=1)
        
        # Lấy giá trị hiện tại
        current_vol1 = vol1.iloc[-1]
        current_vol2 = vol2.iloc[-1]
        
        # Tính tỷ lệ biến động với xử lý NaN và zero
        vol_ratio = 1.0
        if pd.notna(current_vol1) and pd.notna(current_vol2) and current_vol2 != 0:
            vol_ratio = current_vol1 / current_vol2
        
        return vol_ratio, current_vol1, current_vol2
        
    except Exception as e:
        print(f"❌ Error calculating volatility ratio: {e}")
        return 1.0, 0.0, 0.0

def calculate_pair_z_score(pair1, pair2, window=60, timeframe="1h"):
    """
    Tính z-score của spread chuẩn hóa giữa 2 tài sản với hedge ratio:
    - Align theo timestamp
    - Ước lượng hedge ratio beta (và alpha) bằng OLS
    - Rolling mean/std của spread
    - Xử lý NaN và division by zero
    """
    try:
        # Lấy nhiều dữ liệu hơn cho OLS estimation
        df1 = get_klines_data(pair1, interval=timeframe, limit=max(500, window+100))
        df2 = get_klines_data(pair2, interval=timeframe, limit=max(500, window+100))
        
        if df1 is None or df2 is None:
            return None, None, None, None, None, None, None
        
        # Chỉ giữ cột cần thiết, đổi tên cho dễ join
        a = df1[['timestamp','close']].rename(columns={'close': 'A'})
        b = df2[['timestamp','close']].rename(columns={'close': 'B'})
        
        # Align dữ liệu theo timestamp (inner join để tránh lệch nến)
        df = pd.merge(a, b, on='timestamp', how='inner').sort_values('timestamp').reset_index(drop=True)
        
        # Đảm bảo đủ dữ liệu
        if len(df) < max(window, 50):
            return None, None, None, None, None, None, None
        
        # Sử dụng log-price để giảm hiệu ứng scale
        df['logA'] = np.log(df['A'])
        df['logB'] = np.log(df['B'])
        
        # Ước lượng alpha, beta bằng OLS (trên toàn bộ mẫu gần đây)
        # beta = cov(B,A)/var(B); alpha = mean(A) - beta*mean(B)
        cov_matrix = np.cov(df['logB'], df['logA'], ddof=1)
        var_B = np.var(df['logB'], ddof=1)
        
        if var_B == 0 or np.isnan(var_B):
            return None, None, None, None, None, None, None
            
        beta = cov_matrix[0,1] / var_B
        alpha = df['logA'].mean() - beta * df['logB'].mean()
        
        # Residual (spread) = logA - (alpha + beta*logB)
        df['spread'] = df['logA'] - (alpha + beta * df['logB'])
        
        # Rolling stats trên spread
        roll_mean = df['spread'].rolling(window=window, min_periods=window).mean()
        roll_std = df['spread'].rolling(window=window, min_periods=window).std(ddof=1)
        
        # Tránh chia 0 và thay thế bằng NaN
        roll_std = roll_std.replace(0, np.nan)
        
        # Tính z-score
        df['zscore'] = (df['spread'] - roll_mean) / roll_std
        
        # Giá trị hiện tại (bỏ NaN đầu window)
        valid = df.dropna(subset=['zscore', 'spread'])
        if valid.empty:
            return None, None, None, None, None, None, None
        
        last = valid.iloc[-1]
        
        # Volatility info (trên log-returns để công bằng)
        df['rA'] = df['logA'].diff()
        df['rB'] = df['logB'].diff()
        volA = df['rA'].rolling(window=window).std(ddof=1).iloc[-1]
        volB = df['rB'].rolling(window=window).std(ddof=1).iloc[-1]
        
        vol_ratio = np.nan
        if pd.notna(volA) and pd.notna(volB) and volB != 0:
            vol_ratio = volA / volB
        
        return (
            float(last['zscore']),
            float(last['spread']), 
            float(roll_mean.iloc[-1]),
            float(roll_std.iloc[-1]),
            float(vol_ratio) if pd.notna(vol_ratio) else None,
            float(volA) if pd.notna(volA) else None,
            float(volB) if pd.notna(volB) else None
        )
        
    except Exception as e:
        print(f"❌ Error calculating improved pair z-score for {pair1}-{pair2}: {e}")
        return None, None, None, None, None, None, None


def calculate_pair_z_score_batch(pairs_batch, window=60, timeframe="1h"):
    """
    Simplified signal generation với 2 điều kiện:
    1. Z-score > 2.5 hoặc < -2.5
    2. Bollinger Bands breakout: dưới band → LONG, trên band → SHORT
    """
    results = []
    
    for pair in pairs_batch:
        pair1 = pair['pair1']
        pair2 = pair['pair2']
        
        # Lấy z-score với improved method
        z_score, spread, rolling_mean, rolling_std, vol_ratio, volA, volB = calculate_pair_z_score(pair1, pair2, window, timeframe)
        
        # Điều kiện 1: Z-score threshold
        if z_score is None or abs(z_score) < 2.5:
            continue
            
        try:
            # Chọn coin có momentum mạnh hơn để trade
            df1 = get_klines_data(pair1, interval=timeframe, limit=max(500, window+100))
            df2 = get_klines_data(pair2, interval=timeframe, limit=max(500, window+100))
            if df1 is None or df2 is None:
                continue
                
            momentum1 = (df1['close'].iloc[-1] - df1['close'].iloc[-10]) / df1['close'].iloc[-10]
            momentum2 = (df2['close'].iloc[-1] - df2['close'].iloc[-10]) / df2['close'].iloc[-10]
            
            if abs(momentum1) > abs(momentum2):
                selected_coin = pair1
                selected_df = df1
            else:
                selected_coin = pair2
                selected_df = df2

            # Điều kiện 2: Bollinger Bands breakout
            def calculate_bollinger_bands(prices, window=20, std_dev=2):
                sma = prices.rolling(window=window).mean()
                std = prices.rolling(window=window).std()
                upper_band = sma + (std * std_dev)
                lower_band = sma - (std * std_dev)
                return upper_band, sma, lower_band
            
            upper_band, lower_band = calculate_bollinger_bands(selected_df['close'])
            current_price = selected_df['close'].iloc[-1]
            
            signal_type = None
            signal_reason = ""
            
            # Logic quyết định signal
            if current_price < lower_band.iloc[-1]:
                signal_type = "BUY"  # Long khi vượt qua biên dưới
                signal_reason = f"BB_BREAKOUT_DOWN (Price: {current_price:.4f} < Lower: {lower_band.iloc[-1]:.4f})"
                print(f"🟢 {pair1}-{pair2}: Z-score {z_score:.3f} + Bollinger breakout DOWN → LONG {selected_coin}")
                
            elif current_price > upper_band.iloc[-1]:
                signal_type = "SELL"  # Short khi vượt qua biên trên
                signal_reason = f"BB_BREAKOUT_UP (Price: {current_price:.4f} > Upper: {upper_band.iloc[-1]:.4f})"
                print(f"🔴 {pair1}-{pair2}: Z-score {z_score:.3f} + Bollinger breakout UP → SHORT {selected_coin}")
            else:
                print(f"⚪ {pair1}-{pair2}: Z-score {z_score:.3f} nhưng giá trong Bollinger bands → Không trade")
                continue

            if signal_type is not None:
                selected_close = float(selected_df['close'].iloc[-1])
                
                # Tính TP/SL
                if signal_type == "BUY":
                    tp = round(selected_close * 1.02, 4)  # +2% TP
                    sl = round(selected_close * 0.98, 4)  # -2% SL
                    entry = round(selected_close, 4)
                else:  # SELL
                    tp = round(selected_close * 0.98, 4)  # -2% TP
                    sl = round(selected_close * 1.02, 4)  # +2% SL
                    entry = round(selected_close, 4)
                
                results.append({
                    'pair1': pair1,
                    'pair2': pair2,
                    'symbol': selected_coin,
                    'signal_type': signal_type,
                    'z_score': z_score,
                    'spread': spread,
                    'timestamp': datetime.now().isoformat(),
                    'tp': tp,
                    'sl': sl,
                    'entry': entry,
                    'confirmation_details': f"Z_SCORE_{z_score:.3f}; {signal_reason}"
                })
                
        except Exception as e:
            print(f"❌ Lỗi phân tích {pair1}-{pair2}: {e}")
            continue
            
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
        future_to_batch = {executor.submit(calculate_pair_z_score_batch, batch, 60, timeframe): batch for batch in batches}
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
