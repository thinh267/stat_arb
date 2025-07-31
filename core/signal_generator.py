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
    """Tính z-score với 4 lớp confirmation: RSI, MACD, Bollinger Bands, Linear Regression (track BUY/SELL confirmations separately)"""
    results = []
    
    for pair in pairs_batch:
        pair1 = pair['pair1']
        pair2 = pair['pair2']
        
        # Lấy z-score
        z_score, spread, mean, std, vol_ratio, vol1, vol2 = calculate_pair_z_score(pair1, pair2, window, timeframe)
        
        if z_score is None or abs(z_score) < 2.0:
            continue
            
        try:
            df1 = get_klines_data(pair1, interval=timeframe, limit=168)
            df2 = get_klines_data(pair2, interval=timeframe, limit=168)
            if df1 is None or df2 is None:
                continue
            momentum1 = (df1['close'].iloc[-1] - df1['close'].iloc[-5]) / df1['close'].iloc[-5]
            momentum2 = (df2['close'].iloc[-1] - df2['close'].iloc[-5]) / df2['close'].iloc[-5]
            if abs(momentum1) > abs(momentum2):
                selected_coin = pair1
                selected_df = df1
            else:
                selected_coin = pair2
                selected_df = df2

            # Track confirmations for BUY/SELL
            buy_confirms = 0
            sell_confirms = 0
            confirmation_details = []
            rsi_confirmed = False
            macd_confirmed = False
            bollinger_confirmed = False
            linear_confirmed = False

            # RSI Confirmation
            def calculate_rsi(prices, window=14):
                delta = prices.diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                return rsi
            rsi = calculate_rsi(selected_df['close'])
            price_current = selected_df['close'].iloc[-1]
            price_prev = selected_df['close'].iloc[-10]
            rsi_current = rsi.iloc[-1]
            rsi_prev = rsi.iloc[-10]
            if price_current < price_prev and rsi_current > rsi_prev:
                buy_confirms += 1
                rsi_confirmed = True
                confirmation_details.append("RSI_BULLISH_DIVERGENCE_BUY")
                print(f"✅ {pair1}-{pair2}: RSI bullish divergence (BUY)")
            elif price_current > price_prev and rsi_current < rsi_prev:
                sell_confirms += 1
                rsi_confirmed = True
                confirmation_details.append("RSI_BEARISH_DIVERGENCE_SELL")
                print(f"✅ {pair1}-{pair2}: RSI bearish divergence (SELL)")
            elif price_current > price_prev and rsi_current > rsi_prev:
                buy_confirms += 1
                rsi_confirmed = True
                confirmation_details.append("RSI_TREND_UP_BUY")
                print(f"✅ {pair1}-{pair2}: RSI trend UP (BUY)")
            elif price_current < price_prev and rsi_current < rsi_prev:
                sell_confirms += 1
                rsi_confirmed = True
                confirmation_details.append("RSI_TREND_DOWN_SELL")
                print(f"✅ {pair1}-{pair2}: RSI trend DOWN (SELL)")
            elif rsi_current < 30:
                buy_confirms += 1
                rsi_confirmed = True
                confirmation_details.append(f"RSI_OVERSOLD_{rsi_current:.1f}_BUY")
                print(f"✅ {pair1}-{pair2}: RSI oversold ({rsi_current:.1f}) (BUY)")
            elif rsi_current > 70:
                sell_confirms += 1
                rsi_confirmed = True
                confirmation_details.append(f"RSI_OVERBOUGHT_{rsi_current:.1f}_SELL")
                print(f"✅ {pair1}-{pair2}: RSI overbought ({rsi_current:.1f}) (SELL)")

            # MACD Confirmation
            def calculate_macd(prices, fast=12, slow=26, signal=9):
                ema_fast = prices.ewm(span=fast).mean()
                ema_slow = prices.ewm(span=slow).mean()
                macd_line = ema_fast - ema_slow
                signal_line = macd_line.ewm(span=signal).mean()
                return macd_line, signal_line
            macd_line, signal_line = calculate_macd(selected_df['close'])
            
            if macd_line.iloc[-1] > signal_line.iloc[-1] and macd_line.iloc[-2] <= signal_line.iloc[-2]:
                buy_confirms += 1
                macd_confirmed = True
                confirmation_details.append("MACD_BULLISH_CROSSOVER_BUY")
                print(f"✅ {pair1}-{pair2}: MACD bullish crossover (BUY)")
            elif macd_line.iloc[-1] < signal_line.iloc[-1] and macd_line.iloc[-2] >= signal_line.iloc[-2]:
                sell_confirms += 1
                macd_confirmed = True
                confirmation_details.append("MACD_BEARISH_CROSSOVER_SELL")
                print(f"✅ {pair1}-{pair2}: MACD bearish crossover (SELL)")
            elif macd_line.iloc[-1] > signal_line.iloc[-1]:
                buy_confirms += 1
                macd_confirmed = True
                confirmation_details.append("MACD_BULLISH_MOMENTUM_BUY")
                print(f"✅ {pair1}-{pair2}: MACD bullish momentum (BUY)")
            elif macd_line.iloc[-1] < signal_line.iloc[-1]:
                sell_confirms += 1
                macd_confirmed = True
                confirmation_details.append("MACD_BEARISH_MOMENTUM_SELL")
                print(f"✅ {pair1}-{pair2}: MACD bearish momentum (SELL)")

            # Bollinger Bands Confirmation
            def calculate_bollinger_bands(prices, window=20, std_dev=2):
                sma = prices.rolling(window=window).mean()
                std = prices.rolling(window=window).std()
                upper_band = sma + (std * std_dev)
                lower_band = sma - (std * std_dev)
                return upper_band, sma, lower_band
            upper_band, middle_band, lower_band = calculate_bollinger_bands(selected_df['close'])
            current_price = selected_df['close'].iloc[-1]
            if current_price > upper_band.iloc[-1]:
                buy_confirms += 1
                bollinger_confirmed = True
                confirmation_details.append("BOLLINGER_BREAKOUT_UP_BUY")
                print(f"✅ {pair1}-{pair2}: Bollinger breakout UP (BUY)")
            elif current_price < lower_band.iloc[-1]:
                sell_confirms += 1
                bollinger_confirmed = True
                confirmation_details.append("BOLLINGER_BREAKOUT_DOWN_SELL")
                print(f"✅ {pair1}-{pair2}: Bollinger breakout DOWN (SELL)")
            elif current_price > middle_band.iloc[-1]:
                buy_confirms += 1
                bollinger_confirmed = True
                confirmation_details.append("BOLLINGER_ABOVE_MIDDLE_BUY")
                print(f"✅ {pair1}-{pair2}: Bollinger above middle band (BUY)")
            elif current_price < middle_band.iloc[-1]:
                sell_confirms += 1
                bollinger_confirmed = True
                confirmation_details.append("BOLLINGER_BELOW_MIDDLE_SELL")
                print(f"✅ {pair1}-{pair2}: Bollinger below middle band (SELL)")

            # Linear Regression Confirmation (24 nến)
            def calculate_linear_trend_with_distance(prices, window=24):
                if len(prices) < window:
                    return None, None, None
                recent_prices = prices.tail(window).values
                X = np.arange(len(recent_prices)).reshape(-1, 1)
                y = recent_prices.reshape(-1, 1)
                model = LinearRegression().fit(X, y)
                slope = model.coef_[0][0]
                
                # Tính khoảng cách từ giá hiện tại đến đường trendline
                current_price = recent_prices[-1]
                predicted_price = model.predict([[window-1]])[0][0]
                distance = abs(current_price - predicted_price)
                
                # Tính độ lệch chuẩn của residuals để so sánh
                residuals = recent_prices.flatten() - model.predict(X).flatten()
                std_residuals = np.std(residuals)
                
                return slope, distance, std_residuals
            
            linear_result = calculate_linear_trend_with_distance(selected_df['close'])
            if linear_result is not None:
                slope, distance, std_residuals = linear_result
                threshold = 0.001
                distance_threshold = 1.5 * std_residuals  # Khoảng cách tối đa cho phép
                
                if slope > threshold and distance <= distance_threshold:
                    buy_confirms += 1
                    linear_confirmed = True
                    confirmation_details.append(f"LINEAR_TREND_UP_{slope:.6f}_BUY")
                    print(f"✅ {pair1}-{pair2}: Linear trend UP (BUY, slope: {slope:.6f}, distance: {distance:.4f})")
                elif slope < -threshold and distance <= distance_threshold:
                    sell_confirms += 1
                    linear_confirmed = True
                    confirmation_details.append(f"LINEAR_TREND_DOWN_{slope:.6f}_SELL")
                    print(f"✅ {pair1}-{pair2}: Linear trend DOWN (SELL, slope: {slope:.6f}, distance: {distance:.4f})")
                elif distance > distance_threshold:
                    print(f"⚠️ {pair1}-{pair2}: Giá hiện tại cách trendline quá xa (distance: {distance:.4f} > {distance_threshold:.4f})")

            total_confirms = buy_confirms + sell_confirms
            signal_type = None
            if buy_confirms >= 3:
                signal_type = "BUY"
            elif sell_confirms >= 3:
                signal_type = "SELL"
            else:
                signal_type = None  # Không đủ 3 confirmations cho cùng 1 hướng

            if signal_type is not None:
                selected_close = float(selected_df['close'].iloc[-1])
                if signal_type == "BUY":
                    tp = round(selected_close * 1.02, 4)
                    sl = round(selected_close * 0.98, 4)
                    entry = round(selected_close, 4)
                    print(f" {pair1}-{pair2}: {total_confirms}/4 confirmations → BUY {selected_coin}")
                else:  # SELL
                    tp = round(selected_close * 0.98, 4)
                    sl = round(selected_close * 1.02, 4)
                    entry = round(selected_close, 4)
                    print(f" {pair1}-{pair2}: {total_confirms}/4 confirmations → SELL {selected_coin}")
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
                    'confirmations': total_confirms,
                    'rsi_confirmation': rsi_confirmed,
                    'macd_confirmation': macd_confirmed,
                    'bollinger_confirmation': bollinger_confirmed,
                    'linear_confirmation': linear_confirmed,
                    'confirmation_details': '; '.join(confirmation_details)
                })
            else:
                print(f"⚪ {pair1}-{pair2}: Không đủ xác nhận hoặc BUY/SELL bằng nhau → Không trade")
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
