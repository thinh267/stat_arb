#!/usr/bin/env python3
"""
Test file for 4 confirmation layers: RSI, MACD, Bollinger Bands, Linear Regression
"""

from core.signal_generator import calculate_pair_z_score_batch, get_top_pairs_from_db
from core.supabase_manager import SupabaseManager
import pandas as pd
from datetime import datetime

def test_4_confirmation_layers():
    """Test the new 4 confirmation layers system with detailed debug"""
    print("=== TEST 4 CONFIRMATION LAYERS WITH DETAILED DEBUG ===")
    
    # Lấy top pairs từ database
    top_pairs = get_top_pairs_from_db()
    if not top_pairs:
        print("❌ Không có top pairs để test")
        return
    
    print(f"📊 Testing với {len(top_pairs)} top pairs")
    
    # Test với 2 pairs đầu tiên
    test_pairs = top_pairs[:2]
    
    # Generate signals với 4 lớp confirmation
    signals = calculate_pair_z_score_batch(test_pairs, window=20, timeframe="1h")
    
    print(f"\n📈 Generated {len(signals)} signals")
    
    if signals:
        print("\n=== SIGNAL DETAILS ===")
        for i, signal in enumerate(signals, 1):
            print(f"\n🔍 SIGNAL {i} ANALYSIS:")
            print(f"  Pair: {signal['pair1']}-{signal['pair2']}")
            print(f"  Symbol: {signal['symbol']}")
            print(f"  Signal Type: {signal['signal_type']}")
            print(f"  Z-Score: {signal['z_score']:.4f}")
            print(f"  Entry: {signal['entry']}")
            print(f"  TP: {signal['tp']}")
            print(f"  SL: {signal['sl']}")
            print(f"  Total Confirmations: {signal['confirmations']}/4")
            print(f"  RSI Confirmation: {signal['rsi_confirmation']}")
            print(f"  MACD Confirmation: {signal['macd_confirmation']}")
            print(f"  Bollinger Confirmation: {signal['bollinger_confirmation']}")
            print(f"  Linear Confirmation: {signal['linear_confirmation']}")
            print(f"  Details: {signal['confirmation_details']}")
            # Debug breakdown từng lớp xác nhận
            if signal['confirmation_details']:
                print("  Confirmation breakdown:")
                for detail in signal['confirmation_details'].split(';'):
                    detail = detail.strip()
                    if not detail:
                        continue
                    if '_BUY' in detail:
                        print(f"    [CONFIRM] {detail.replace('_BUY','')}: +1 BUY")
                    elif '_SELL' in detail:
                        print(f"    [CONFIRM] {detail.replace('_SELL','')}: +1 SELL")
                    else:
                        print(f"    [CONFIRM] {detail}")
            # Thêm debug kết luận tín hiệu
            if signal['signal_type'] == 'BUY':
                print(f"  👉 FINAL DECISION: BUY (Đủ xác nhận cho BUY)")
            elif signal['signal_type'] == 'SELL':
                print(f"  👉 FINAL DECISION: SELL (Đủ xác nhận cho SELL)")
            else:
                print(f"  👉 FINAL DECISION: NO TRADE (Không đủ xác nhận hoặc xác nhận bị chia đều)")
        
        # Test save to database
        print("\n=== TEST SAVE TO DATABASE ===")
        supabase_manager = SupabaseManager()
        success = supabase_manager.save_pair_signals(signals)
        
        if success:
            print("✅ Đã lưu signals lên database thành công!")
        else:
            print("❌ Lỗi khi lưu signals lên database!")
    else:
        print("❌ Không có signals nào được generate!")

def test_confirmation_breakdown():
    """Test phân tích chi tiết từng confirmation layer"""
    print("\n=== TEST CONFIRMATION BREAKDOWN ===")
    
    # Lấy top pairs
    top_pairs = get_top_pairs_from_db()
    if not top_pairs:
        return
    
    # Test với 1 pair
    test_pair = top_pairs[0]
    print(f"Testing pair: {test_pair['pair1']}-{test_pair['pair2']}")
    
    # Generate signal
    signals = calculate_pair_z_score_batch([test_pair], window=20, timeframe="1h")
    
    if signals:
        signal = signals[0]
        print(f"\n🔍 CONFIRMATION BREAKDOWN:")
        print(f"  RSI: {'✅' if signal['rsi_confirmation'] else '❌'}")
        print(f"  MACD: {'✅' if signal['macd_confirmation'] else '❌'}")
        print(f"  Bollinger: {'✅' if signal['bollinger_confirmation'] else '❌'}")
        print(f"  Linear: {'✅' if signal['linear_confirmation'] else '❌'}")
        print(f"  Total: {signal['confirmations']}/4")
        print(f"  Details: {signal['confirmation_details']}")
        
        # Debug breakdown từng lớp xác nhận
        if signal['confirmation_details']:
            print("  Confirmation breakdown:")
            for detail in signal['confirmation_details'].split(';'):
                detail = detail.strip()
                if not detail:
                    continue
                if '_BUY' in detail:
                    print(f"    [CONFIRM] {detail.replace('_BUY','')}: +1 BUY")
                elif '_SELL' in detail:
                    print(f"    [CONFIRM] {detail.replace('_SELL','')}: +1 SELL")
                else:
                    print(f"    [CONFIRM] {detail}")
        
        # Thêm debug kết luận tín hiệu
        if signal['signal_type'] == 'BUY':
            print(f"  👉 FINAL DECISION: BUY (Đủ xác nhận cho BUY)")
        elif signal['signal_type'] == 'SELL':
            print(f"  👉 FINAL DECISION: SELL (Đủ xác nhận cho SELL)")
        else:
            print(f"  👉 FINAL DECISION: NO TRADE (Không đủ xác nhận hoặc xác nhận bị chia đều)")
    else:
        print("❌ Không có signal nào được generate!")

def test_debug_individual_layers():
    """Test debug chi tiết từng layer riêng biệt"""
    print("\n=== TEST DEBUG INDIVIDUAL LAYERS ===")
    
    # Lấy top pairs
    top_pairs = get_top_pairs_from_db()
    if not top_pairs:
        return
    
    # Test với 1 pair
    test_pair = top_pairs[0]
    print(f"Testing pair: {test_pair['pair1']}-{test_pair['pair2']}")
    
    # Import các hàm cần thiết để debug
    from core.signal_generator import get_klines_data
    import numpy as np
    from sklearn.linear_model import LinearRegression
    
    try:
        # Lấy dữ liệu
        df1 = get_klines_data(test_pair['pair1'], interval="1h", limit=168)
        df2 = get_klines_data(test_pair['pair2'], interval="1h", limit=168)
        
        if df1 is None or df2 is None:
            print("❌ Không lấy được dữ liệu")
            return
        
        # Chọn coin có momentum mạnh hơn
        momentum1 = (df1['close'].iloc[-1] - df1['close'].iloc[-5]) / df1['close'].iloc[-5]
        momentum2 = (df2['close'].iloc[-1] - df2['close'].iloc[-5]) / df2['close'].iloc[-5]
        
        if abs(momentum1) > abs(momentum2):
            selected_coin = test_pair['pair1']
            selected_df = df1
        else:
            selected_coin = test_pair['pair2']
            selected_df = df2
        
        print(f"Selected coin: {selected_coin}")
        print(f"Momentum: {momentum1:.4f} vs {momentum2:.4f}")
        
        # Debug RSI
        print(f"\n🔍 RSI DEBUG:")
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
        
        print(f"  Current price: {price_current:.4f}")
        print(f"  Previous price: {price_prev:.4f}")
        print(f"  Current RSI: {rsi_current:.2f}")
        print(f"  Previous RSI: {rsi_prev:.2f}")
        print(f"  Price change: {price_current - price_prev:.4f}")
        print(f"  RSI change: {rsi_current - rsi_prev:.2f}")
        
        # Xác định tín hiệu RSI
        rsi_signal = None
        if price_current < price_prev and rsi_current > rsi_prev:
            rsi_signal = "BUY (Bullish Divergence)"
        elif price_current > price_prev and rsi_current < rsi_prev:
            rsi_signal = "SELL (Bearish Divergence)"
        elif price_current > price_prev and rsi_current > rsi_prev:
            rsi_signal = "BUY (Trend UP)"
        elif price_current < price_prev and rsi_current < rsi_prev:
            rsi_signal = "SELL (Trend DOWN)"
        elif rsi_current < 30:
            rsi_signal = "BUY (Oversold)"
        elif rsi_current > 70:
            rsi_signal = "SELL (Overbought)"
        else:
            rsi_signal = "NO SIGNAL"
        
        print(f"  RSI Signal: {rsi_signal}")
        
        # Debug MACD
        print(f"\n🔍 MACD DEBUG:")
        def calculate_macd(prices, fast=12, slow=26, signal=9):
            ema_fast = prices.ewm(span=fast).mean()
            ema_slow = prices.ewm(span=slow).mean()
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=signal).mean()
            return macd_line, signal_line
        
        macd_line, signal_line = calculate_macd(selected_df['close'])
        print(f"  MACD line: {macd_line.iloc[-1]:.6f}")
        print(f"  Signal line: {signal_line.iloc[-1]:.6f}")
        print(f"  MACD line (prev): {macd_line.iloc[-2]:.6f}")
        print(f"  Signal line (prev): {signal_line.iloc[-2]:.6f}")
        
        # Xác định tín hiệu MACD
        macd_signal = None
        if macd_line.iloc[-1] > signal_line.iloc[-1] and macd_line.iloc[-2] <= signal_line.iloc[-2]:
            macd_signal = "BUY (Bullish Crossover)"
        elif macd_line.iloc[-1] < signal_line.iloc[-1] and macd_line.iloc[-2] >= signal_line.iloc[-2]:
            macd_signal = "SELL (Bearish Crossover)"
        elif macd_line.iloc[-1] > signal_line.iloc[-1]:
            macd_signal = "BUY (Bullish Momentum)"
        elif macd_line.iloc[-1] < signal_line.iloc[-1]:
            macd_signal = "SELL (Bearish Momentum)"
        else:
            macd_signal = "NO SIGNAL"
        
        print(f"  MACD Signal: {macd_signal}")
        
        # Debug Bollinger Bands
        print(f"\n🔍 BOLLINGER BANDS DEBUG:")
        def calculate_bollinger_bands(prices, window=20, std_dev=2):
            sma = prices.rolling(window=window).mean()
            std = prices.rolling(window=window).std()
            upper_band = sma + (std * std_dev)
            lower_band = sma - (std * std_dev)
            return upper_band, sma, lower_band
        
        upper_band, middle_band, lower_band = calculate_bollinger_bands(selected_df['close'])
        current_price = selected_df['close'].iloc[-1]
        print(f"  Current price: {current_price:.4f}")
        print(f"  Upper band: {upper_band.iloc[-1]:.4f}")
        print(f"  Middle band: {middle_band.iloc[-1]:.4f}")
        print(f"  Lower band: {lower_band.iloc[-1]:.4f}")
        
        # Xác định tín hiệu Bollinger Bands
        bollinger_signal = None
        if current_price > upper_band.iloc[-1]:
            bollinger_signal = "BUY (Breakout UP)"
        elif current_price < lower_band.iloc[-1]:
            bollinger_signal = "SELL (Breakout DOWN)"
        elif current_price > middle_band.iloc[-1]:
            bollinger_signal = "BUY (Above Middle)"
        elif current_price < middle_band.iloc[-1]:
            bollinger_signal = "SELL (Below Middle)"
        else:
            bollinger_signal = "NO SIGNAL"
        
        print(f"  Bollinger Signal: {bollinger_signal}")
        
        # Debug Linear Regression
        print(f"\n🔍 LINEAR REGRESSION DEBUG:")
        def calculate_linear_trend_with_distance(prices, window=24):
            if len(prices) < window:
                return None, None, None
            recent_prices = prices.tail(window).values
            X = np.arange(len(recent_prices)).reshape(-1, 1)
            y = recent_prices.reshape(-1, 1)
            model = LinearRegression().fit(X, y)
            slope = model.coef_[0][0]
            
            current_price = recent_prices[-1]
            predicted_price = model.predict([[window-1]])[0][0]
            distance = abs(current_price - predicted_price)
            
            residuals = recent_prices.flatten() - model.predict(X).flatten()
            std_residuals = np.std(residuals)
            
            return slope, distance, std_residuals
        
        linear_result = calculate_linear_trend_with_distance(selected_df['close'])
        if linear_result is not None:
            slope, distance, std_residuals = linear_result
            threshold = 0.001
            distance_threshold = 1.5 * std_residuals
            
            print(f"  Slope: {slope:.6f}")
            print(f"  Distance: {distance:.4f}")
            print(f"  Std residuals: {std_residuals:.4f}")
            print(f"  Distance threshold: {distance_threshold:.4f}")
            print(f"  Slope threshold: {threshold:.6f}")
            print(f"  Slope > threshold: {slope > threshold}")
            print(f"  Distance <= threshold: {distance <= distance_threshold}")
            
            # Xác định tín hiệu Linear Regression
            linear_signal = None
            if slope > threshold and distance <= distance_threshold:
                linear_signal = "BUY (Trend UP)"
            elif slope < -threshold and distance <= distance_threshold:
                linear_signal = "SELL (Trend DOWN)"
            elif distance > distance_threshold:
                linear_signal = "NO SIGNAL (Too far from trendline)"
            else:
                linear_signal = "NO SIGNAL (Weak trend)"
            
            print(f"  Linear Signal: {linear_signal}")
        
        # Tổng kết tất cả tín hiệu
        print(f"\n📊 SUMMARY OF ALL SIGNALS:")
        print(f"  RSI: {rsi_signal}")
        print(f"  MACD: {macd_signal}")
        print(f"  Bollinger: {bollinger_signal}")
        print(f"  Linear: {linear_signal}")
        
        # Đếm confirmations
        buy_count = 0
        sell_count = 0
        
        if "BUY" in rsi_signal:
            buy_count += 1
        elif "SELL" in rsi_signal:
            sell_count += 1
            
        if "BUY" in macd_signal:
            buy_count += 1
        elif "SELL" in macd_signal:
            sell_count += 1
            
        if "BUY" in bollinger_signal:
            buy_count += 1
        elif "SELL" in bollinger_signal:
            sell_count += 1
            
        if "BUY" in linear_signal:
            buy_count += 1
        elif "SELL" in linear_signal:
            sell_count += 1
        
        print(f"\n🎯 FINAL DECISION:")
        print(f"  BUY confirmations: {buy_count}")
        print(f"  SELL confirmations: {sell_count}")
        
        if buy_count >= 3:
            print(f"  👉 RESULT: BUY (Đủ {buy_count}/4 confirmations)")
        elif sell_count >= 3:
            print(f"  👉 RESULT: SELL (Đủ {sell_count}/4 confirmations)")
        else:
            print(f"  👉 RESULT: NO TRADE (Không đủ confirmations hoặc bị chia đều)")
        
    except Exception as e:
        print(f"❌ Lỗi debug: {e}")

if __name__ == "__main__":
    test_4_confirmation_layers()
    test_confirmation_breakdown()
    test_debug_individual_layers() 