# data_collector.py
from binance.client import Client
import pandas as pd
import numpy as np
from itertools import combinations
from datetime import datetime
from config import BINANCE_API_KEY, BINANCE_API_SECRET, DAILY_TOP_N
from core.supabase_manager import SupabaseManager
from statsmodels.tsa.stattools import coint
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from functools import lru_cache

# Tăng timeout cho Binance client
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=False)
client.timeout = 30  # Tăng timeout lên 30 giây
supabase_manager = SupabaseManager()

# Cache để lưu dữ liệu đã fetch
_data_cache = {}
_cache_lock = threading.Lock()

def get_data_with_retry(symbol, interval="1h", limit=168, max_retries=3):  
    """Lấy dữ liệu với retry mechanism và cache"""
    cache_key = f"{symbol}_{interval}_{limit}"
    
    # Kiểm tra cache trước
    with _cache_lock:
        if cache_key in _data_cache:
            return _data_cache[cache_key]
    
    for attempt in range(max_retries):
        try:
            klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)
            
            # Cache kết quả
            with _cache_lock:
                _data_cache[cache_key] = df
            
            return df
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for {symbol}: {e}")
                time.sleep(1)  # Đợi 1 giây trước khi retry
            else:
                print(f"❌ Failed to get data for {symbol} after {max_retries} attempts: {e}")
                return None

def get_data_via_rest_api(symbol, interval="1h", limit=168):
    try:
        # Kiểm tra cache trước
        cache_key = f"{symbol}_rest_{interval}_{limit}"
        with _cache_lock:
            if cache_key in _data_cache:
                return _data_cache[cache_key]
        
        # Lấy data từ REST API
        df = get_data_with_retry(symbol, interval, limit)
        
        if df is not None and len(df) > 0:
            # Cache kết quả
            with _cache_lock:
                _data_cache[cache_key] = df
            return df
        
        return None
        
    except Exception as e:
        print(f"❌ Error getting data via REST API cho {symbol}: {e}")
        return None

def get_data(symbol, interval="1h", limit=168):
    """Wrapper function - sử dụng REST API với retry mechanism"""
    return get_data_with_retry(symbol, interval, limit)

def get_all_usdt_pairs():
    """Lấy tất cả cặp USDT đang trading với retry"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            exchange_info = client.futures_exchange_info()
            
            # Lọc chỉ cặp USDT và đang trading
            usdt_pairs = []
            for symbol in exchange_info['symbols']:
                if (symbol['symbol'].endswith('USDT') and 
                    symbol['status'] == 'TRADING' and
                    symbol['contractType'] == 'PERPETUAL'):
                    usdt_pairs.append(symbol['symbol'])
            
            print(f"📊 Tìm thấy {len(usdt_pairs)} cặp USDT")
            return usdt_pairs
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} getting pairs: {e}")
                time.sleep(2)  # Giảm delay từ 3s xuống 2s
            else:
                print(f"❌ Error getting pairs after {max_retries} attempts: {e}")
                return []

def calculate_usdt_volume_optimized(symbol, limit=24):
    """Tính volume theo USDT cho một cặp - tối ưu hóa"""
    try:
        # Sử dụng cache nếu có
        cache_key = f"{symbol}_1h_{limit}"
        with _cache_lock:
            if cache_key in _data_cache:
                df = _data_cache[cache_key]
            else:
                # Lấy dữ liệu 24h
                klines = client.futures_klines(symbol=symbol, interval="1h", limit=limit)
                df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
                df['volume'] = df['volume'].astype(float)
                df['close'] = df['close'].astype(float)
                
                # Cache kết quả
                _data_cache[cache_key] = df
        
        # Tính volume theo USDT
        avg_volume_base = df['volume'].mean()  # Số lượng coin
        avg_price = df['close'].mean()  # Giá trung bình
        usdt_volume = avg_volume_base * avg_price  # Volume theo USDT
        
        return {
            'symbol': symbol,
            'base_volume': avg_volume_base,
            'avg_price': avg_price,
            'usdt_volume': usdt_volume,
            'current_price': df['close'].iloc[-1]
        }
        
    except Exception as e:
        print(f"❌ Error calculating volume for {symbol}: {e}")
        return None

def calculate_volume_batch(pairs_batch):
    """Tính volume cho một batch pairs - parallel processing"""
    results = []
    for symbol in pairs_batch:
        data = calculate_usdt_volume_optimized(symbol)
        if data and data['usdt_volume'] > 0:
            results.append(data)
    return results

def filter_pairs_by_usdt_volume_parallel(top_percentile=50, max_workers=10):
    """Lọc cặp theo volume USDT với parallel processing"""
    print(f"\n🔍 TÍNH TOÁN VOLUME THEO USDT (PARALLEL)")
    print("=" * 50)
    
    # Lấy tất cả cặp USDT
    pairs = get_all_usdt_pairs()
    
    if not pairs:
        print("❌ Không lấy được danh sách cặp")
        return []
    
    print(f"📊 Đang tính volume cho {len(pairs)} cặp (parallel)...")
    
    # Chia pairs thành batches
    batch_size = max(1, len(pairs) // max_workers)
    batches = [pairs[i:i + batch_size] for i in range(0, len(pairs), batch_size)]
    
    # Parallel processing
    volume_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(calculate_volume_batch, batch): batch for batch in batches}
        
        completed = 0
        for future in as_completed(future_to_batch):
            batch_results = future.result()
            volume_data.extend(batch_results)
            completed += 1
            print(f"📊 Hoàn thành batch {completed}/{len(batches)} ({len(volume_data)} pairs processed)")
    
    if not volume_data:
        print("❌ Không có dữ liệu volume")
        return []
    
    # Tạo DataFrame và sắp xếp
    df = pd.DataFrame(volume_data)
    df = df.sort_values('usdt_volume', ascending=False)
    
    # Tính percentile và lọc top 50%
    volume_threshold = np.percentile(df['usdt_volume'], 100 - top_percentile)
    top_pairs = df[df['usdt_volume'] >= volume_threshold]
    
    print(f"\n📊 KẾT QUẢ FILTER:")
    print(f"- Tổng cặp: {len(df)}")
    print(f"- Top {top_percentile}%: {len(top_pairs)} cặp")
    print(f"- Volume threshold: ${volume_threshold:,.2f}")
    
    # Hiển thị top 10 pairs
    print(f"\n🏆 TOP 10 PAIRS THEO VOLUME USDT:")
    print("=" * 80)
    print(f"{'Rank':<5} {'Symbol':<12} {'Volume (USDT)':<15} {'Base Volume':<15} {'Price':<12}")
    print("-" * 80)
    
    for i, row in top_pairs.head(10).iterrows():
        rank = i + 1
        symbol = row['symbol']
        usdt_vol = f"${row['usdt_volume']:,.0f}"
        base_vol = f"{row['base_volume']:.2f}"
        price = f"${row['current_price']:.2f}"
        
        print(f"{rank:<5} {symbol:<12} {usdt_vol:<15} {base_vol:<15} {price:<12}")
    
    return top_pairs['symbol'].tolist()

def check_data_quality(symbol, min_data_points=100):
    """Kiểm tra chất lượng dữ liệu của một symbol"""
    try:
        # Lấy dữ liệu với cache
        df = get_data(symbol, interval="1h", limit=168)
        
        if df is None:
            return False, 0, "No data"
        
        # Kiểm tra số lượng data points
        if len(df) < min_data_points:
            return False, len(df), f"Insufficient data points ({len(df)} < {min_data_points})"
        
        # Kiểm tra giá hằng số
        if df['close'].std() == 0 or df['close'].nunique() <= 1:
            return False, len(df), "Constant price"
        
        # Kiểm tra missing values
        if df['close'].isna().any():
            return False, len(df), "Missing values"
        
        # Kiểm tra volume > 0
        if df['volume'].sum() == 0:
            return False, len(df), "No volume"
        
        return True, len(df), "OK"
        
    except Exception as e:
        return False, 0, f"Error: {str(e)}"

def check_data_quality_batch(symbols_batch, min_data_points=100):
    """Kiểm tra chất lượng dữ liệu cho một batch symbols"""
    results = []
    for symbol in symbols_batch:
        is_valid, data_points, reason = check_data_quality(symbol, min_data_points)
        if is_valid:
            results.append(symbol)
        else:
            print(f"❌ {symbol}: {reason} ({data_points} points)")
    return results

def filter_data_quality_parallel(symbols, min_data_points=100, max_workers=8):
    """Lọc symbols theo chất lượng dữ liệu với parallel processing"""
    print(f"\n🔍 BƯỚC 2: KIỂM TRA CHẤT LƯỢNG DỮ LIỆU")
    print("=" * 50)
    print(f"📊 Đang kiểm tra {len(symbols)} cặp (min {min_data_points} data points)...")
    
    # Chia symbols thành batches
    batch_size = max(1, len(symbols) // max_workers)
    batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
    
    # Parallel processing
    valid_symbols = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(check_data_quality_batch, batch, min_data_points): batch for batch in batches}
        
        completed = 0
        for future in as_completed(future_to_batch):
            batch_results = future.result()
            valid_symbols.extend(batch_results)
            completed += 1
            print(f"📊 Hoàn thành batch {completed}/{len(batches)} ({len(valid_symbols)} valid pairs)")
    
    print(f"\n📊 KẾT QUẢ FILTER DỮ LIỆU:")
    print(f"- Tổng cặp: {len(symbols)}")
    print(f"- Cặp hợp lệ: {len(valid_symbols)}")
    print(f"- Tỷ lệ loại bỏ: {((len(symbols) - len(valid_symbols)) / len(symbols) * 100):.1f}%")
    
    return valid_symbols

def calculate_correlation_cointegration(symbol1, symbol2):
    try:
        # Lấy dữ liệu giá sử dụng hàm get_data
        df1 = get_data(symbol1, interval="1h", limit=168)
        df2 = get_data(symbol2, interval="1h", limit=168)
        
        if df1 is None or df2 is None:
            return None, None, None, None, None, None
        
        if len(df1) < 100 or len(df2) < 100:
            print(f"Bỏ qua {symbol1}-{symbol2}: không đủ dữ liệu ({len(df1)}, {len(df2)})")
            return None, None, None, None, None, None
        
        # Kiểm tra giá hằng số - cải thiện
        if (df1['close'].std() == 0 or df2['close'].std() == 0 or 
            df1['close'].nunique() <= 1 or df2['close'].nunique() <= 1 or
            df1['close'].isna().any() or df2['close'].isna().any()):
            return None, None, None, None, None, None
        
        # Tính correlation
        correlation = df1['close'].corr(df2['close'])
        
        # Early exit nếu correlation quá thấp - tăng threshold
        if pd.isna(correlation) or abs(correlation) < 0.5: 
            return None, None, None, None, None, None
        
        # Tính rolling correlation (7 periods)
        rolling_corr = df1['close'].rolling(7).corr(df2['close']).mean()
        
        # Kiểm tra cointegration với try-catch
        try:
            result = coint(df1['close'], df2['close'])
            p_value = result[1]
        except Exception as coint_error:
            return None, None, None, None, None, None
        
        # Kiểm tra p_value có hợp lệ không
        if pd.isna(p_value):
            return None, None, None, None, None, None
        
        # Tính volatility
        vol1 = df1['close'].pct_change().std() * np.sqrt(24)  # Annualized
        vol2 = df2['close'].pct_change().std() * np.sqrt(24)
        
        return correlation, p_value, rolling_corr, vol1, vol2, None
        
    except Exception as e:
        return None, None, None, None, None, None

def analyze_pair_batch(pair_batch):
    results = []
    for symbol1, symbol2 in pair_batch:
        correlation, p_value, rolling_corr, vol1, vol2, _ = calculate_correlation_cointegration(symbol1, symbol2)
        
        # Chỉ lưu những cặp có correlation cao (>0.5) và cointegrated
        if (correlation is not None and p_value is not None and 
            abs(correlation) > 0.5 and p_value < 0.05):
            results.append({
                'pair1': symbol1,
                'pair2': symbol2,
                'correlation': correlation,
                'rolling_correlation': rolling_corr,
                'cointegration_p_value': p_value,
                'is_cointegrated': p_value < 0.05,
                'volatility_1': vol1,
                'volatility_2': vol2
            })
    return results

def analyze_correlation_stats(results_df):
    """Phân tích thống kê correlation: min, max, mean, median, std"""
    stats = {}
    if len(results_df) == 0 or 'correlation' not in results_df:
        print("No correlation data to analyze.")
        return stats
    correlations = results_df['correlation'].dropna()
    stats['count'] = len(correlations)
    stats['mean'] = correlations.mean()
    stats['median'] = correlations.median()
    stats['std'] = correlations.std()
    stats['min'] = correlations.min()
    stats['max'] = correlations.max()
    print("\n========== CORRELATION STATISTICS ==========")
    print(f"Số cặp: {stats['count']}")
    print(f"Mean: {stats['mean']:.4f}")
    print(f"Median: {stats['median']:.4f}")
    print(f"Std: {stats['std']:.4f}")
    print(f"Min: {stats['min']:.4f}")
    print(f"Max: {stats['max']:.4f}")
    print("===========================================\n")

    # Lưu thống kê vào Supabase
    supabase_manager = SupabaseManager()
    success = supabase_manager.save_correlation_stats(stats)
    if success:
        print("✅ Đã lưu correlation stats vào Supabase thành công!")
    else:
        print("❌ Lỗi khi lưu correlation stats vào Supabase")
    return stats
    

def scan_market_for_stable_pairs_optimized():
    """Scan thị trường với tối ưu hóa parallel processing và data quality filter"""
    # Bước 1: Lọc cặp theo volume USDT (top 50%) - parallel
    print("🔍 BƯỚC 1: LỌC CẶP THEO VOLUME USDT (PARALLEL)")
    filtered_pairs = filter_pairs_by_usdt_volume_parallel(top_percentile=50, max_workers=8)
    
    if not filtered_pairs:
        print("❌ Không có cặp nào sau khi lọc volume")
        return []
    
    # Bước 2: Lọc theo chất lượng dữ liệu - parallel
    print(f"\n🔍 BƯỚC 2: LỌC THEO CHẤT LƯỢNG DỮ LIỆU (PARALLEL)")
    quality_filtered_pairs = filter_data_quality_parallel(filtered_pairs, min_data_points=100, max_workers=8)
    
    if not quality_filtered_pairs:
        print("❌ Không có cặp nào sau khi lọc chất lượng dữ liệu")
        return []
    
    # Bước 3: Tạo combinations và phân tích parallel
    print(f"\n🔍 BƯỚC 3: PHÂN TÍCH COMBINATIONS (PARALLEL)")
    print(f"📊 Đang tạo combinations từ {len(quality_filtered_pairs)} cặp...")
    
    pair_combinations = list(combinations(quality_filtered_pairs, 2))
    print(f"📊 Tổng số combinations: {len(pair_combinations)}")
    
    # Chia combinations thành batches cho parallel processing
    max_workers = 6  # Giảm số workers để tránh rate limit
    batch_size = max(1, len(pair_combinations) // max_workers)
    batches = [pair_combinations[i:i + batch_size] for i in range(0, len(pair_combinations), batch_size)]
    
    # Parallel processing với progress tracking
    results = []
    completed_batches = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(analyze_pair_batch, batch): batch for batch in batches}
        
        for future in as_completed(future_to_batch):
            batch_results = future.result()
            results.extend(batch_results)
            completed_batches += 1
            
            # Progress tracking
            progress = (completed_batches / len(batches)) * 100
            print(f"📈 Progress: {progress:.1f}% ({completed_batches}/{len(batches)} batches) - Found {len(results)} valid pairs")
    
    # Bước 4: Phân tích kết quả
    print(f"\n📊 BƯỚC 4: PHÂN TÍCH KẾT QUẢ")
    print(f"✅ Tìm thấy {len(results)} cặp có correlation cao và cointegrated")
    
    results_df = pd.DataFrame(results)
    
    if len(results_df) > 0:
        analyze_correlation_stats(results_df)
        results_df = results_df.sort_values('correlation', ascending=False)
        
        # Chỉ lấy top 10 cặp
        top_10_pairs = results_df.head(10)
        print(f"\n🏆 TOP 10 PAIRS:")
        print("=" * 80)
        print(f"{'Rank':<5} {'Pair':<20} {'Correlation':<12} {'P-Value':<12} {'Cointegrated':<12}")
        print("-" * 80)
        
        for idx, row in top_10_pairs.iterrows():
            rank = idx + 1
            pair = f"{row['pair1']}-{row['pair2']}"
            corr = f"{row['correlation']:.4f}"
            p_val = f"{row['cointegration_p_value']:.4f}"
            coint = "✅" if row['is_cointegrated'] else "❌"
            
            print(f"{rank:<5} {pair:<20} {corr:<12} {p_val:<12} {coint:<12}")
        
        # Lưu chỉ top 10 vào Supabase
        today = datetime.now().date()
        pairs_data = []
        for idx, row in top_10_pairs.iterrows():
            pairs_data.append({
                'date': str(today),
                'pair1': row['pair1'],
                'pair2': row['pair2'],
                'correlation': float(row['correlation']),
                'rolling_correlation': float(row['rolling_correlation']) if not pd.isna(row['rolling_correlation']) else None,
                'cointegration_p_value': float(row['cointegration_p_value']),
                'is_cointegrated': bool(row['is_cointegrated']),
                'volatility_1': float(row['volatility_1']) if not pd.isna(row['volatility_1']) else None,
                'volatility_2': float(row['volatility_2']) if not pd.isna(row['volatility_2']) else None,
                'rank': int(idx) + 1
            })
        
        # Lưu daily pairs và lấy lại để có ID
        saved_pairs = supabase_manager.save_daily_pairs(pairs_data)
        print(f"✅ Đã lưu top 10 cặp vào database")

        # Lưu hourly ranking với ID trực tiếp từ saved_pairs
        ranking_data = []
        for idx, pair in enumerate(saved_pairs):
            correlation, p_value, rolling_corr, vol1, vol2, _ = calculate_correlation_cointegration(pair['pair1'], pair['pair2'])
            if correlation is not None:
                ranking_data.append({
                    'timestamp': datetime.now().isoformat(),
                    'pair_id': pair['id'],  # Sử dụng ID trực tiếp từ saved_pairs
                    'current_rank': idx + 1,
                    'current_correlation': float(correlation),
                    'rolling_correlation': float(rolling_corr) if rolling_corr is not None else None,
                    'volatility_1': float(vol1) if vol1 is not None else None,
                    'volatility_2': float(vol2) if vol2 is not None else None
                })
                print(f"✅ Hourly ranking: {pair['pair1']}-{pair['pair2']}: pair_id {pair['id']}")
        
        if ranking_data:
            supabase_manager.update_hourly_ranking(ranking_data)
            print(f"✅ Đã lưu hourly ranking với {len(ranking_data)} pairs")
        else:
            print("⚠️ Không có hourly ranking data để lưu")

        return pairs_data
    else:
        print("❌ Không tìm thấy cặp nào hợp lệ")
        return []

def reorder_pairs_by_correlation():
    # Get today's pairs
    top_pairs = supabase_manager.get_current_top_n(DAILY_TOP_N)
    ranking_data = []
    
    print(f"🔄 Reordering {len(top_pairs)} pairs by correlation...")
    
    # Tính correlation mới cho tất cả pairs
    pairs_with_correlation = []
    for pair in top_pairs:
        correlation, p_value, rolling_corr, vol1, vol2, _ = calculate_correlation_cointegration(pair['pair1'], pair['pair2'])
        if correlation is not None:
            pairs_with_correlation.append({
                'pair': pair,
                'correlation': correlation,
                'rolling_correlation': rolling_corr,
                'volatility_1': vol1,
                'volatility_2': vol2
            })
            print(f"📊 {pair['pair1']}-{pair['pair2']}: corr {correlation:.4f}")
        else:
            print(f"⚠️ Bỏ qua {pair['pair1']}-{pair['pair2']}: không tính được correlation")
    
    # Sắp xếp theo correlation mới (cao → thấp)
    pairs_with_correlation.sort(key=lambda x: x['correlation'], reverse=True)
    
    # Tạo ranking data với rank đúng
    for idx, pair_data in enumerate(pairs_with_correlation):
        pair = pair_data['pair']
        ranking_data.append({
            'timestamp': datetime.now().isoformat(),
            'pair_id': pair['id'],
            'current_rank': idx + 1,  # Rank theo correlation mới
            'current_correlation': float(pair_data['correlation']),
            'rolling_correlation': float(pair_data['rolling_correlation']) if pair_data['rolling_correlation'] is not None else None,
            'volatility_1': float(pair_data['volatility_1']) if pair_data['volatility_1'] is not None else None,
            'volatility_2': float(pair_data['volatility_2']) if pair_data['volatility_2'] is not None else None
        })
        print(f"✅ {pair['pair1']}-{pair['pair2']}: rank {idx+1}, corr {pair_data['correlation']:.4f}, pair_id {pair['id']}")
    
    # Cập nhật vào database
    if ranking_data:
        supabase_manager.update_hourly_ranking(ranking_data)
        print(f"✅ Đã cập nhật hourly ranking với {len(ranking_data)} pairs")
    else:
        print("⚠️ Không có ranking data để cập nhật")
    
    return ranking_data

# Alias cho backward compatibility
scan_market_for_stable_pairs = scan_market_for_stable_pairs_optimized 