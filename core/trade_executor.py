# trade_executor.py
from binance.client import Client
from config import BINANCE_API_KEY, BINANCE_API_SECRET, DAILY_LIMIT
from core.supabase_manager import SupabaseManager
from datetime import datetime, timedelta
import time
import threading
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY
from data_collector import get_data

# Khởi tạo Binance client
# Kiểm tra xem có phải testnet keys không (thường có format khác)
if "testnet" in BINANCE_API_KEY.lower() or len(BINANCE_API_KEY) > 100:
    # Sử dụng testnet
    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=True)
    print("🔧 Sử dụng Binance Testnet")
else:
    # Sử dụng mainnet
    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    print("🔧 Sử dụng Binance Mainnet")

supabase_manager = SupabaseManager()

# Supabase client cho realtime
supabase_realtime: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_capital_by_rank(rank, daily_limit):
    """
    Phân bổ vốn theo rank của pair
    - Rank 1-3: 15% daily_limit mỗi pair
    - Rank 4-6: 10% daily_limit mỗi pair  
    - Rank 7-10: 5% daily_limit mỗi pair
    """
    if rank <= 3:
        return daily_limit * 0.15
    elif rank <= 6:
        return daily_limit * 0.10
    elif rank <= 10:
        return daily_limit * 0.05
    else:
        return 0

def get_account_balance():
    """Lấy balance của tài khoản futures"""
    try:
        account = client.futures_account()
        total_balance = 0
        for asset in account['assets']:
            if asset['asset'] == 'USDT':
                total_balance = float(asset['walletBalance'])
                break
        return total_balance
    except Exception as e:
        print(f"Lỗi khi lấy account balance: {e}")
        return DAILY_LIMIT  # Fallback to config limit

def get_pair_by_id(pair_id):
    """Lấy thông tin pair theo ID"""
    try:
        pair = supabase_manager.get_pair_by_id(pair_id)
        return pair
    except Exception as e:
        return None

def check_existing_position(symbol):
    """Kiểm tra xem đã có position cho symbol này chưa"""
    try:
        positions = supabase_manager.get_open_positions_by_symbol(symbol)
        return len(positions) > 0
    except Exception as e:
        print(f"Lỗi khi kiểm tra existing position: {e}")
        return False

def calculate_current_zscore(position):
    """
    Tính z-score hiện tại cho position
    """
    try:
        # Lấy thông tin pair
        pair = get_pair_by_id(position['pair_id'])
        if not pair:
            return None
        
        # Lấy dữ liệu giá hiện tại
        df1 = get_data(pair['pair1'], interval='15m', limit=100)
        df2 = get_data(pair['pair2'], interval='15m', limit=100)
        
        if df1 is None or df2 is None:
            return None
        
        # Tính spread và z-score
        spread = df1['close'] - df2['close']
        mean = spread.rolling(window=30).mean()
        std = spread.rolling(window=30).std()
        zscore = (spread - mean) / std
        
        if len(zscore) == 0:
            return None
        
        return zscore.iloc[-1]
        
    except Exception as e:
        print(f"Lỗi khi tính z-score: {e}")
        return None

def should_close_pair_positions(pair_id):
    """
    Kiểm tra xem có nên đóng cả 2 positions của pair không
    """
    try:
        # Lấy tất cả positions của pair này
        pair_positions = supabase_manager.get_open_positions_by_pair_id(pair_id)
        
        if len(pair_positions) < 2:
            print(f"⚠️ Pair {pair_id} chỉ có {len(pair_positions)} position(s), không đủ để đóng")
            return False
        
        # Tính z-score hiện tại (chỉ cần tính 1 lần vì cùng pair)
        current_zscore = calculate_current_zscore(pair_positions[0])
        if current_zscore is None:
            return False
        
        # Lấy z-score khi vào lệnh (từ position đầu tiên)
        entry_zscore = pair_positions[0].get('z_score', 0)
        
        print(f"🔍 Checking pair {pair_id} positions:")
        print(f"   - Entry z-score: {entry_zscore:.2f}")
        print(f"   - Current z-score: {current_zscore:.2f}")
        print(f"   - Number of positions: {len(pair_positions)}")
        
        # Đóng lệnh khi z-score về gần 0 (mean reversion)
        # Nếu entry z-score > 0 (BUY signal), đóng khi z-score < 0.5
        # Nếu entry z-score < 0 (SELL signal), đóng khi z-score > -0.5
        if entry_zscore > 0 and current_zscore < 0.5:
            print(f"✅ Z-score mean reversion: {entry_zscore:.2f} → {current_zscore:.2f}")
            return True
        elif entry_zscore < 0 and current_zscore > -0.5:
            print(f"✅ Z-score mean reversion: {entry_zscore:.2f} → {current_zscore:.2f}")
            return True
        
        return False
        
    except Exception as e:
        print(f"Lỗi khi kiểm tra close condition cho pair: {e}")
        return False

def close_pair_positions(pair_id):
    """
    Đóng cả 2 positions của pair
    """
    try:
        # Lấy tất cả positions của pair
        pair_positions = supabase_manager.get_open_positions_by_pair_id(pair_id)
        
        if len(pair_positions) < 2:
            print(f"⚠️ Không đủ positions để đóng cho pair {pair_id}")
            return False
        
        print(f"🔄 Đóng {len(pair_positions)} positions cho pair {pair_id}...")
        
        total_pnl = 0
        closed_count = 0
        
        for position in pair_positions:
            try:
                # Lấy giá hiện tại
                ticker = client.futures_symbol_ticker(symbol=position['symbol'])
                exit_price = float(ticker['price'])
                
                # Đóng position
                result = close_position(position, exit_price)
                
                if result:
                    closed_count += 1
                    # Tính PnL (sẽ được tính trong close_position function)
                
            except Exception as e:
                print(f"❌ Lỗi khi đóng position {position['symbol']}: {e}")
        
        print(f"✅ Đã đóng {closed_count}/{len(pair_positions)} positions cho pair {pair_id}")
        return closed_count == len(pair_positions)
        
    except Exception as e:
        print(f"❌ Lỗi khi đóng pair positions: {e}")
        return False

def get_open_positions():
    """
    Lấy tất cả open positions
    """
    try:
        positions = supabase_manager.get_all_open_positions()
        return positions
    except Exception as e:
        print(f"Lỗi khi lấy open positions: {e}")
        return []

def get_unique_pair_ids():
    """
    Lấy danh sách unique pair IDs có open positions
    """
    try:
        positions = get_open_positions()
        pair_ids = list(set([pos['pair_id'] for pos in positions]))
        return pair_ids
    except Exception as e:
        print(f"Lỗi khi lấy unique pair IDs: {e}")
        return []

def monitor_and_close_positions():
    """
    Monitor và đóng positions khi z-score về 0 (đóng cả 2 positions của pair)
    """
    print("🔍 Bắt đầu monitor positions cho z-score mean reversion...")
    
    while True:
        try:
            # Lấy danh sách unique pair IDs có open positions
            pair_ids = get_unique_pair_ids()
            
            if pair_ids:
                print(f"📊 Đang monitor {len(pair_ids)} pairs có open positions...")
                
                for pair_id in pair_ids:
                    # Kiểm tra có nên đóng cả 2 positions của pair không
                    if should_close_pair_positions(pair_id):
                        # Đóng cả 2 positions
                        close_pair_positions(pair_id)
            
            # Sleep 30 giây trước khi check lại
            time.sleep(30)
            
        except KeyboardInterrupt:
            print("\n⏹️ Dừng monitor positions...")
            break
        except Exception as e:
            print(f"❌ Lỗi trong monitor positions loop: {e}")
            time.sleep(30)

def execute_trade(signal, pair, account_balance):
    """
    Execute trade dựa trên signal
    """
    symbol = signal['symbol']
    side = signal['signal_type']
    rank = pair['rank']
    z_score = signal['z_score']
    
    print(f"🚨 REALTIME SIGNAL: {side} {symbol} (rank {rank}, z={z_score:.2f})")
    
    # Kiểm tra xem đã có position cho symbol này chưa
    if check_existing_position(symbol):
        print(f"⚠️ Đã có position cho {symbol}, bỏ qua signal")
        return None
    
    # Tính vốn phân bổ
    capital = get_capital_by_rank(rank, account_balance)
    if capital == 0:
        return None
    
    # Lấy giá thị trường hiện tại
    try:
        ticker = client.futures_symbol_ticker(symbol=symbol)
        entry_price = float(ticker['price'])
    except Exception as e:
        print(f"❌ Không lấy được giá cho {symbol}: {e}")
        return None
    
    # Tính quantity
    quantity = capital / entry_price
    
    # Execute order
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity=quantity
        )
        
        # Lưu position vào database
        position_data = {
            'pair_id': pair['id'],
            'symbol': symbol,
            'entry_price': float(entry_price),
            'quantity': float(quantity),
            'status': 'OPEN',
            'entry_time': datetime.now().isoformat(),
            'binance_order_id': order['orderId'],
            'z_score': float(z_score),
            'signal_id': signal.get('id')
        }
        supabase_manager.save_position(position_data)
        
        print(f"✅ REALTIME EXECUTE: {side} {symbol}")
        print(f"   - Vốn: {capital:.2f} USD")
        print(f"   - Giá: {entry_price:.4f}")
        print(f"   - Số lượng: {quantity:.4f}")
        print(f"   - Z-score: {z_score:.2f}")
        print(f"   - Thời gian: {datetime.now().strftime('%H:%M:%S')}")
        
        return order
        
    except Exception as e:
        print(f"❌ Lỗi khi execute trade: {e}")
        return None

def close_position(position, exit_price):
    """Đóng position"""
    try:
        # Xác định side để đóng position
        if position['status'] == 'OPEN':
            # Nếu entry_price < exit_price (lãi), thì SELL để đóng BUY position
            # Nếu entry_price > exit_price (lỗ), thì BUY để đóng SELL position
            side = 'SELL' if position['entry_price'] < exit_price else 'BUY'
            
            order = client.futures_create_order(
                symbol=position['symbol'],
                side=side,
                type='MARKET',
                quantity=position['quantity']
            )
            
            # Tính PnL
            pnl = (exit_price - position['entry_price']) * position['quantity']
            
            # Cập nhật status trong database
            supabase_manager.update_position_status(position['id'], 'CLOSED', pnl=pnl)
            
            print(f"✅ Đã đóng position {position['symbol']}")
            print(f"   - PnL: {pnl:.2f} USD")
            print(f"   - Exit price: {exit_price:.4f}")
            print(f"   - Reason: Z-score mean reversion")
            
            return order
            
    except Exception as e:
        print(f"❌ Lỗi khi đóng position: {e}")
        return None

def handle_realtime_signal(payload):
    """
    Xử lý signal realtime từ Supabase
    """
    try:
        # Lấy thông tin signal từ payload
        signal = payload['record']
        
        print(f"🎯 REALTIME SIGNAL DETECTED!")
        print(f"   - Symbol: {signal['symbol']}")
        print(f"   - Signal: {signal['signal_type']}")
        print(f"   - Z-score: {signal.get('z_score', 'N/A')}")
        print(f"   - Pair ID: {signal['pair_id']}")
        
        # Lấy account balance
        account_balance = get_account_balance()
        
        # Lấy thông tin pair
        pair = get_pair_by_id(signal['pair_id'])
        if not pair:
            print(f"❌ Không tìm thấy pair info cho ID: {signal['pair_id']}")
            return
        
        # Execute trade ngay lập tức
        execute_trade(signal, pair, account_balance)
        
    except Exception as e:
        print(f"❌ Lỗi khi xử lý realtime signal: {e}")

def start_realtime_monitoring():
    """
    Bắt đầu realtime monitoring cho signals
    """
    print("🚀 Bắt đầu REALTIME signal monitoring...")
    
    try:
        # Subscribe to trading_signals table
        channel = supabase_realtime.channel('trading_signals_changes')
        
        # Listen for INSERT events (new signals)
        channel.on('postgres_changes', 
                  event='INSERT', 
                  schema='public', 
                  table='trading_signals',
                  callback=handle_realtime_signal)
        
        # Subscribe to the channel
        channel.subscribe()
        
        print("✅ Đã subscribe realtime channel cho trading_signals")
        print("⏳ Đang chờ signals realtime...")
        
        # Keep the connection alive
        while True:
            time.sleep(1)
            
    except Exception as e:
        print(f"❌ Lỗi trong realtime monitoring: {e}")
        # Retry after 5 seconds
        time.sleep(5)
        start_realtime_monitoring()

def monitor_and_execute_trades():
    """
    Monitor signals và execute trades tự động (fallback method)
    """
    print("🔄 Bắt đầu monitor và execute trades (fallback)...")
    
    while True:
        try:
            # Lấy account balance
            account_balance = get_account_balance()
            print(f"💰 Account balance: {account_balance:.2f} USD")
            
            # Lấy recent signals (trong 1 phút gần đây)
            recent_time = datetime.now() - timedelta(minutes=1)
            signals = supabase_manager.get_recent_signals(recent_time)
            
            if signals:
                print(f"📊 Tìm thấy {len(signals)} signals gần đây")
                
                for signal in signals:
                    # Lấy thông tin pair
                    pair = get_pair_by_id(signal['pair_id'])
                    if not pair:
                        continue
                    
                    # Execute trade
                    execute_trade(signal, pair, account_balance)
            
            # Sleep 10 giây trước khi check lại (nhanh hơn cho fallback)
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n⏹️ Dừng monitor trades...")
            break
        except Exception as e:
            print(f"❌ Lỗi trong monitor loop: {e}")
            time.sleep(10)

def main():
    """Main function"""
    print("🎯 Trade Executor - REALTIME SIGNAL MONITORING + PAIR Z-SCORE CLOSING")
    print("=" * 75)
    
    # Test connection
    try:
        balance = get_account_balance()
        print(f"✅ Kết nối Binance thành công. Balance: {balance:.2f} USD")
    except Exception as e:
        print(f"❌ Lỗi kết nối Binance: {e}")
        return
    
    # Test Supabase connection
    try:
        pairs = supabase_manager.get_top_pairs(limit=1)
        print(f"✅ Kết nối Supabase thành công")
    except Exception as e:
        print(f"❌ Lỗi kết nối Supabase: {e}")
        return
    
    print("\n🎯 Chọn mode:")
    print("1. Realtime monitoring + Pair z-score closing (recommended)")
    print("2. Polling fallback + Pair z-score closing")
    print("3. Pair z-score closing only")
    
    try:
        choice = input("Nhập lựa chọn (1/2/3): ").strip()
        
        if choice == "1":
            print("\n🚀 Bắt đầu REALTIME monitoring + Pair z-score closing...")
            # Start z-score monitoring in background thread
            zscore_thread = threading.Thread(target=monitor_and_close_positions, daemon=True)
            zscore_thread.start()
            
            # Start realtime monitoring
            start_realtime_monitoring()
        elif choice == "2":
            print("\n🔄 Bắt đầu polling fallback + Pair z-score closing...")
            # Start z-score monitoring in background thread
            zscore_thread = threading.Thread(target=monitor_and_close_positions, daemon=True)
            zscore_thread.start()
            
            # Start polling
            monitor_and_execute_trades()
        else:
            print("\n🔍 Bắt đầu Pair z-score closing only...")
            monitor_and_close_positions()
            
    except KeyboardInterrupt:
        print("\n⏹️ Dừng trade executor...")

if __name__ == "__main__":
    main() 