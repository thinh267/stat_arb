# trade_executor_simulation.py
from datetime import datetime, timedelta
import time
from core.supabase_manager import SupabaseManager
from core.data_collector import get_data
from collections import defaultdict

# =====================
# 1. Helper functions
# =====================

def get_capital_by_rank(rank, daily_limit=None):
    """
    Phân bổ vốn cố định:
    - Rank 1-3: 15 USD
    - Rank 4-5: 10 USD
    - Rank 6-10: 5 USD
    - Khác: 0 USD
    """
    if rank <= 3:
        return 15.0
    elif rank <= 5:
        return 10.0
    elif rank <= 10:
        return 5.0
    else:
        return 0.0

SIMULATION_BALANCE = 100.0
simulation_balance = SIMULATION_BALANCE
supabase_manager = SupabaseManager()

def get_simulation_balance():
    global simulation_balance
    return simulation_balance

def get_current_price(symbol):
    try:
        from binance.client import Client as BinanceClient
        from config import BINANCE_API_KEY, BINANCE_API_SECRET
        client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=False)
        ticker = client.futures_symbol_ticker(symbol=symbol)
        return float(ticker['price'])
    except Exception as e:
        print(f"❌ Không lấy được giá cho {symbol}: {e}")
        return None

# =====================
# 2. Position helpers
# =====================

def check_existing_position(symbol):
    try:
        positions = supabase_manager.get_open_positions_by_symbol(symbol)
        return len(positions) > 0
    except Exception as e:
        print(f"Lỗi khi kiểm tra existing position: {e}")
        return False

def get_open_positions():
    try:
        return supabase_manager.get_all_open_positions()
    except Exception as e:
        print(f"Lỗi khi lấy open positions: {e}")
        return []

def get_unique_pair_ids():
    try:
        positions = get_open_positions()
        pair_ids = set()
        for position in positions:
            pair_id = position.get('pair_id')
            if pair_id:
                pair_ids.add(pair_id)
        return list(pair_ids)
    except Exception as e:
        print(f"Lỗi khi lấy unique pair IDs: {e}")
        return []

# =====================
# 3. Z-score logic
# =====================

def calculate_current_zscore(position):
    try:
        pair1 = position.get('pair1')
        pair2 = position.get('pair2')
        if not pair1 or not pair2:
            return None
        df1 = get_data(pair1, interval='1h', limit=100)
        df2 = get_data(pair2, interval='1h', limit=100)
        if df1 is None or df2 is None:
            return None
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

# =====================
# 4. Position logic
# =====================

def execute_trade_simulation(signal, pair, account_balance):
    global simulation_balance
    symbol = signal['symbol']
    side = signal['signal_type']
    z_score = signal['z_score']
    pair_id = signal.get('pair_id')
    rank = 10
    if pair_id:
        try:
            hourly_rankings = supabase_manager.get_hourly_rankings()
            for ranking in hourly_rankings:
                if ranking.get('pair_id') == pair_id:
                    rank = ranking.get('current_rank', 10)
                    break
        except Exception as e:
            print(f"⚠️ Không lấy được rank cho pair_id {pair_id}: {e}")
    print(f"🚨 SIMULATION SIGNAL: {side} {symbol} (rank {rank}, z={z_score:.2f})")
    if check_existing_position(symbol):
        print(f"⚠️ Đã có position cho {symbol}, bỏ qua signal")
        return None
    capital = get_capital_by_rank(rank, account_balance)
    if capital == 0:
        print(f"⚠️ Không đủ vốn cho rank {rank}")
        return None
    if capital > simulation_balance or simulation_balance <= 0:
        print(f"⚠️ Không đủ balance: cần {capital:.2f}, có {simulation_balance:.2f}")
        return None
    entry_price = get_current_price(symbol)
    if entry_price is None:
        return None
    quantity = capital / entry_price
    # --- TP/SL logic ---
    if side == 'BUY':
        tp = entry_price * 1.10
        sl = entry_price * 0.90
    elif side == 'SELL':
        tp = entry_price * 0.90
        sl = entry_price * 1.10
    else:
        tp = None
        sl = None
    try:
        simulation_balance -= capital
        order_id = f"SIM_{int(time.time())}_{symbol}"
        position_data = {
            'pair_id': pair['id'],
            'symbol': symbol,
            'entry_price': float(entry_price),
            'quantity': float(quantity),
            'status': 'OPEN',
            'entry_time': datetime.now().isoformat(),
            'binance_order_id': order_id,
            'pnl': 0.0,
            'tp': float(tp) if tp is not None else None,
            'sl': float(sl) if sl is not None else None,
            'z_score': float(z_score),
            'signal_type': side,  # Thêm trường này
        }
        saved_position = supabase_manager.save_position(position_data)
        if saved_position:
            print(f"✅ SIMULATION EXECUTE: {side} {symbol}")
            print(f"   - Vốn: {capital:.2f} USD")
            print(f"   - Giá: {entry_price:.4f}")
            print(f"   - Số lượng: {quantity:.4f}")
            print(f"   - Z-score: {z_score:.2f}")
            print(f"   - Balance còn lại: {simulation_balance:.2f} USD")
            print(f"   - Thời gian: {datetime.now().strftime('%H:%M:%S')}")
            print(f"   - Position ID: {saved_position[0]['id'] if saved_position else 'N/A'}")
        else:
            print(f"❌ Lỗi khi lưu position cho {symbol}")
            simulation_balance += capital
            return None
        return {
            'orderId': order_id,
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'price': entry_price,
            'capital': capital
        }
    except Exception as e:
        print(f"❌ Lỗi khi execute simulation trade: {e}")
        simulation_balance += capital
        return None

# --- 1. Kiểm tra điều kiện đóng lệnh ---
def should_close_position_tp_sl(position, current_price):
    tp = position.get('tp')
    sl = position.get('sl')
    if tp is not None and current_price >= tp:
        return True, 'TP hit'
    if sl is not None and current_price <= sl:
        return True, 'SL hit'
    return False, None

def should_close_pair_zscore(pair_id):
    try:
        pair_positions = supabase_manager.get_open_positions_by_pair_id(pair_id)
        if len(pair_positions) < 2:
            return False
        current_zscore = calculate_current_zscore(pair_positions[0])
        if current_zscore is None:
            return False
        entry_zscore = pair_positions[0].get('z_score', 0)
        if entry_zscore > 0 and current_zscore < 0.5:
            return True
        elif entry_zscore < 0 and current_zscore > -0.5:
            return True
        return False
    except Exception as e:
        print(f"Lỗi khi kiểm tra z-score pair: {e}")
        return False

# --- 2. Thực thi đóng lệnh ---
def close_position_simulation(position, exit_price, reason):
    global simulation_balance
    try:
        entry_price = position['entry_price']
        quantity = position['quantity']
        pnl = (exit_price - entry_price) * quantity
        capital_used = entry_price * quantity
        simulation_balance += capital_used + pnl
        supabase_manager.update_position_status(position['id'], 'CLOSED', pnl=pnl, reason=reason)
        print(f"✅ Đã đóng position {position['symbol']} (SIMULATION)")
        print(f"   - Entry: {entry_price:.4f}")
        print(f"   - Exit: {exit_price:.4f}")
        print(f"   - PnL: {pnl:.2f} USD")
        print(f"   - Balance mới: {simulation_balance:.2f} USD")
        print(f"   - Reason: {reason}")
        return {
            'symbol': position['symbol'],
            'entry_price': entry_price,
            'exit_price': exit_price,
            'pnl': pnl,
            'balance': simulation_balance,
            'reason': reason
        }
    except Exception as e:
        print(f"❌ Lỗi khi đóng position simulation: {e}")
        return None

def close_pair_positions(pair_id):
    try:
        pair_positions = supabase_manager.get_open_positions_by_pair_id(pair_id)
        if len(pair_positions) < 2:
            print(f"⚠️ Không đủ positions để đóng cho pair {pair_id}")
            return False
        print(f"🔄 Đóng {len(pair_positions)} positions cho pair {pair_id} (theo z-score)...")
        total_pnl = 0
        closed_count = 0
        for position in pair_positions:
            try:
                exit_price = get_current_price(position['symbol'])
                if exit_price is None:
                    continue
                result = close_position_simulation(position, exit_price, reason='Z-score mean reversion')
                if result:
                    closed_count += 1
                    total_pnl += result['pnl']
            except Exception as e:
                print(f"❌ Lỗi khi đóng position {position['symbol']}: {e}")
        print(f"✅ Đã đóng {closed_count}/{len(pair_positions)} positions cho pair {pair_id}")
        print(f"💰 Total PnL: {total_pnl:.2f} USD")
        return closed_count == len(pair_positions)
    except Exception as e:
        print(f"❌ Lỗi khi đóng pair positions: {e}")
        return False

# =====================
# 5. Monitor logic
# =====================

def monitor_and_execute_trades_simulation():
    print("🔄 Bắt đầu monitor và execute trades (SIMULATION)...")
    executed_signals = set()
    while True:
        try:
            account_balance = get_simulation_balance()
            print(f"💰 Simulation balance: {account_balance:.2f} USD")
            if account_balance <= 0:
                print("⚠️ Hết vốn simulation, không execute thêm trades")
                time.sleep(60)
                continue
            recent_time = datetime.now() - timedelta(minutes=5)
            signals = supabase_manager.get_recent_signals(recent_time)
            if signals:
                print(f"📊 Tìm thấy {len(signals)} signals mới trong 5 phút")
                executed_count = 0
                # Giả sử signals là list các dict đã lấy từ DB
                signals_by_pair_time = defaultdict(list)
                for signal in signals:
                    key = (signal['pair_id'], signal['timestamp'])
                    signals_by_pair_time[key].append(signal)

                for (pair_id, timestamp), group in signals_by_pair_time.items():
                    if len(group) == 2:
                        for signal in group:
                            # Gọi execute_trade_simulation(signal, pair, account_balance)
                            signal_id = signal.get('id')
                            if signal_id in executed_signals:
                                continue
                            pair = supabase_manager.get_pair_by_id(pair_id)
                            if not pair:
                                print(f"⚠️ Không tìm thấy pair cho pair_id {pair_id}")
                                continue
                            # Lấy rank từ hourly_rankings
                            rank = 10
                            try:
                                hourly_rankings = supabase_manager.get_hourly_rankings()
                                for ranking in hourly_rankings:
                                    if ranking.get('pair_id') == pair_id:
                                        rank = ranking.get('current_rank', 10)
                                        break
                            except Exception as e:
                                print(f"⚠️ Không lấy được rank cho pair_id {pair_id}: {e}")
                            required_capital = get_capital_by_rank(rank, account_balance)
                            if required_capital > account_balance:
                                print(f"⚠️ Không đủ vốn cho signal {signal_id}: cần {required_capital:.2f}, có {account_balance:.2f}")
                                continue
                            result = execute_trade_simulation(signal, pair, account_balance)
                            if result:
                                executed_signals.add(signal_id)
                                executed_count += 1
                                print(f"✅ Đã execute signal {signal_id}")
                                account_balance = get_simulation_balance()
                                print(f"💰 Balance còn lại: {account_balance:.2f} USD")
                                if account_balance <= 0:
                                    print("⚠️ Hết vốn, dừng execute trades")
                                    break
                            else:
                                print(f"❌ Không thể execute signal {signal_id}")
                    else:
                        print(f"⚠️ Bỏ qua pair {pair_id} tại {timestamp} vì không đủ 2 signal")
                if executed_count > 0:
                    print(f"🎯 Đã execute {executed_count} signals mới")
                else:
                    print("📊 Không có signals mới để execute")
            else:
                print("📊 Không có signals mới trong 5 phút")
            time.sleep(60)
        except KeyboardInterrupt:
            print("\n⏹️ Dừng monitor trades...")
            break
        except Exception as e:
            print(f"❌ Lỗi trong monitor loop: {e}")
            time.sleep(60)

# --- 3. Monitor logic ---
def monitor_and_close_positions():
    print("🔍 MONITORING POSITIONS FOR CLOSING (REALTIME)...")
    while True:
        try:
            pair_ids = get_unique_pair_ids()
            if not pair_ids:
                # Không có open positions, sleep 5 phút rồi kiểm tra lại
                time.sleep(300)
                continue
            closed_positions = []
            for pair_id in pair_ids:
                try:
                    # 1. Đóng từng lệnh nếu chạm TP/SL
                    pair_positions = supabase_manager.get_open_positions_by_pair_id(pair_id)
                    for position in pair_positions:
                        if position.get('status') != 'OPEN':
                            continue
                        current_price = get_current_price(position['symbol'])
                        if current_price is None:
                            continue
                        should_close, reason = should_close_position_tp_sl(position, current_price)
                        if should_close:
                            result = close_position_simulation(position, current_price, reason)
                            if result:
                                closed_positions.append(result)
                    # Lấy lại positions sau khi có thể đã đóng bớt
                    pair_positions = supabase_manager.get_open_positions_by_pair_id(pair_id)
                    # 2. Đóng cả cặp nếu còn đủ 2 lệnh và đạt điều kiện z-score
                    if len(pair_positions) >= 2 and should_close_pair_zscore(pair_id):
                        # Lấy lại positions mới nhất trước khi đóng cặp
                        pair_positions = supabase_manager.get_open_positions_by_pair_id(pair_id)
                        success = close_pair_positions(pair_id)
                        if success:
                            # Lấy lại positions vừa đóng để append vào closed_positions
                            closed = [p for p in pair_positions if p.get('status') == 'CLOSED']
                            closed_positions.extend(closed)
                    # 3. Nếu chỉ còn 1 lệnh mở, kiểm tra z-score, nếu đạt thì đóng luôn lệnh đó
                    pair_positions = supabase_manager.get_open_positions_by_pair_id(pair_id)
                    if len(pair_positions) == 1:
                        position = pair_positions[0]
                        current_zscore = calculate_current_zscore(position)
                        entry_zscore = position.get('z_score', 0)
                        # Chỉ đóng lệnh nếu cả hai đều là số thực
                        if (
                            entry_zscore is not None and current_zscore is not None
                            and isinstance(entry_zscore, (float, int))
                            and isinstance(current_zscore, (float, int))
                        ):
                            if entry_zscore > 0 and current_zscore < 0.5:
                                result = close_position_simulation(position, get_current_price(position['symbol']), reason='Z-score mean reversion (1 leg)')
                                if result:
                                    closed_positions.append(result)
                            elif entry_zscore < 0 and current_zscore > -0.5:
                                result = close_position_simulation(position, get_current_price(position['symbol']), reason='Z-score mean reversion (1 leg)')
                                if result:
                                    closed_positions.append(result)
                        else:
                            print(f"⚠️ Không đủ dữ liệu z-score để đóng lệnh đơn cho position {position.get('id')}, entry_zscore={entry_zscore}, current_zscore={current_zscore}")
                except Exception as e:
                    print(f"❌ Lỗi khi monitor pair {pair_id}: {e}")
            if closed_positions:
                print(f"✅ Đã đóng {len(closed_positions)} positions")
            else:
                print("📊 Không có positions nào cần đóng")
            time.sleep(2)  # Sleep ngắn khi có open position
        except Exception as e:
            print(f"❌ Lỗi trong monitor_and_close_positions: {e}")
            time.sleep(2)

# =====================
# 6. Main entrypoint
# =====================

def main():
    print("🎯 Trade Executor SIMULATION - $100 Virtual Account")
    print("=" * 60)
    global simulation_balance
    simulation_balance = SIMULATION_BALANCE
    print(f"💰 Khởi tạo simulation account: ${simulation_balance:.2f}")
    monitor_and_execute_trades_simulation()

if __name__ == "__main__":
    main() 