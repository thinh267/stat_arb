import os
from datetime import datetime, timedelta
from core.supabase_manager import SupabaseManager

# Cấu hình ngày lấy signal (hôm qua)
yesterday = datetime.now() - timedelta(days=1)
start_time = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
end_time = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)

supabase_manager = SupabaseManager()

# Lấy tất cả signals trong ngày hôm qua
def get_signals_for_yesterday():
    try:
        result = supabase_manager.client.table('trading_signals') \
            .select('*') \
            .gte('timestamp', start_time.isoformat()) \
            .lte('timestamp', end_time.isoformat()) \
            .execute()
        return result.data if result.data else []
    except Exception as e:
        print(f"❌ Lỗi khi lấy signals: {e}")
        return []

def signal_to_position(signal):
    # Map các trường từ signal sang position
    position_data = {
        'pair_id': signal.get('pair_id'),
        'symbol': signal.get('symbol'),
        'entry_price': signal.get('entry_price', 0),  # Nếu signal không có entry_price thì để 0
        'quantity': signal.get('quantity', 0),        # Nếu signal không có quantity thì để 0
        'status': 'OPEN',
        'pnl': 0,
        'entry_time': signal.get('timestamp'),
        'exit_time': None,
        'binance_order_id': f"DEBUG_{signal.get('id', '')}",
        'tp': signal.get('tp'),
        'sl': signal.get('sl'),
        'reason': 'debug_import',
        'z_score': signal.get('z_score'),
    }
    return position_data

def insert_signals_to_positions(signals):
    inserted = 0
    for signal in signals:
        # Kiểm tra đã có position cho signal này chưa (theo pair_id, symbol, entry_time)
        try:
            existing = supabase_manager.client.table('positions') \
                .select('id') \
                .eq('pair_id', signal.get('pair_id')) \
                .eq('symbol', signal.get('symbol')) \
                .eq('entry_time', signal.get('timestamp')) \
                .execute()
            if existing.data and len(existing.data) > 0:
                print(f"⚠️ Đã có position cho signal id={signal.get('id')}, bỏ qua!")
                continue
            position_data = signal_to_position(signal)
            result = supabase_manager.save_position(position_data)
            if result:
                print(f"✅ Đã insert position cho signal id={signal.get('id')}")
                inserted += 1
            else:
                print(f"❌ Lỗi khi insert position cho signal id={signal.get('id')}")
        except Exception as e:
            print(f"❌ Lỗi khi kiểm tra/insert position: {e}")
    print(f"\nTổng số position đã insert: {inserted}")

def main():
    print(f"[DEBUG] Đang lấy signals ngày: {start_time.date()}")
    signals = get_signals_for_yesterday()
    print(f"Tổng số signals lấy được: {len(signals)}")
    insert_signals_to_positions(signals)

if __name__ == "__main__":
    main() 