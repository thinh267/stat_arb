# supabase_manager.py
from supabase import create_client, Client
from datetime import datetime, timedelta
from config import SUPABASE_URL, SUPABASE_KEY

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

class SupabaseManager:
    def __init__(self):
        self.client = supabase

    def save_daily_pairs(self, pairs_data):
        try:
            result = self.client.table('daily_pairs').insert(pairs_data).execute()
            return result.data
        except Exception as e:
            print(f"Error saving daily pairs: {e}")
            return None

    def get_current_top_n(self, n=10):
        try:
            result = self.client.table('daily_pairs') \
                .select('*') \
                .eq('date', datetime.now().date()) \
                .order('rank') \
                .limit(n) \
                .execute()
            return result.data
        except Exception as e:
            print(f"Error getting top pairs: {e}")
            return []

    def get_top_pairs(self, limit=10):
        """
        Lấy top pairs từ database để tạo signals
        """
        try:
            result = self.client.table('daily_pairs') \
                .select('*') \
                .eq('date', datetime.now().date()) \
                .order('rank') \
                .limit(limit) \
                .execute()
            return result.data
        except Exception as e:
            print(f"Error getting top pairs for signals: {e}")
            return []

    def get_recent_signals(self, since_time):
        """
        Lấy signals gần đây từ database
        """
        try:
            result = self.client.table('trading_signals') \
                .select('*') \
                .gte('timestamp', since_time.isoformat()) \
                .order('timestamp', desc=True) \
                .execute()
            return result.data
        except Exception as e:
            print(f"Error getting recent signals: {e}")
            return []

    def get_pair_by_id(self, pair_id):
        """
        Lấy thông tin pair theo ID
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.client.table('daily_pairs') \
                    .select('*') \
                    .eq('id', pair_id) \
                    .execute()
                return result.data[0] if result.data else None
            except Exception as e:
                print(f"Error getting pair by ID (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)  # Wait 1 second before retry
                else:
                    print(f"❌ Failed to get pair by ID {pair_id} after {max_retries} attempts")
                    return None

    def get_open_positions_by_symbol(self, symbol):
        """
        Lấy open positions theo symbol
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.client.table('positions') \
                    .select('*') \
                    .eq('symbol', symbol) \
                    .eq('status', 'OPEN') \
                    .execute()
                return result.data
            except Exception as e:
                print(f"Error getting open positions (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)  # Wait 1 second before retry
                else:
                    print(f"❌ Failed to get open positions for {symbol} after {max_retries} attempts")
                    return []

    def get_open_positions_by_pair_id(self, pair_id):
        """
        Lấy tất cả open positions theo pair_id
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.client.table('positions') \
                    .select('*') \
                    .eq('pair_id', pair_id) \
                    .eq('status', 'OPEN') \
                    .execute()
                return result.data
            except Exception as e:
                print(f"Error getting open positions by pair_id (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)  # Wait 1 second before retry
                else:
                    print(f"❌ Failed to get open positions for pair_id {pair_id} after {max_retries} attempts")
                    return []

    def get_all_open_positions(self):
        """
        Lấy tất cả open positions
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.client.table('positions') \
                    .select('*') \
                    .eq('status', 'OPEN') \
                    .execute()
                return result.data
            except Exception as e:
                print(f"Error getting all open positions (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)  # Wait 1 second before retry
                else:
                    print(f"❌ Failed to get all open positions after {max_retries} attempts")
                    return []

    def get_closed_positions(self):
        """
        Lấy tất cả closed positions
        """
        try:
            result = self.client.table('positions') \
                .select('*') \
                .eq('status', 'CLOSED') \
                .execute()
            return result.data
        except Exception as e:
            print(f"Error getting closed positions: {e}")
            return []

    def update_hourly_ranking(self, ranking_data):
        print(f"[DEBUG] Gửi dữ liệu lên hourly_rankings: {ranking_data}")
        try:
            result = self.client.table('hourly_rankings').insert(ranking_data).execute()
            print(f"[DEBUG] Kết quả insert hourly_rankings: {result}")
            return result.data
        except Exception as e:
            print(f"Error updating hourly ranking: {e}")
            print(f"[DEBUG] Lỗi khi insert hourly_rankings với dữ liệu: {ranking_data}")
            return None

    def get_hourly_rankings(self):
        """
        Lấy hourly rankings mới nhất
        """
        try:
            result = self.client.table('hourly_rankings') \
                .select('*') \
                .order('timestamp', desc=True) \
                .limit(20) \
                .execute()
            return result.data
        except Exception as e:
            print(f"Error getting hourly rankings: {e}")
            return []

    def save_pair_signals(self, signals):
        """
        Lưu signals vào database với 4 lớp confirmation tracking
        """
        try:
            signals_for_db = []
            for signal in signals:
                pair_id = self.get_latest_pair_id(signal['pair1'], signal['pair2'])
                if pair_id is None:
                    print(f"⚠️ Bỏ qua signal cho {signal['pair1']}-{signal['pair2']} (không tìm thấy pair_id)")
                    continue
                existing = self.client.table('trading_signals') \
                    .select('id') \
                    .eq('pair_id', pair_id) \
                    .eq('symbol', signal['symbol']) \
                    .eq('signal_type', signal['signal_type']) \
                    .eq('timestamp', signal['timestamp']) \
                    .execute()
                if existing.data and len(existing.data) > 0:
                    print(f"⚠️ Signal đã tồn tại cho pair_id={pair_id}, symbol={signal['symbol']}, type={signal['signal_type']}, timestamp={signal['timestamp']}, bỏ qua!")
                    continue
                db_signal = {
                    'pair_id': pair_id,
                    'symbol': signal['symbol'],
                    'z_score': signal['z_score'],
                    'spread': signal['spread'],
                    'signal_type': signal['signal_type'],
                    'timestamp': signal['timestamp'],
                    'market_trend': signal.get('market_trend'),
                    'trend_strength': signal.get('trend_strength'),
                    'tp': signal.get('tp'),
                    'sl': signal.get('sl'),
                    'entry': signal.get('entry'),
                    # Thêm 4 lớp confirmation
                    'rsi_confirmation': signal.get('rsi_confirmation', False),
                    'macd_confirmation': signal.get('macd_confirmation', False),
                    'bollinger_confirmation': signal.get('bollinger_confirmation', False),
                    'linear_confirmation': signal.get('linear_confirmation', False),
                    'total_confirmations': signal.get('confirmations', 0),
                    'confirmation_details': signal.get('confirmation_details', '')
                }
                signals_for_db.append(db_signal)
            if signals_for_db:
                result = self.client.table('trading_signals').insert(signals_for_db).execute()
                print(f"✅ Đã lưu {len(signals_for_db)} signals với 4 lớp confirmation tracking")
                return True
            else:
                print("⚠️ Không có signals nào để lưu")
                return False
        except Exception as e:
            print(f"Error saving pair signals: {e}")
            return False

    def save_position(self, position_data):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.client.table('positions').insert(position_data).execute()
                return result.data
            except Exception as e:
                print(f"Error saving position (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)  # Wait 1 second before retry
                else:
                    print(f"❌ Failed to save position after {max_retries} attempts")
                    return None

    def update_position_status(self, position_id, status, pnl=None, reason=None):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                update_data = {'status': status}
                if pnl is not None:
                    update_data['pnl'] = pnl
                if status == 'CLOSED':
                    update_data['exit_time'] = datetime.now().isoformat()
                if reason is not None:
                    update_data['reason'] = reason
                result = self.client.table('positions') \
                    .update(update_data) \
                    .eq('id', position_id) \
                    .execute()
                return result.data
            except Exception as e:
                print(f"Error updating position (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)  # Wait 1 second before retry
                else:
                    print(f"❌ Failed to update position {position_id} after {max_retries} attempts")
                    return None

    def get_daily_performance(self, date):
        try:
            result = self.client.table('daily_performance') \
                .select('*') \
                .eq('date', date) \
                .execute()
            return result.data
        except Exception as e:
            print(f"Error getting performance: {e}")
            return None

    def save_daily_performance(self, perf_data):
        try:
            result = self.client.table('daily_performance').insert(perf_data).execute()
            return result.data
        except Exception as e:
            print(f"Error saving daily performance: {e}")
            return None

    def save_correlation_stats(self, stats):
        """
        Lưu thống kê correlation vào bảng correlation_stats
        """
        from datetime import datetime
        try:
            data = {
                'date': str(datetime.now().date()),
                'count': int(stats['count']),
                'mean': float(stats['mean']),
                'median': float(stats['median']),
                'std': float(stats['std']),
                'min': float(stats['min']),
                'max': float(stats['max'])
            }
            result = self.client.table('correlation_stats').insert(data).execute()
            print("Insert result:", result)
            return result.data
        except Exception as e:
            print(f"Error saving correlation stats: {e}")
            return None

    def get_latest_pair_id(self, pair1, pair2):
        try:
            # Query cả hai chiều đúng cú pháp .or_
            result = self.client.table('daily_pairs') \
                .select('id, date, pair1, pair2') \
                .or_('and(pair1.eq.{},pair2.eq.{}),and(pair1.eq.{},pair2.eq.{})'.format(pair1, pair2, pair2, pair1)) \
                .order('id', desc=True) \
                .limit(1) \
                .execute()
            if result.data and len(result.data) > 0:
                record = result.data[0]
                print(f"✅ Found latest pair_id {record['id']} for {pair1}-{pair2} or {pair2}-{pair1} (date: {record['date']})")
                return record['id']
            else:
                print(f"⚠️ Không tìm thấy pair_id cho {pair1}-{pair2} hoặc {pair2}-{pair1}")
                return None
        except Exception as e:
            print(f"❌ Error getting latest pair_id for {pair1}-{pair2}: {e}")
            return None 