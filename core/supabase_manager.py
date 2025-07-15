# supabase_manager.py
from supabase import create_client, Client
from datetime import datetime
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
        try:
            result = self.client.table('daily_pairs') \
                .select('*') \
                .eq('id', pair_id) \
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Error getting pair by ID: {e}")
            return None

    def get_open_positions_by_symbol(self, symbol):
        """
        Lấy open positions theo symbol
        """
        try:
            result = self.client.table('positions') \
                .select('*') \
                .eq('symbol', symbol) \
                .eq('status', 'OPEN') \
                .execute()
            return result.data
        except Exception as e:
            print(f"Error getting open positions: {e}")
            return []

    def get_open_positions_by_pair_id(self, pair_id):
        """
        Lấy tất cả open positions theo pair_id
        """
        try:
            result = self.client.table('positions') \
                .select('*') \
                .eq('pair_id', pair_id) \
                .eq('status', 'OPEN') \
                .execute()
            return result.data
        except Exception as e:
            print(f"Error getting open positions by pair_id: {e}")
            return []

    def get_all_open_positions(self):
        """
        Lấy tất cả open positions
        """
        try:
            result = self.client.table('positions') \
                .select('*') \
                .eq('status', 'OPEN') \
                .execute()
            return result.data
        except Exception as e:
            print(f"Error getting all open positions: {e}")
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
        try:
            result = self.client.table('hourly_rankings').insert(ranking_data).execute()
            return result.data
        except Exception as e:
            print(f"Error updating hourly ranking: {e}")
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

    def save_trading_signal(self, signal_data):
        try:
            result = self.client.table('trading_signals').insert(signal_data).execute()
            return result.data
        except Exception as e:
            print(f"Error saving signal: {e}")
            return None

    def save_pair_signals(self, signals):
        """
        Lưu pair signals vào database
        """
        try:
            # Chuẩn bị data cho database
            signals_for_db = []
            for signal in signals:
                # Tìm pair_id mới nhất từ daily_pairs table
                pair_id = self.get_latest_pair_id(signal['pair1'], signal['pair2'])
                
                if pair_id is None:
                    print(f"⚠️ Bỏ qua signal cho {signal['pair1']}-{signal['pair2']} (không tìm thấy pair_id)")
                    continue
                
                db_signal = {
                    'pair_id': pair_id,
                    'symbol': signal['symbol'],  # tên coin
                    'z_score': signal['z_score'],
                    'spread': signal['spread'],
                    'signal_type': signal['signal_type'],
                    'timestamp': signal['timestamp']
                }
                signals_for_db.append(db_signal)
            
            if signals_for_db:
                result = self.client.table('trading_signals').insert(signals_for_db).execute()
                print(f"✅ Đã lưu {len(signals_for_db)} signals với pair_id mới nhất")
                return True
            else:
                print("⚠️ Không có signals nào để lưu")
                return False
                
        except Exception as e:
            print(f"Error saving pair signals: {e}")
            return False

    def save_position(self, position_data):
        try:
            result = self.client.table('positions').insert(position_data).execute()
            return result.data
        except Exception as e:
            print(f"Error saving position: {e}")
            return None

    def update_position_status(self, position_id, status, pnl=None, reason=None):
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
            print(f"Error updating position: {e}")
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

    def get_pair_id_from_pairs(self, pair1, pair2):
        """
        Lấy pair_id mới nhất từ daily_pairs table dựa trên pair1 và pair2
        """
        try:
            # Tìm pair với pair1 và pair2, lấy record mới nhất (id lớn nhất)
            result = self.client.table('daily_pairs') \
                .select('id') \
                .or_(f'pair1.eq.{pair1},pair2.eq.{pair1}') \
                .or_(f'pair1.eq.{pair2},pair2.eq.{pair2}') \
                .order('id', desc=True) \
                .limit(1) \
                .execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]['id']
            else:
                print(f"⚠️ Không tìm thấy pair_id cho {pair1}-{pair2}")
                return None
                
        except Exception as e:
            print(f"Error getting pair_id for {pair1}-{pair2}: {e}")
            return None

    def get_latest_pair_id(self, pair1, pair2):
        """
        Lấy pair_id mới nhất cho một cặp pair cụ thể
        """
        try:
            print(f"🔍 Searching for pair_id: {pair1}-{pair2}")
            
            # Tìm pair với chính xác pair1 và pair2, lấy theo id lớn nhất (mới nhất)
            result = self.client.table('daily_pairs') \
                .select('id, date, pair1, pair2') \
                .eq('pair1', pair1) \
                .eq('pair2', pair2) \
                .order('id', desc=True) \
                .limit(1) \
                .execute()
            
            print(f"📊 Query result for {pair1}-{pair2}: {len(result.data)} records")
            if result.data:
                for record in result.data:
                    print(f"   - ID: {record['id']}, Date: {record['date']}, Pair: {record['pair1']}-{record['pair2']}")
            
            if result.data and len(result.data) > 0:
                pair_id = result.data[0]['id']
                date = result.data[0]['date']
                print(f"✅ Found pair_id {pair_id} for {pair1}-{pair2} (date: {date})")
                return pair_id
            else:
                # Thử ngược lại (pair2-pair1)
                print(f"🔄 Trying reverse order: {pair2}-{pair1}")
                result = self.client.table('daily_pairs') \
                    .select('id, date, pair1, pair2') \
                    .eq('pair1', pair2) \
                    .eq('pair2', pair1) \
                    .order('id', desc=True) \
                    .limit(1) \
                    .execute()
                
                print(f"📊 Query result for {pair2}-{pair1}: {len(result.data)} records")
                if result.data:
                    for record in result.data:
                        print(f"   - ID: {record['id']}, Date: {record['date']}, Pair: {record['pair1']}-{record['pair2']}")
                
                if result.data and len(result.data) > 0:
                    pair_id = result.data[0]['id']
                    date = result.data[0]['date']
                    print(f"✅ Found pair_id {pair_id} for {pair2}-{pair1} (date: {date})")
                    return pair_id
                else:
                    print(f"⚠️ Không tìm thấy pair_id cho {pair1}-{pair2}")
                    return None
                    
        except Exception as e:
            print(f"❌ Error getting latest pair_id for {pair1}-{pair2}: {e}")
            return None 