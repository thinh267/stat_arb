import pandas as pd
from datetime import datetime, timedelta
from core.supabase_manager import SupabaseManager

def get_daily_performance_from_positions():
    """
    Tính daily performance từ positions đã đóng trong database, đúng schema mới
    """
    try:
        supabase_manager = SupabaseManager()
        closed_positions = supabase_manager.get_closed_positions()
        if not closed_positions:
            print("⚠️ Không có positions đã đóng để tính performance")
            return pd.DataFrame()
        df = pd.DataFrame(closed_positions)
        df['entry_time'] = pd.to_datetime(df['entry_time'])
        df['exit_time'] = pd.to_datetime(df['exit_time'])
        df['date'] = df['exit_time'].dt.date
        # Tính các trường cần thiết
        perf_list = []
        for date, group in df.groupby('date'):
            total_trades = len(group)
            profitable_trades = (group['pnl'] > 0).sum()
            total_pnl = group['pnl'].sum()
            win_rate = (profitable_trades / total_trades * 100) if total_trades > 0 else 0
            perf_list.append({
                'date': date,
                'total_pnl': float(total_pnl),
                'win_rate': float(win_rate),
                'total_trades': int(total_trades),
                'profitable_trades': int(profitable_trades)
            })
        daily_perf = pd.DataFrame(perf_list)
        print(f"✅ Tính daily performance từ {len(closed_positions)} positions đã đóng")
        print(f"📊 Có {len(daily_perf)} ngày có performance")
        return daily_perf
    except Exception as e:
        print(f"❌ Lỗi khi tính daily performance: {e}")
        return pd.DataFrame()

def get_simulation_performance_summary():
    """
    Lấy summary performance của simulation (không cần get_all_positions)
    """
    try:
        supabase_manager = SupabaseManager()
        closed_positions = supabase_manager.get_closed_positions()
        open_positions = supabase_manager.get_all_open_positions()
        # Tính tổng PnL
        total_pnl = sum(pos.get('pnl', 0) for pos in closed_positions)
        summary = {
            'closed_positions': len(closed_positions),
            'open_positions': len(open_positions),
            'total_realized_pnl': total_pnl,
        }
        return summary
    except Exception as e:
        print(f"❌ Lỗi khi lấy performance summary: {e}")
        return {}

def save_daily_performance_to_db(daily_df):
    """
    Lưu daily performance vào database, nếu đã có ngày đó thì update, chưa có thì insert
    """
    try:
        supabase_manager = SupabaseManager()
        for _, row in daily_df.iterrows():
            date_str = str(row['date'])
            # Kiểm tra đã có bản ghi cho ngày này chưa
            existing = supabase_manager.get_daily_performance(date_str)
            data = {
                'date': date_str,
                'total_pnl': float(row['total_pnl']),
                'win_rate': float(row['win_rate']),
                'total_trades': int(row['total_trades']),
                'profitable_trades': int(row['profitable_trades'])
            }
            if existing and len(existing) > 0:
                # Update bản ghi cũ (ghi đè)
                supabase_manager.client.table('daily_performance').update(data).eq('date', date_str).execute()
            else:
                # Insert mới
                supabase_manager.save_daily_performance([data])
        print(f"✅ Đã lưu {len(daily_df)} daily performance records")
        return True
    except Exception as e:
        print(f"❌ Lỗi khi lưu daily performance: {e}")
        return False

def run_backtest_from_positions():
    """
    Chạy backtest từ positions trong database
    """
    print("📈 RUNNING BACKTEST FROM POSITIONS")
    print("=" * 50)
    try:
        daily_perf = get_daily_performance_from_positions()
        if daily_perf.empty:
            print("⚠️ Không có data để backtest")
            return None
        save_daily_performance_to_db(daily_perf)
        print("\n📊 DAILY PERFORMANCE:")
        for _, row in daily_perf.iterrows():
            print(f"   {row['date']}: PnL = {row['total_pnl']:.4f}, Win rate = {row['win_rate']:.2f}%, Total trades = {row['total_trades']}, Profitable trades = {row['profitable_trades']}")
        return daily_perf
    except Exception as e:
        print(f"❌ Lỗi trong backtest: {e}")
        return None

if __name__ == "__main__":
    print("🎯 BACKTEST ENGINE - FROM POSITIONS")
    print("=" * 50)
    daily_perf = run_backtest_from_positions()
    if daily_perf is not None:
        print("\n✅ BACKTEST COMPLETED")
    else:
        print("\n❌ BACKTEST FAILED") 