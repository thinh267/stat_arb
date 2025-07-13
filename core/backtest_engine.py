import pandas as pd
from datetime import datetime
from core.supabase_manager import SupabaseManager
from core.data_collector import get_data

def run_backtest(signals, fee=0.0004):
    price_data = {}
    for s in signals:
        symbol = s['symbol']
        if symbol not in price_data:
            df = get_data(symbol, interval="1h", limit=2000)
            price_data[symbol] = df

    trades = []
    for signal in signals:
        symbol = signal['symbol']
        ts = pd.to_datetime(signal['timestamp'])
        side = signal['signal_type']
        z = signal['z_score']
        spread = signal['spread']
        df = price_data[symbol]
        next_bar = df[df['timestamp'] > ts].head(1)
        if next_bar.empty or len(df[df['timestamp'] <= ts]) == 0:
            continue
        entry_price = df[df['timestamp'] <= ts]['close'].iloc[-1]
        exit_price = next_bar['close'].iloc[0]
        if side == 'BUY':
            pnl = (exit_price - entry_price) / entry_price - fee
        else:
            pnl = (entry_price - exit_price) / entry_price - fee
        trades.append({
            'symbol': symbol,
            'side': side,
            'entry_time': ts,
            'entry_price': entry_price,
            'exit_time': next_bar['timestamp'].iloc[0],
            'exit_price': exit_price,
            'pnl': pnl,
            'z_score': z,
            'spread': spread
        })
    return trades

def aggregate_daily_performance(trades):
    df = pd.DataFrame(trades)
    df['date'] = df['entry_time'].dt.date
    daily = df.groupby('date')['pnl'].sum().reset_index()
    return daily

def save_daily_performance_to_db(daily_df):
    supabase_manager = SupabaseManager()
    for _, row in daily_df.iterrows():
        data = {
            'date': str(row['date']),
            'pnl': float(row['pnl'])
        }
        supabase_manager.save_daily_performance([data])

if __name__ == "__main__":
    supabase_manager = SupabaseManager()
    since_time = datetime.now() - pd.Timedelta(days=30)
    signals = supabase_manager.get_recent_signals(since_time)
    print(f"Found {len(signals)} signals for backtest.")
    trades = run_backtest(signals)
    daily_perf = aggregate_daily_performance(trades)
    print(daily_perf)
    save_daily_performance_to_db(daily_perf)
    print("Backtest & daily performance saved to DB.") 