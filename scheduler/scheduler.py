# scheduler.py
import time
from datetime import datetime
import threading
from core.data_collector import scan_market_for_stable_pairs, reorder_pairs_by_correlation
from core.signal_generator import generate_signals_for_top_pairs
from core.supabase_manager import SupabaseManager
from config import DAILY_TOP_N, HOURLY_UPDATE_INTERVAL, SIGNAL_CHECK_INTERVAL

supabase_manager = SupabaseManager()

def daily_task():
    print(f"[Daily] {datetime.now()} - Scanning market for stable pairs...")
    scan_market_for_stable_pairs()

def hourly_task():
    print(f"[4h] {datetime.now()} - Reordering pairs by correlation...")
    reorder_pairs_by_correlation()

def signal_task():
    print(f"[Signal] {datetime.now()} - Generating trading signals...")
    for tf in ['15m', '30m']:
        generate_signals_for_top_pairs(timeframe=tf)

def run_scheduler():
    # Run daily at 9:00
    def daily_loop():
        while True:
            now = datetime.now()
            if now.hour == 9 and now.minute == 0:
                daily_task()
                time.sleep(60)
            time.sleep(30)
    # Run every 4 hours
    def hourly_loop():
        while True:
            now = datetime.now()
            if now.hour % HOURLY_UPDATE_INTERVAL == 0 and now.minute == 0:
                hourly_task()
                time.sleep(60)
            time.sleep(30)
    # Run every 15 minutes
    def signal_loop():
        while True:
            now = datetime.now()
            if now.minute % SIGNAL_CHECK_INTERVAL == 0:
                signal_task()
                time.sleep(60)
            time.sleep(30)
    threading.Thread(target=daily_loop, daemon=True).start()
    threading.Thread(target=hourly_loop, daemon=True).start()
    threading.Thread(target=signal_loop, daemon=True).start()
    print("Scheduler started. Press Ctrl+C to exit.")
    while True:
        time.sleep(60) 