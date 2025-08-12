#!/usr/bin/env python3
"""
Test scheduler interval để đảm bảo chạy đúng 5 phút
"""

import time
from datetime import datetime

def test_5_minute_logic():
    """Test logic 5 phút"""
    SIGNAL_CHECK_INTERVAL = 5
    
    print("🕐 TESTING 5-MINUTE SCHEDULER LOGIC")
    print("=" * 50)
    print(f"SIGNAL_CHECK_INTERVAL = {SIGNAL_CHECK_INTERVAL}")
    
    # Test trigger minutes
    trigger_minutes = [i for i in range(0, 60) if i % SIGNAL_CHECK_INTERVAL == 0]
    print(f"Sẽ trigger tại các phút: {trigger_minutes}")
    
    # Test current time
    now = datetime.now()
    current_minute = now.minute
    should_trigger = (current_minute % SIGNAL_CHECK_INTERVAL == 0)
    
    print(f"\nCurrent time: {now.strftime('%H:%M:%S')}")
    print(f"Current minute: {current_minute}")
    print(f"Should trigger now: {should_trigger}")
    
    # Show next trigger times
    next_triggers = []
    for i in range(1, 4):
        next_min = current_minute + i
        if next_min >= 60:
            next_min -= 60
        if next_min % SIGNAL_CHECK_INTERVAL == 0:
            next_triggers.append(next_min)
    
    print(f"Next triggers at minutes: {next_triggers}")
    
    return should_trigger

def simulate_scheduler_loop():
    """Simulate scheduler loop for testing"""
    SIGNAL_CHECK_INTERVAL = 5
    last_minute = -1
    
    print(f"\n🔄 SIMULATING SCHEDULER LOOP (next 2 minutes)")
    print("=" * 50)
    
    start_time = time.time()
    while time.time() - start_time < 120:  # 2 phút
        now = datetime.now()
        current_minute = now.minute
        
        # Logic giống scheduler thực
        if (current_minute % SIGNAL_CHECK_INTERVAL == 0 and 
            current_minute != last_minute):
            print(f"🚀 TRIGGER at {now.strftime('%H:%M:%S')} (minute {current_minute})")
            last_minute = current_minute
            time.sleep(1)  # Giả lập signal generation
        
        time.sleep(1)  # Check mỗi giây để test
    
    print("✅ Simulation completed")

if __name__ == "__main__":
    should_trigger = test_5_minute_logic()
    
    if should_trigger:
        print("\n🟢 Scheduler SHOULD trigger right now!")
    else:
        print("\n🟡 Waiting for next 5-minute mark...")
    
    # Uncomment để test simulation
    # simulate_scheduler_loop()
    
    print(f"\n📋 SUMMARY:")
    print(f"✅ Logic: trigger mỗi 5 phút (00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)")
    print(f"✅ Force SIGNAL_CHECK_INTERVAL = 5 trong scheduler.py")
    print(f"🔄 Restart scheduler để apply changes!")
