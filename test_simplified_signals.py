#!/usr/bin/env python3
"""
Test script cho simplified signal generation:
- Z-score > 2.5 hoặc < -2.5
- Bollinger Bands breakout (dưới → LONG, trên → SHORT)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.signal_generator import generate_signals_for_top_pairs

def test_simplified_signals():
    """Test simplified signal generation logic"""
    print("🚀 TESTING SIMPLIFIED SIGNAL GENERATION")
    print("=" * 80)
    print("📋 Logic:")
    print("   1️⃣ Z-score threshold: |z| >= 2.5")
    print("   2️⃣ Bollinger Bands breakout:")
    print("      🟢 Price < Lower Band → LONG")
    print("      🔴 Price > Upper Band → SHORT")
    print("      ⚪ Price trong bands → Skip")
    print("=" * 80)
    
    # Test với timeframe 1h
    signals = generate_signals_for_top_pairs(timeframe="1h")
    
    if signals:
        print(f"\n✅ Generated {len(signals)} signals với logic mới!")
        
        # Phân tích kết quả
        buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
        sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
        
        print(f"\n📊 PHÂN TÍCH SIGNALS:")
        print(f"   🟢 BUY signals: {len(buy_signals)}")
        print(f"   🔴 SELL signals: {len(sell_signals)}")
        
        # Show top signals
        print(f"\n🏆 TOP SIGNALS (sorted by |z-score|):")
        sorted_signals = sorted(signals, key=lambda x: abs(x['z_score']), reverse=True)
        
        for i, signal in enumerate(sorted_signals[:5], 1):
            symbol = signal['symbol']
            signal_type = signal['signal_type']
            z_score = signal['z_score']
            reason = signal['signal_reason']
            entry = signal['entry']
            
            emoji = "🟢" if signal_type == "BUY" else "🔴"
            print(f"   {i}. {emoji} {symbol} | {signal_type} @ {entry} | Z: {z_score:.3f}")
            print(f"      📝 {reason}")
    else:
        print("❌ Không có signals nào được tạo")
        print("💡 Có thể cần:")
        print("   - Kiểm tra top pairs trong database")
        print("   - Điều chỉnh Z-score threshold")
        print("   - Kiểm tra Binance API connection")

if __name__ == "__main__":
    test_simplified_signals()
