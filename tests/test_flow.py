# test_flow.py
import time
from core.data_collector import scan_market_for_stable_pairs, reorder_pairs_by_correlation

def test_data_collector_flow():
    """Test toàn bộ flow của data collector"""
    print("🚀 BẮT ĐẦU TEST DATA COLLECTOR FLOW")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        # Test 1: Scan market for stable pairs
        print("\n📊 TEST 1: SCAN MARKET FOR STABLE PAIRS")
        print("-" * 40)
        
        pairs_data = scan_market_for_stable_pairs()
        
        if pairs_data:
            print(f"✅ Tìm thấy {len(pairs_data)} cặp stable")
            for i, pair in enumerate(pairs_data[:3]):  # Hiển thị 3 cặp đầu
                print(f"  {i+1}. {pair['pair1']} - {pair['pair2']} (Corr: {pair['correlation']:.4f})")
        else:
            print("❌ Không tìm thấy cặp nào")
        
        # Test 2: Reorder pairs by correlation
        print("\n📊 TEST 2: REORDER PAIRS BY CORRELATION")
        print("-" * 40)
        
        ranking_data = reorder_pairs_by_correlation()
        
        if ranking_data:
            print(f"✅ Cập nhật ranking cho {len(ranking_data)} cặp")
            for i, rank in enumerate(ranking_data[:3]):  # Hiển thị 3 ranking đầu
                print(f"  {i+1}. Rank {rank['current_rank']} - Corr: {rank['current_correlation']:.4f}")
        else:
            print("❌ Không có ranking data")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n⏱️  TỔNG THỜI GIAN: {total_time:.2f} giây")
        print("✅ TEST HOÀN THÀNH THÀNH CÔNG!")
        
    except Exception as e:
        print(f"❌ LỖI TRONG TEST: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_data_collector_flow() 