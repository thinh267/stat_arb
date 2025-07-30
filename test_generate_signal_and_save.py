from core.signal_generator import generate_signals_for_top_pairs
from core.supabase_manager import SupabaseManager

if __name__ == "__main__":
    print("=== TEST GENERATE SIGNAL AND SAVE TO SUPABASE ===")
    # Sinh signals cho top pairs (timeframe mặc định 1h)
    signals = generate_signals_for_top_pairs(timeframe="1h")
    print(f"Tổng số signals sinh ra: {len(signals)}")
    if signals:
        supabase_manager = SupabaseManager()
        success = supabase_manager.save_pair_signals(signals)
        if success:
            print("✅ Đã lưu signals lên Supabase thành công!")
        else:
            print("❌ Lỗi khi lưu signals lên Supabase!")
    else:
        print("❌ Không có signal nào để lưu!") 