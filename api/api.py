from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from core.supabase_manager import SupabaseManager
from datetime import datetime
from config import API_HOST, API_PORT, CORS_ORIGINS

app = FastAPI(title="Trading API", version="1.0.0")

# Cấu hình CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

supabase_manager = SupabaseManager()

@app.get("/")
def root():
    return {"message": "Supabase Trading API is running!", "host": API_HOST, "port": API_PORT}

@app.get("/top-pairs")
def get_top_pairs():
    pairs = supabase_manager.get_current_top_n(10)
    return {"top_pairs": pairs}

@app.get("/all-pairs")
def get_all_pairs():
    # Lấy tất cả pairs thay vì chỉ top 10
    result = supabase_manager.client.table('daily_pairs').select('*').eq('date', datetime.now().date()).order('rank').execute()
    return {"all_pairs": result.data}

@app.get("/pairs-stats")
def get_pairs_stats():
    # Thống kê về pairs
    result = supabase_manager.client.table('daily_pairs').select('*').eq('date', datetime.now().date()).execute()
    total_pairs = len(result.data)
    high_corr_pairs = len([p for p in result.data if p['correlation'] > 0.8])
    cointegrated_pairs = len([p for p in result.data if p['is_cointegrated']])
    return {
        "total_pairs": total_pairs,
        "high_correlation_pairs": high_corr_pairs,
        "cointegrated_pairs": cointegrated_pairs,
        "date": str(datetime.now().date())
    }

@app.get("/signals")
def get_signals(limit: int = 20):
    result = supabase_manager.client.table('trading_signals').select('*').order('timestamp', desc=True).limit(limit).execute()
    return {"signals": result.data}

@app.get("/positions")
def get_positions(status: str = "OPEN"):
    result = supabase_manager.client.table('positions').select('*').eq('status', status).order('entry_time', desc=True).limit(20).execute()
    return {"positions": result.data}

@app.get("/correlation-stats")
def get_correlation_stats():
    result = supabase_manager.client.table('correlation_stats').select('*').order('date', desc=True).limit(1).execute()
    return {"correlation_stats": result.data[0] if result.data else {}}

@app.get("/performance")
def get_performance():
    result = supabase_manager.client.table('daily_performance').select('*').order('date', desc=True).limit(10).execute()
    return {"performance": result.data} 