import os
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(__file__), '.env')
print(f"Loading .env from: {env_path}")
print(f"File exists: {os.path.exists(env_path)}")
load_dotenv(dotenv_path=env_path)

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Debug print
print(f"SUPABASE_URL: '{SUPABASE_URL}'")
print(f"SUPABASE_KEY: '{SUPABASE_KEY[:10]}...' if SUPABASE_KEY else 'None'")

DATABASE_URL = os.getenv("DATABASE_URL", "")

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", 8000))

# Parse CORS_ORIGINS as a list
CORS_ORIGINS = os.getenv("CORS_ORIGINS", '"http://localhost:3000"')
CORS_ORIGINS = [origin.strip().strip('"') for origin in CORS_ORIGINS.strip('[]').split(",") if origin.strip()]

# Trading parameters
DAILY_TOP_N = int(os.getenv("DAILY_TOP_N", 10))
HOURLY_UPDATE_INTERVAL = int(os.getenv("HOURLY_UPDATE_INTERVAL", 4))
SIGNAL_CHECK_INTERVAL = int(os.getenv("SIGNAL_CHECK_INTERVAL", 15))
DAILY_LIMIT = float(os.getenv("DAILY_LIMIT", 100))
MAX_PAIRS_PER_DAY = int(os.getenv("MAX_PAIRS_PER_DAY", 10))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", 0.02))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", 0.02))

API_KEY = os.getenv("API_KEY", "")

