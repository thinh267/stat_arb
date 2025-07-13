<<<<<<< HEAD
# STAT ARB Project

## Tổng quan
Dự án này là một hệ thống giao dịch thống kê (Statistical Arbitrage) bao gồm backend Python và frontend React. Backend thực hiện thu thập dữ liệu, sinh tín hiệu, backtest, thực thi lệnh và quản lý lịch trình. Frontend cung cấp dashboard trực quan hóa hiệu suất và tín hiệu giao dịch.

## Cấu trúc thư mục
- `api/`: API backend (Flask hoặc FastAPI)
- `core/`: Thành phần cốt lõi (backtest, thu thập dữ liệu, sinh tín hiệu, thực thi lệnh, quản lý supabase)
- `scheduler/`: Lên lịch các tác vụ tự động
- `tests/`: Unit test cho backend
- `trading-dashboard/`: Frontend React
- `config.py`: Cấu hình chung
- `main.py`: Entry point backend
- `.env`: Biến môi trường
- `requirements.txt`: Dependencies Python

## Hướng dẫn cài đặt

### Backend
1. Cài đặt Python >= 3.8
2. Tạo virtualenv và cài dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # hoặc venv\Scripts\activate trên Windows
   pip install -r requirements.txt
   ```
3. Tạo file `.env` từ mẫu và điền thông tin cấu hình.
4. Chạy backend:
   ```bash
   python main.py
   ```

### Frontend
1. Vào thư mục `trading-dashboard`:
   ```bash
   cd trading-dashboard
   npm install
   npm start
   ```

## Đóng góp
Pull request và issue luôn được hoan nghênh! 
=======
# stat_arb
statistical arbitrage
>>>>>>> afd00d8e5e93cd4b3f15c86b76594482c4564fc1
