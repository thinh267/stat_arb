# 🚀 CHEAT SHEET DEPLOY & QUẢN LÝ BACKEND PYTHON TRÊN VPS

---

## 1. Deploy code lần đầu lên VPS

### A. Cài đặt các phần mềm cần thiết
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv git supervisor
```

### B. Clone code từ GitHub
```bash
cd /root
git clone <YOUR_REPO_URL> stat_arb
cd stat_arb
```
> Thay `<YOUR_REPO_URL>` bằng link repo của bạn.

### C. Tạo virtual environment và cài requirements
```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### D. Tạo file .env
```bash
nano .env
# (Hoặc dùng script tạo .env như đã hướng dẫn ở trên)
```

---

## 2. Cấu hình Supervisor để chạy backend

### A. Tạo file cấu hình Supervisor
```bash
sudo nano /etc/supervisor/conf.d/stat-arb.conf
```
**Nội dung mẫu:**
```ini
[program:stat-arb-backend]
command=/root/stat_arb/venv/bin/python /root/stat_arb/main.py
directory=/root/stat_arb
user=root
autostart=true
autorestart=true
stderr_logfile=/var/log/stat-arb-backend.err.log
stdout_logfile=/var/log/stat-arb-backend.out.log
environment=PYTHONPATH="/root/stat_arb"
```

### B. Nạp lại cấu hình và khởi động backend
```bash
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start stat-arb-backend
```

---

## 3. Update code & restart backend khi có thay đổi

### A. Kéo code mới nhất từ GitHub
```bash
cd /root/stat_arb
git pull origin main
```

### B. (Nếu có thay đổi requirements)
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### C. Khởi động lại backend
```bash
sudo supervisorctl restart stat-arb-backend
```

---

## 4. Các lệnh quản lý Supervisor thường dùng

| Lệnh Supervisor                        | Ý nghĩa                                 |
|----------------------------------------|-----------------------------------------|
| `sudo supervisorctl status`            | Xem trạng thái các process              |
| `sudo supervisorctl start <name>`      | Khởi động process                       |
| `sudo supervisorctl stop <name>`       | Dừng process                            |
| `sudo supervisorctl restart <name>`    | Khởi động lại process                   |
| `sudo supervisorctl reread`            | Đọc lại cấu hình mới                    |
| `sudo supervisorctl update`            | Áp dụng cấu hình mới                    |

> `<name>` ở đây là `stat-arb-backend` (theo ví dụ trên).

---

## 5. Xem log backend

```bash
tail -f /var/log/stat-arb-backend.out.log
tail -f /var/log/stat-arb-backend.err.log
```

---

## 6. Một số lệnh Linux hữu ích

- **Kiểm tra tiến trình python đang chạy:**
  ```bash
  ps aux | grep python
  ```
- **Kiểm tra port (nếu backend mở port):**
  ```bash
  sudo netstat -tulnp | grep 5000
  ```
- **Kiểm tra file trong thư mục:**
  ```bash
  ls -l
  ```

---

## 7. Khi muốn chỉnh sửa file .env hoặc code
- Sửa file `.env`:
  ```bash
  nano /root/stat_arb/.env
  sudo supervisorctl restart stat-arb-backend
  ```
- Sửa code, rồi:
  ```bash
  git pull origin main
  sudo supervisorctl restart stat-arb-backend
  ```

---

## 8. Khi muốn deploy project mới
- Lặp lại các bước từ **1A** đến **2B** với tên thư mục và tên process khác.

---

**TIP:**  
- Luôn chạy backend bằng supervisor để tự động restart khi lỗi hoặc khi VPS reboot.
- Không up file `.env` lên GitHub, chỉ tạo/copy lên VPS.

---

**Lưu lại cheat sheet này, bạn sẽ deploy và quản lý backend Python trên VPS cực kỳ dễ dàng!**
Nếu cần mẫu file, script tự động, hoặc gặp lỗi cụ thể, chỉ cần hỏi nhé! 🚀 