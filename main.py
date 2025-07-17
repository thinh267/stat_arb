# main.py
from core.signal_generator import test_save_signal_to_db
from scheduler.scheduler import run_scheduler,signal_task
if __name__ == "__main__":  
    # signal_task()
    # run_scheduler()   
    test_save_signal_to_db()