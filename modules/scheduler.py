import schedule
import time
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Callable

# Set up logging for the module
logger = logging.getLogger("JobBot.Scheduler")

def get_next_run_time(run_time: str) -> str:
    """
    Calculates the time until the next scheduled run and returns a human-readable string.
    
    Args:
        run_time: Time in "HH:MM" 24-hour format.
        
    Returns:
        Human-readable next run description.
    """
    now = datetime.now()
    try:
        scheduled_time = datetime.strptime(run_time, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day
        )
    except ValueError:
        logger.error(f"Invalid run_time format: {run_time}. Expected HH:MM.")
        return "Invalid run time format"

    if scheduled_time <= now:
        scheduled_time += timedelta(days=1)

    diff = scheduled_time - now
    hours, remainder = divmod(int(diff.total_seconds()), 3600)
    minutes, _ = divmod(remainder, 60)

    time_str = scheduled_time.strftime("%I:%M %p")
    return f"Next run in {hours} hours {minutes} minutes (at {time_str})"

def log_run(status: str, jobs_found: int, jobs_matched: int, error_message: str = "", log_file: str = "logs/run_log.csv"):
    """
    Appends a summary of the job run to a CSV log file.
    
    Args:
        status: "success" or "failure".
        jobs_found: Total jobs scraped.
        jobs_matched: Total jobs matching filters.
        error_message: Error description if status is failure.
        log_file: Path to the log CSV.
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    data = {
        "run_timestamp": [run_timestamp],
        "status": [status],
        "jobs_scraped": [jobs_found],
        "jobs_matched": [jobs_matched],
        "error_message": [error_message]
    }
    df = pd.DataFrame(data)
    
    file_exists = os.path.isfile(log_file)
    df.to_csv(log_file, mode='a', index=False, header=not file_exists)
    logger.info(f"Run logged to {log_file} with status: {status}")

def run_once_now(job_function: Callable):
    """
    Executes the job function immediately and logs performance timing.
    
    Args:
        job_function: The main job bot function to execute.
    """
    logger.info("Starting manual job run execution...")
    print(f"\n{'-'*30}")
    print(f"Starting Manual Run at {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'-'*30}")
    
    start_time = datetime.now()
    
    try:
        job_function()
        status = "success"
    except Exception as e:
        logger.error(f"Error during manual run: {e}")
        status = f"failure: {str(e)}"
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info(f"Manual run finished. Status: {status}. Total execution time: {duration}")
    print(f"{'-'*30}")
    print(f"Run Finished. Duration: {duration}")
    print(f"{'-'*30}\n")

def start_scheduler(job_function: Callable, run_time: str = "09:00"):
    """
    Schedules job_function to run daily at the specified time and enters a check loop.
    
    Args:
        job_function: The main job bot function to execute.
        run_time: Time in "HH:MM" 24-hour format.
    """
    schedule.every().day.at(run_time).do(job_function)
    
    logger.info(f"JobBot scheduled to run daily at {run_time}")
    print(f"JobBot scheduled to run daily at {run_time}")
    print(f"Next run info: {get_next_run_time(run_time)}")
    print("Press Ctrl+C to stop the scheduler.")
    
    last_heartbeat = datetime.now()
    
    try:
        while True:
            schedule.run_pending()
            
            # Use heartbeat logic (every 30 mins)
            now = datetime.now()
            if now - last_heartbeat >= timedelta(minutes=30):
                next_run_info = get_next_run_time(run_time)
                msg = f"JobBot is running... {next_run_info}"
                logger.info(msg)
                print(msg)
                last_heartbeat = now
                
            time.sleep(60) # Check every 60 seconds
    except KeyboardInterrupt:
        logger.info("Shutting down JobBot scheduler...")
        print("\nShutting down JobBot scheduler...")

if __name__ == "__main__":
    # Test Block: Automatic Verification
    print("Running automatic verification for scheduler.py module...")
    
    # 1. Test get_next_run_time
    print(f"Test get_next_run_time('23:59'): {get_next_run_time('23:59')}")
    
    # 2. Test log_run
    test_log = "logs/test_run_log.csv"
    if os.path.exists(test_log):
        os.remove(test_log)
    
    log_run("success", 10, 5, log_file=test_log)
    if os.path.exists(test_log):
        print(f"SUCCESS: {test_log} created.")
        df = pd.read_csv(test_log)
        print(f"Log content:\n{df}")
    else:
        print(f"FAILURE: {test_log} not created.")
    
    # 3. Test run_once_now
    def dummy_job():
        print("Executing dummy job...")
        time.sleep(1)
        print("Dummy job complete.")
        
    run_once_now(dummy_job)
    
    print("\nVerification complete.")
