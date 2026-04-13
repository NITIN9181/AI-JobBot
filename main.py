import argparse
import logging
import sys
import traceback
from datetime import datetime
from typing import Optional

from config import get_config
from modules.scraper import scrape_all_jobs
from modules.filter_engine import filter_jobs, remove_duplicates
from modules.deduplicator import deduplicate_with_history
from modules.exporter import export_to_csv, export_latest_csv, display_terminal_summary
from modules.scheduler import log_run, run_once_now, start_scheduler

# Set up logging
logger = logging.getLogger("JobBot.Main")

def print_banner():
    """Prints the startup banner."""
    banner = """
+-----------------------------------+
|            JobBot v1.0            |
|   Remote Job Search Automation    |
+-----------------------------------+
    """
    print(banner)

def run_job_search(test_mode: bool = False):
    """
    Orchestrates the entire job search pipeline.
    
    Args:
        test_mode: If True, limit the number of results per site to 5.
    """
    try:
        # Step 1 — Load Config
        config = get_config()
        if test_mode:
            config["results_per_site"] = 5
            logger.info("Running in TEST MODE (results_per_site=5)")
        
        logger.info("Configuration loaded successfully")

        # Step 2 — Scrape Jobs
        raw_jobs = scrape_all_jobs(config)
        logger.info(f"Scraped {len(raw_jobs)} raw jobs")
        
        if raw_jobs.empty:
            logger.warning("No jobs found. Exiting gracefully.")
            log_run("success", 0, 0, "No jobs found during scrape")
            return

        # Step 3 — Filter Jobs
        filtered_jobs = filter_jobs(raw_jobs, config)
        filtered_jobs = remove_duplicates(filtered_jobs)
        logger.info(f"Filtered down to {len(filtered_jobs)} matching jobs")

        # Step 4 — Deduplicate Against History
        new_jobs = deduplicate_with_history(filtered_jobs)
        logger.info(f"{len(new_jobs)} new jobs (not seen before)")

        # Step 5 — Export Results
        if not new_jobs.empty:
            export_to_csv(new_jobs)
            export_latest_csv(new_jobs)
            display_terminal_summary(new_jobs)
        else:
            logger.info("No new jobs to export.")

        # Step 6 — Log Run
        log_run("success", len(raw_jobs), len(new_jobs))
        
    except Exception as e:
        error_msg = f"Error during job search: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        log_run("failure", 0, 0, error_msg)
        if not test_mode and "--schedule" not in sys.argv:
             # In manual runs, we might want to see the error clearly
             print(f"\n[CRITICAL ERROR] {e}")

def main():
    """Parsing arguments and initiating the requested mode."""
    parser = argparse.ArgumentParser(description="JobBot - Remote Job Search Automation")
    parser.add_argument("--now", action="store_true", help="Run the job search immediately once")
    parser.add_argument("--schedule", action="store_true", help="Start the daily scheduler")
    parser.add_argument("--test", action="store_true", help="Run with test config (only 5 results per site)")
    
    args = parser.parse_args()
    
    print_banner()
    
    if args.test:
        run_job_search(test_mode=True)
    elif args.schedule:
        # Get run time from config or default to 09:00
        config = get_config()
        run_time = config.get("scheduler_time", "09:00")
        start_scheduler(lambda: run_job_search(), run_time=run_time)
    elif args.now:
        run_once_now(lambda: run_job_search())
    else:
        # Default behavior: run once immediately
        run_once_now(lambda: run_job_search())

if __name__ == "__main__":
    main()
