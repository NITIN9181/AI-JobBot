import pandas as pd
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Set up logging for this module
logger = logging.getLogger(__name__)

def update_history(jobs: pd.DataFrame, history_file: str = "output/job_history.csv"):
    """
    Appends the given jobs to the history CSV for future runs.
    
    Args:
        jobs (pd.DataFrame): New jobs to add to history.
        history_file (str): Path to the history CSV file.
    """
    if jobs.empty:
        return

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(history_file), exist_ok=True)

    # Add first_seen_date if it doesn't exist
    if 'first_seen_date' not in jobs.columns:
        jobs = jobs.copy()
        jobs['first_seen_date'] = datetime.now().strftime("%Y-%m-%d")

    # Append to CSV
    file_exists = os.path.isfile(history_file)
    try:
        jobs.to_csv(history_file, mode='a', index=False, header=not file_exists)
        logger.info(f"Added {len(jobs)} new job(s) to history at {history_file}.")
    except Exception as e:
        logger.error(f"Failed to update history file: {e}")

def deduplicate_with_history(new_jobs: pd.DataFrame, history_file: str = "output/job_history.csv") -> pd.DataFrame:
    """
    Checks new jobs against ALL previously found jobs to avoid duplicates.
    
    Args:
        new_jobs (pd.DataFrame): DataFrame of jobs from current scrape.
        history_file (str): Path to the history CSV file.
        
    Returns:
        pd.DataFrame: DataFrame containing only truly new jobs.
    """
    if new_jobs.empty:
        return new_jobs

    if not os.path.exists(history_file):
        logger.info("No history file found. All jobs are considered new.")
        update_history(new_jobs, history_file)
        return new_jobs

    try:
        history_df = pd.read_csv(history_file)
        if history_df.empty:
            update_history(new_jobs, history_file)
            return new_jobs
    except Exception as e:
        logger.warning(f"Could not read history file ({e}). Treating all jobs as new.")
        update_history(new_jobs, history_file)
        return new_jobs

    # Normalization for robust matching
    def normalize_series(series):
        return series.fillna('').astype(str).str.lower().str.strip()

    # Create keys for matching
    history_urls = set(history_df['job_url'].dropna().unique()) if 'job_url' in history_df.columns else set()
    
    # Composite key: title + company
    history_df['comp_key'] = normalize_series(history_df['title']) + "|" + normalize_series(history_df['company'])
    history_comp_keys = set(history_df['comp_key'].unique())

    # Filter new jobs
    def is_new(row):
        # 1. Check URL
        url = row.get('job_url')
        if pd.notnull(url) and url in history_urls:
            return False
            
        # 2. Check Composite Key
        title = str(row.get('title', ''))
        company = str(row.get('company', ''))
        comp_key = title.lower().strip() + "|" + company.lower().strip()
        if comp_key in history_comp_keys:
            return False
            
        return True

    truly_new_jobs = new_jobs[new_jobs.apply(is_new, axis=1)].copy()
    
    removed_count = len(new_jobs) - len(truly_new_jobs)
    if removed_count > 0:
        logger.info(f"Deduplicator: Filtered out {removed_count} jobs already in history.")

    # Update history with the truly new jobs
    if not truly_new_jobs.empty:
        update_history(truly_new_jobs, history_file)

    return truly_new_jobs

def get_history_stats(history_file: str = "output/job_history.csv") -> dict:
    """
    Returns statistics about the jobs found across all runs.
    
    Returns:
        dict: Stats including total jobs, jobs today, jobs this week, and top companies.
    """
    stats = {
        "total_jobs_seen": 0,
        "jobs_today": 0,
        "jobs_this_week": 0,
        "top_companies": {}
    }

    if not os.path.exists(history_file):
        return stats

    try:
        df = pd.read_csv(history_file)
        if df.empty:
            return stats
            
        stats["total_jobs_seen"] = len(df)
        
        # Date stats
        if 'first_seen_date' in df.columns:
            df['first_seen_date'] = pd.to_datetime(df['first_seen_date'], errors='coerce')
            today = datetime.now().date()
            week_ago = today - timedelta(days=7)
            
            stats["jobs_today"] = len(df[df['first_seen_date'].dt.date == today])
            stats["jobs_this_week"] = len(df[df['first_seen_date'].dt.date >= week_ago])

        # Top companies
        if 'company' in df.columns:
            top_5 = df['company'].value_counts().head(5).to_dict()
            stats["top_companies"] = top_5

    except Exception as e:
        logger.error(f"Error generating history stats: {e}")

    return stats

if __name__ == "__main__":
    # AUTOMATIC VERIFICATION BLOCK
    import shutil
    
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    test_file = "output/test_history.csv"
    
    # Ensure clean state
    if os.path.exists(test_file):
        os.remove(test_file)
    
    print("\n" + "="*50)
    print("      DEDUPLICATOR AUTO-VERIFICATION")
    print("="*50)

    # 1. Test Initial Run (No history)
    print("\nTest 1: Initial Run (No History)")
    jobs_1 = pd.DataFrame([
        {"title": "Job A", "company": "Co 1", "job_url": "url1"},
        {"title": "Job B", "company": "Co 2", "job_url": "url2"}
    ])
    new_jobs_1 = deduplicate_with_history(jobs_1, test_file)
    assert len(new_jobs_1) == 2, "Should return all jobs on first run"
    assert os.path.exists(test_file), "History file should be created"

    # 2. Test Partial Duplicates
    print("\nTest 2: Partial Duplicates")
    jobs_2 = pd.DataFrame([
        {"title": "Job A", "company": "Co 1", "job_url": "url1"}, # Duplicate URL
        {"title": "JOB B", "company": " CO 2 ", "job_url": "urlX"}, # Duplicate composite (norm)
        {"title": "Job C", "company": "Co 3", "job_url": "url3"}  # New
    ])
    new_jobs_2 = deduplicate_with_history(jobs_2, test_file)
    assert len(new_jobs_2) == 1, f"Should find exactly 1 new job, found {len(new_jobs_2)}"
    assert new_jobs_2.iloc[0]['title'] == "Job C"

    # 3. Test Stats
    print("\nTest 3: History Stats")
    stats = get_history_stats(test_file)
    print(f"Stats found: {stats}")
    assert stats["total_jobs_seen"] == 3, f"Expected 3 total jobs, got {stats['total_jobs_seen']}"
    assert stats["jobs_today"] == 3
    assert "Co 1" in stats["top_companies"]

    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
        
    print("\n[SUCCESS] All deduplicator verification checks passed!")
    print("="*50 + "\n")
