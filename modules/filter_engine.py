import pandas as pd
import logging
from typing import List, Dict, Any, Optional

# Set up logging for this module
logger = logging.getLogger(__name__)

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate jobs based on title + company combination and job URL.
    
    Args:
        df (pd.DataFrame): The DataFrame to deduplicate.
        
    Returns:
        pd.DataFrame: A deduplicated DataFrame.
    """
    if df.empty:
        return df
        
    original_count = len(df)
    
    # Create normalized columns for robust deduplication
    df_temp = df.copy()
    df_temp['title_norm'] = df_temp['title'].fillna('').str.lower().str.strip()
    df_temp['company_norm'] = df_temp['company'].fillna('').str.lower().str.strip()
    
    # Deduplicate by Title + Company
    df = df.loc[df_temp.drop_duplicates(subset=['title_norm', 'company_norm']).index]
    
    # Deduplicate by job_url if it exists
    if 'job_url' in df.columns:
        df = df.drop_duplicates(subset=['job_url'])
    
    removed = original_count - len(df)
    if removed > 0:
        logger.info(f"Removed {removed} duplicate job(s).")
        
    return df

def sort_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts jobs by skill match count (descending) and date posted (descending).
    
    Args:
        df (pd.DataFrame): The DataFrame to sort.
        
    Returns:
        pd.DataFrame: The sorted DataFrame.
    """
    if df.empty:
        return df
        
    sort_cols = []
    ascending = []
    
    if 'skill_match_count' in df.columns:
        sort_cols.append('skill_match_count')
        ascending.append(False)
        
    if 'date_posted' in df.columns:
        # Ensure date_posted is datetime for proper sorting
        df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')
        sort_cols.append('date_posted')
        ascending.append(False)
        
    if sort_cols:
        df = df.sort_values(by=sort_cols, ascending=ascending)
        
    return df

def filter_jobs(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Applies multiple filters to job listings based on configuration.
    
    Args:
        df (pd.DataFrame): Raw job data.
        config (Dict[str, Any]): Filtering configuration.
        
    Returns:
        pd.DataFrame: Filtered and sorted DataFrame.
    """
    initial_count = len(df)
    if df.empty:
        logger.warning("Filtering skipped: Input DataFrame is empty.")
        return df
        
    logger.info(f"Starting filtering process: {initial_count} jobs in queue.")

    # --- Filter 1: Remote Only ---
    def is_remote_match(row):
        remote_val = row.get('is_remote')
        if pd.notnull(remote_val) and remote_val is True:
            return True
        
        location_val = str(row.get('location', '')).lower()
        if 'remote' in location_val:
            return True
        return False

    df = df[df.apply(is_remote_match, axis=1)].copy()
    count_remote = len(df)
    logger.info(f"Filter 1 (Remote Only): {count_remote} jobs remaining.")

    # --- Filter 2: Skills/Keywords Match ---
    skills = [s.lower() for s in config.get('skills', [])]
    if skills:
        def match_skills(row):
            text = (str(row.get('title', '')) + " " + str(row.get('description', ''))).lower()
            matches = [skill for skill in skills if skill in text]
            return matches

        df['matched_skills'] = df.apply(match_skills, axis=1)
        df['skill_match_count'] = df['matched_skills'].apply(len)
        
        # Remove jobs with 0 matching skills
        df = df[df['skill_match_count'] > 0].copy()
        
        # Format matched_skills as string for easier viewing in CSV/table if needed
        # But for now we keep it as a list for potential programmatic use
        
    count_skills = len(df)
    logger.info(f"Filter 2 (Skills Match): {count_skills} jobs remaining.")

    # --- Filter 3: Minimum Salary ---
    min_salary = config.get('min_salary', 0)
    if min_salary > 0:
        def filter_salary(row):
            min_amt = row.get('min_amount')
            max_amt = row.get('max_amount')
            
            # If both missing, keep it
            if pd.isnull(min_amt) and pd.isnull(max_amt):
                return True
            
            # Check if either meets the threshold
            if (pd.notnull(max_amt) and max_amt >= min_salary) or \
               (pd.notnull(min_amt) and min_amt >= min_salary):
                return True
                
            return False

        df = df[df.apply(filter_salary, axis=1)].copy()
        
    count_salary = len(df)
    logger.info(f"Filter 3 (Min Salary): {count_salary} jobs remaining.")

    # --- Filter 4: Job Type ---
    target_job_type = str(config.get('job_type', 'any')).lower().replace('-', '').replace(' ', '')
    if target_job_type != 'any':
        if 'job_type' in df.columns:
            def match_job_type(val):
                if pd.isnull(val) or str(val).strip() == "":
                    return True  # Permit if unknown
                val_norm = str(val).lower().replace('-', '').replace(' ', '')
                return val_norm == target_job_type
            
            df = df[df['job_type'].apply(match_job_type)].copy()
            
    count_type = len(df)
    logger.info(f"Filter 4 (Job Type): {count_type} jobs remaining.")

    # --- Filter 5: Blacklisted Companies ---
    blacklist = [c.lower() for c in config.get('blacklisted_companies', [])]
    if blacklist:
        if 'company' in df.columns:
            df = df[~df['company'].fillna('').str.lower().isin(blacklist)].copy()
            
    count_blacklist = len(df)
    logger.info(f"Filter 5 (Blacklist): {count_blacklist} jobs remaining.")

    # Final Deduplication and Sorting
    df = remove_duplicates(df)
    df = sort_jobs(df)
    
    final_count = len(df)
    logger.info(f"Filtered: {initial_count} jobs → {final_count} matching jobs.")
    
    return df

if __name__ == "__main__":
    # Standalone Test with Sample Data
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    test_config = {
        "skills": ["python", "django", "sql", "aws"],
        "min_salary": 80000,
        "job_type": "full-time",
        "blacklisted_companies": ["SpamJobs Inc", "Evil Corp"]
    }
    
    sample_jobs = pd.DataFrame([
        {
            "title": "Python Developer",
            "company": "Tech Stars",
            "is_remote": True,
            "location": "NY",
            "description": "Looking for a Python expert with SQL experience.",
            "min_amount": 90000,
            "max_amount": 120000,
            "job_type": "full-time",
            "date_posted": "2026-04-10",
            "job_url": "http://example.com/1"
        },
        {
            "title": "Django Backend Engineer",
            "company": "Startup Co",
            "is_remote": False,
            "location": "Remote",
            "description": "Django and AWS skills needed.",
            "min_amount": 70000,
            "max_amount": 85000,
            "job_type": "full-time",
            "date_posted": "2026-04-12",
            "job_url": "http://example.com/2"
        },
        {
            "title": "Data Cleaner",
            "company": "SpamJobs Inc",
            "is_remote": True,
            "location": "Remote",
            "description": "No skills required.",
            "min_amount": 50000,
            "max_amount": 60000,
            "job_type": "full-time",
            "date_posted": "2026-04-11",
            "job_url": "http://example.com/3"
        },
        {
            "title": "Python Developer",
            "company": "Tech Stars",
            "is_remote": True,
            "location": "NY",
            "description": "Looking for a Python expert with SQL experience.",
            "min_amount": 90000,
            "max_amount": 120000,
            "job_type": "full-time",
            "date_posted": "2026-04-10",
            "job_url": "http://example.com/1" # DUPLICATE
        },
        {
            "title": "Senior Engineer",
            "company": "Big Corp",
            "is_remote": True,
            "location": "Global",
            "description": "Python and SQL. Salary unknown.",
            "min_amount": None,
            "max_amount": None,
            "job_type": "full-time",
            "date_posted": "2026-04-13",
            "job_url": "http://example.com/4"
        }
    ])
    
    print("\n" + "="*50)
    print("      FILTER ENGINE AUTO-VERIFICATION")
    print("="*50)
    
    filtered_df = filter_jobs(sample_jobs, test_config)
    
    print("\nFiltered Results:")
    if not filtered_df.empty:
        print(filtered_df[['title', 'company', 'skill_match_count', 'matched_skills']])
        
        # Automatic Verification Checks
        assert len(filtered_df) == 3, f"Expected 3 jobs, got {len(filtered_df)}"
        assert "SpamJobs Inc" not in filtered_df['company'].values, "Blacklist failed"
        assert all(filtered_df['skill_match_count'] > 0), "Skill filter failed"
        assert filtered_df.iloc[0]['skill_match_count'] >= filtered_df.iloc[1]['skill_match_count'], "Sorting failed"
        
        print("\n[SUCCESS] All automatic verification checks passed!")
    else:
        print("\n[ERROR] No jobs passed the filters!")
    
    print("="*50 + "\n")
