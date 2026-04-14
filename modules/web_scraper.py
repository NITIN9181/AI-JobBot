import pandas as pd
import requests
import feedparser
import re
import time
import logging
import html
from typing import List, Dict, Any, Optional
from modules.utils import retry

# Set up logger
logger = logging.getLogger(__name__)

def strip_html_tags(text: Optional[str]) -> str:
    """
    Removes HTML tags and decodes HTML entities from a string.
    """
    if not text:
        return ""
    # Remove HTML tags
    clean_text = re.sub('<[^<]+?>', '', text)
    # Decode HTML entities (e.g., &amp; -> &, &lt; -> <)
    clean_text = html.unescape(clean_text)
    # Remove extra whitespace
    clean_text = " ".join(clean_text.split())
    return clean_text

def extract_experience_keywords(text: str) -> List[str]:
    """
    Scans text for experience-related keywords and returns a list of matches.
    """
    if not text:
        return []
    
    text = text.lower()
    matches = []
    
    # Simple keyword patterns
    keywords = {
        "entry level": ["entry level", "entry-level", "junior", "fresher", "new grad", "graduate"],
        "mid level": ["mid level", "mid-level", "intermediate"],
        "senior": ["senior", "lead", "staff", "principal", "architect", "manager"]
    }
    
    for level, patterns in keywords.items():
        if any(pattern in text for pattern in patterns):
            matches.append(level)
            
    # Year patterns
    # Matches "0-1 years", "1+ years", "5 years experience", etc.
    year_patterns = [
        r'(\d+)\s*[-+]\s*(\d*)\s*years?',
        r'(\d+)\s*years?\s*(?:of)?\s*experience'
    ]
    
    for pattern in year_patterns:
        year_matches = re.findall(pattern, text)
        for ym in year_matches:
            if isinstance(ym, tuple):
                matches.append(f"{ym[0]}+ years")
            else:
                matches.append(f"{ym} years")
                
    return list(set(matches))

@retry(max_attempts=2, delay=5)
def fetch_url(url: str, headers: Dict[str, str] = None, timeout: int = 30) -> requests.Response:
    """
    Helper to fetch a URL with the retry decorator.
    """
    if headers is None:
        headers = {"User-Agent": "JobBot/1.0"}
    
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response

def scrape_remoteok() -> pd.DataFrame:
    """
    Fetches remote jobs from RemoteOK API.
    """
    url = "https://remoteok.com/api"
    logger.info("Scraping RemoteOK...")
    
    try:
        response = fetch_url(url)
        data = response.json()
        
        # First item is metadata, skip it
        if len(data) <= 1:
            return pd.DataFrame()
        
        jobs = []
        for item in data[1:]:
            # Extract salary if present
            salary_min = item.get("salary_min")
            salary_max = item.get("salary_max")
            salary_info = ""
            if salary_min or salary_max:
                salary_info = f"{salary_min or '?'}-{salary_max or '?'}"
            
            tags = item.get("tags", [])
            description = strip_html_tags(item.get("description", ""))
            
            # AI/ML Relevance check
            ai_ml_tags = ["machine-learning", "ai", "data-science", "python", "deep-learning", "nlp", "computer-vision"]
            is_ai_ml_role = any(tag.lower() in [t.lower() for t in tags] for tag in ai_ml_tags)
            
            job = {
                "title": item.get("position"),
                "company": item.get("company"),
                "job_url": item.get("url"),
                "location": item.get("location", "Remote"),
                "description": description,
                "date_posted": item.get("date"),
                "salary_info": salary_info,
                "experience_tags": ", ".join(extract_experience_keywords(description)),
                "source_platform": "RemoteOK",
                "is_ai_ml_role": is_ai_ml_role
            }
            jobs.append(job)
            
        return pd.DataFrame(jobs)
        
    except Exception as e:
        logger.error(f"RemoteOK scrape failed: {e}")
        return pd.DataFrame()

def scrape_himalayas(country: str = "India") -> pd.DataFrame:
    """
    Fetches remote jobs from Himalayas API.
    """
    base_url = f"https://himalayas.app/jobs/api/search?country={country}"
    logger.info(f"Scraping Himalayas (Country: {country})...")
    
    all_jobs = []
    limit = 20
    
    try:
        for page in range(5):
            offset = page * limit
            url = f"{base_url}&offset={offset}&limit={limit}"
            
            try:
                response = fetch_url(url)
                data = response.json()
                jobs_data = data.get("jobs", [])
                
                if not jobs_data:
                    break
                    
                for item in jobs_data:
                    salary_info = ""
                    min_sal = item.get("salaryMin")
                    max_sal = item.get("salaryMax")
                    curr = item.get("salaryCurrency", "USD")
                    if min_sal or max_sal:
                        salary_info = f"{curr} {min_sal or '?'}-{max_sal or '?'}"
                    
                    job = {
                        "title": item.get("title"),
                        "company": item.get("companyName"),
                        "job_url": f"https://himalayas.app/jobs/{item.get('id')}",
                        "location": item.get("locationRestrictions") or "Remote",
                        "description": strip_html_tags(item.get("description", "")),
                        "date_posted": item.get("pubDate") or item.get("publishedDate"),
                        "salary_info": salary_info,
                        "experience_tags": item.get("seniority", ""),
                        "source_platform": "Himalayas"
                    }
                    all_jobs.append(job)
                
                if len(jobs_data) < limit:
                    break
                    
                time.sleep(2)
                
            except requests.exceptions.HTTPError as he:
                if he.response.status_code == 429:
                    logger.warning("Himalayas rate limited (429). Stopping pagination.")
                    break
                raise he
                
        return pd.DataFrame(all_jobs)
        
    except Exception as e:
        logger.error(f"Himalayas scrape failed: {e}")
        return pd.DataFrame()

def scrape_jobicy() -> pd.DataFrame:
    """
    Fetches remote jobs from Jobicy API.
    """
    url = "https://jobicy.com/api/v2/remote-jobs?count=50"
    logger.info("Scraping Jobicy...")
    
    try:
        response = fetch_url(url)
        data = response.json()
        jobs_data = data.get("jobs", [])
        
        jobs = []
        for item in jobs_data:
            salary_info = ""
            min_sal = item.get("annualSalaryMin")
            max_sal = item.get("annualSalaryMax")
            curr = item.get("salaryCurrency", "USD")
            if min_sal or max_sal:
                salary_info = f"{curr} {min_sal or '?'}-{max_sal or '?'}"
            
            job = {
                "title": item.get("jobTitle"),
                "company": item.get("companyName"),
                "job_url": item.get("url"),
                "location": item.get("jobGeo"),
                "description": strip_html_tags(item.get("jobDescription", "")),
                "date_posted": item.get("pubDate"),
                "salary_info": salary_info,
                "experience_tags": item.get("jobLevel", ""),
                "source_platform": "Jobicy"
            }
            jobs.append(job)
            
        return pd.DataFrame(jobs)
        
    except Exception as e:
        logger.error(f"Jobicy scrape failed: {e}")
        return pd.DataFrame()

def scrape_weworkremotely_rss() -> pd.DataFrame:
    """
    Parses WeWorkRemotely RSS feed.
    """
    url = "https://weworkremotely.com/remote-jobs.rss"
    logger.info("Scraping WeWorkRemotely (RSS)...")
    
    try:
        feed = feedparser.parse(url)
        jobs = []
        
        for entry in feed.entries:
            # WWR title format: "Company: Job Title"
            raw_title = entry.title
            company = "Unknown"
            title = raw_title
            
            if ":" in raw_title:
                parts = raw_title.split(":", 1)
                company = parts[0].strip()
                title = parts[1].strip()
            
            description = strip_html_tags(getattr(entry, 'summary', entry.get('description', "")))
            
            # Scrappy salary extraction from description
            salary_pattern = r'\$\d{2,3},?\d{3}'
            salaries = re.findall(salary_pattern, description)
            salary_info = " - ".join(salaries) if salaries else ""
            
            job = {
                "title": title,
                "company": company,
                "job_url": entry.link,
                "location": "Remote",
                "description": description,
                "date_posted": getattr(entry, 'published', ""),
                "salary_info": salary_info,
                "experience_tags": ", ".join(extract_experience_keywords(description)),
                "source_platform": "WeWorkRemotely"
            }
            jobs.append(job)
            
        return pd.DataFrame(jobs)
        
    except Exception as e:
        logger.error(f"WeWorkRemotely RSS scrape failed: {e}")
        return pd.DataFrame()

def scrape_all_sources(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Orchestrates all extended job sources.
    """
    extended_config = config.get("extended_sources", {})
    if not extended_config.get("enabled", False):
        return pd.DataFrame()
    
    sources = [
        ("remoteok", scrape_remoteok),
        ("himalayas", scrape_himalayas),
        ("jobicy", scrape_jobicy),
        ("weworkremotely", scrape_weworkremotely_rss)
    ]
    
    all_dfs = []
    
    for source_id, source_func in sources:
        if extended_config.get(source_id, True):
            try:
                df = source_func()
                if not df.empty:
                    all_dfs.append(df)
                    logger.info(f"Source {source_id}: Found {len(df)} jobs")
                else:
                    logger.info(f"Source {source_id}: No jobs found")
            except Exception as e:
                logger.error(f"Error in source {source_id}: {e}")
            
            time.sleep(3) # Delay between sources
            
    if not all_dfs:
        return pd.DataFrame()
    
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # AI/ML Relevance Column
    AI_ML_KEYWORDS = [
        "ai", "artificial intelligence", "machine learning", "ml", "data scien",
        "deep learning", "nlp", "natural language", "computer vision", "cv engineer",
        "sde", "software development engineer", "software engineer",
        "mlops", "data engineer", "generative ai", "llm", "transformer",
        "neural network", "pytorch", "tensorflow"
    ]
    
    def check_relevance(row):
        title_desc = f"{row['title']} {row['description']}".lower()
        return any(kw in title_desc for kw in AI_ML_KEYWORDS)
    
    combined_df['ai_ml_relevant'] = combined_df.apply(check_relevance, axis=1)
    
    relevant_count = combined_df['ai_ml_relevant'].sum()
    logger.info(f"Extended Sources: Found {len(combined_df)} jobs from {len(all_dfs)} platforms ({relevant_count} AI/ML relevant)")
    
    return combined_df

if __name__ == "__main__":
    from modules.logger_setup import setup_logging
    setup_logging()
    
    logger.info("Testing extended sources...")
    
    # Test orchestrator
    test_config = {
        "extended_sources": {
            "enabled": True,
            "remoteok": True,
            "himalayas": True,
            "jobicy": True,
            "weworkremotely": True
        }
    }
    
    all_jobs = scrape_all_sources(test_config)
    
    if not all_jobs.empty:
        print("\n--- SAMPLE JOBS ---")
        print(all_jobs[['title', 'company', 'source_platform', 'ai_ml_relevant']].head(10))
        print(f"\nTotal from all extended sources: {len(all_jobs)} jobs")
        
        # Check specific platform counts
        print("\n--- COUNTS BY PLATFORM ---")
        print(all_jobs['source_platform'].value_counts())
    else:
        print("No jobs found in test run.")
