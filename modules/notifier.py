import os
import smtplib
import logging
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, Any, Optional
import pandas as pd
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def format_salary(min_amt: Any, max_amt: Any, currency: Optional[str] = "USD") -> str:
    """
    Formats salary range into a readable string like '$80K - $120K USD'.
    Returns 'Not listed' if values are missing.
    """
    if pd.isna(min_amt) and pd.isna(max_amt):
        return "Not listed"
    
    parts = []
    
    def to_k(val):
        if pd.isna(val):
            return None
        try:
            val_num = float(val)
            if val_num >= 1000:
                return f"${int(val_num/1000)}K"
            return f"${int(val_num)}"
        except (ValueError, TypeError):
            return str(val)

    min_str = to_k(min_amt)
    max_str = to_k(max_amt)
    
    if min_str and max_str:
        if min_str == max_str:
            res = f"{min_str}"
        else:
            res = f"{min_str} - {max_str}"
    elif min_str:
        res = f"{min_str}+"
    elif max_str:
        res = f"Up to {max_str}"
    else:
        return "Not listed"
    
    if currency:
        res += f" {currency}"
        
    return res

def send_email_digest(jobs: pd.DataFrame, config: Dict[str, Any]):
    """
    Sends an HTML email summary of the top job matches.
    """
    gmail_address = os.getenv("GMAIL_ADDRESS")
    gmail_app_password = os.getenv("GMAIL_APP_PASSWORD")
    
    if not gmail_address or not gmail_app_password:
        logger.warning("Email credentials missing (GMAIL_ADDRESS/GMAIL_APP_PASSWORD). Skipping email digest.")
        return False

    if jobs.empty:
        logger.info("No jobs to send in digest. Skipping email.")
        return False

    # Take top 20 jobs
    top_jobs = jobs.head(20).copy()
    count = len(top_jobs)
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    # Create the root message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"🤖 JobBot Daily Report — {date_str} | {count} New Jobs Found"
    msg['From'] = gmail_address
    msg['To'] = gmail_address

    # Build HTML Body
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #1a73e8, #0d47a1); color: white; padding: 30px; text-align: center; border-radius: 8px 8px 0 0; }}
            .stats {{ background: #f8f9fa; padding: 15px; border-bottom: 2px solid #eee; display: flex; justify-content: space-around; }}
            .stat-item {{ text-align: center; }}
            .stat-value {{ font-size: 24px; font-weight: bold; color: #1a73e8; }}
            .stat-label {{ font-size: 14px; color: #666; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th {{ background-color: #f1f3f4; color: #5f6368; text-align: left; padding: 12px; border-bottom: 2px solid #dee2e6; }}
            td {{ padding: 12px; border-bottom: 1px solid #eee; vertical-align: middle; }}
            tr:nth-child(even) {{ background-color: #fafbfc; }}
            .job-title {{ font-weight: bold; color: #1a73e8; text-decoration: none; }}
            .company {{ color: #5f6368; font-size: 14px; }}
            .salary {{ font-weight: 500; color: #34a853; }}
            .skills {{ font-size: 12px; color: #666; font-style: italic; }}
            .score-high {{ background-color: #e6ffed; color: #22863a; padding: 4px 8px; border-radius: 4px; font-weight: bold; }}
            .score-medium {{ background-color: #fff8e1; color: #b78103; padding: 4px 8px; border-radius: 4px; font-weight: bold; }}
            .score-low {{ color: #666; }}
            .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #999; border-top: 1px solid #eee; padding-top: 20px; }}
            @media screen and (max-width: 600px) {{
                .stats {{ flex-direction: column; }}
                th:nth-child(4), td:nth-child(4) {{ display: none; }} /* Hide skills on mobile */
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 style="margin:0;">Your Daily Job Matches</h1>
                <p style="margin:10px 0 0 0; opacity: 0.9;">Hand-picked opportunities for {date_str}</p>
            </div>
            
            <div class="stats">
                <div class="stat-item">
                    <div class="stat-value">{len(jobs)}</div>
                    <div class="stat-label">Total Found</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{count}</div>
                    <div class="stat-label">Top Matches Sent</div>
                </div>
            </div>

            <table>
                <thead>
                    <tr>
                        <th>Job Title</th>
                        <th>Company</th>
                        <th>Salary</th>
                        <th>Skills</th>
                        <th>Score</th>
                    </tr>
                </thead>
                <tbody>
    """

    for _, job in top_jobs.iterrows():
        title = job.get('title', 'Unknown Title')
        url = job.get('job_url', '#')
        company = job.get('company', 'Unknown Company')
        
        # Format salary
        sal_min = job.get('min_amount')
        sal_max = job.get('max_amount')
        currency = job.get('currency', 'USD')
        salary_str = format_salary(sal_min, sal_max, currency)
        
        # Skills
        skills = job.get('matched_skills', [])
        if isinstance(skills, list):
            skills_str = ", ".join(skills)
        else:
            skills_str = str(skills)
            
        # AI Score logic
        score = job.get('ai_score', 0)
        score_class = "score-low"
        if score > 80:
            score_class = "score-high"
        elif score >= 60:
            score_class = "score-medium"
        
        score_display = f"{int(score)}%" if score > 0 else "N/A"

        html_content += f"""
                    <tr>
                        <td><a href="{url}" class="job-title">{title}</a></td>
                        <td><div class="company">{company}</div></td>
                        <td><div class="salary">{salary_str}</div></td>
                        <td><div class="skills">{skills_str}</div></td>
                        <td><span class="{score_class}">{score_display}</span></td>
                    </tr>
        """

    html_content += f"""
                </tbody>
            </table>
            
            <div class="footer">
                <p>Generated by <strong>JobBot</strong> — <a href="https://github.com/yourname/jobbot" style="color:#999;">github.com/yourname/jobbot</a></p>
                <p>You are receiving this because you configured JobBot notifications.</p>
            </div>
        </div>
    </body>
    </html>
    """

    part1 = MIMEText(html_content, 'html')
    msg.attach(part1)

    # SMTP Send with Retry logic
    max_retries = 1
    for attempt in range(max_retries + 1):
        try:
            logger.info(f"Connecting to SMTP server (Attempt {attempt + 1})...")
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(gmail_address, gmail_app_password)
                server.send_message(msg)
                logger.info(f"Email digest sent successfully to {gmail_address}")
                return True # Success
        except Exception as e:
            logger.error(f"Failed to send email on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries:
                logger.info("Retrying in 30 seconds...")
                time.sleep(30)
            else:
                logger.error("All email send attempts failed.")
                return False

def send_telegram_message(text: str, token: str, chat_id: str) -> bool:
    """
    Generic function to send any text via Telegram Bot API.
    Returns True if sent successfully, False otherwise.
    
    Telegram Setup Instructions:
    1. Create Bot: Message @BotFather on Telegram, use /newbot, and get the BOT_TOKEN.
    2. Get Chat ID: 
       - Start a chat with your bot and send a message.
       - Visit: https://api.telegram.org/bot<TOKEN>/getUpdates
       - Find "id" inside the "chat" object.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    
    max_retries = 1
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info("Telegram message sent successfully.")
                return True
            else:
                logger.error(f"Telegram API error ({response.status_code}): {response.text}")
        except Exception as e:
            logger.error(f"Telegram connection failed (Attempt {attempt+1}): {str(e)}")
        
        if attempt < max_retries:
            logger.info("Retrying Telegram in 10 seconds...")
            time.sleep(10)
            
    return False

def send_telegram_alert(jobs: pd.DataFrame, config: Dict[str, Any]):
    """
    Sends a Telegram alert with the top matched jobs.
    Splits message if it exceeds 4096 characters.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        logger.warning("Telegram credentials missing (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID). Skipping Telegram alert.")
        return False

    if jobs.empty:
        logger.info("No jobs to send via Telegram. Skipping.")
        return False

    # Prepare header
    date_str = datetime.now().strftime("%Y-%m-%d")
    header = (
        f"🤖 *JobBot Daily Report*\n"
        f"📅 Date: {date_str}\n"
        f"📊 New Jobs Found: {len(jobs)}\n\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
    )
    
    # Take top 10 jobs
    top_jobs = jobs.head(10)
    job_blocks = []
    
    for idx, (_, job) in enumerate(top_jobs.iterrows(), 1):
        title = job.get('title', 'Unknown Title')
        company = job.get('company', 'Unknown Company')
        url = job.get('job_url', '#')
        
        sal_min = job.get('min_amount')
        sal_max = job.get('max_amount')
        currency = job.get('currency', 'USD')
        salary_str = format_salary(sal_min, sal_max, currency)
        
        skills = job.get('matched_skills', [])
        skills_str = ", ".join(skills) if isinstance(skills, list) else str(skills)
        
        block = (
            f"{idx}️⃣ *{title}*\n"
            f"🏢 {company}\n"
            f"💰 {salary_str}\n"
            f"🔗 [Apply Here]({url})\n"
            f"🎯 Skills: {skills_str}\n\n"
        )
        job_blocks.append(block)

    footer = "━━━━━━━━━━━━━━━━━━━\n📋 Full list exported to CSV"
    
    # Construct message and handle length limits
    full_message = header + "".join(job_blocks) + footer
    
    # Split message if > 4096 chars
    if len(full_message) <= 4096:
        return send_telegram_message(full_message, token, chat_id)
    else:
        # Simple splitting
        parts = []
        current_part = header
        for block in job_blocks:
            if len(current_part) + len(block) > 4000: # Leave some buffer
                parts.append(current_part)
                current_part = ""
            current_part += block
        
        current_part += footer
        parts.append(current_part)
        
        success = True
        for p in parts:
            if not send_telegram_message(p, token, chat_id):
                success = False
        return success

def send_notifications(jobs: pd.DataFrame, config: Dict[str, Any]):
    notif_config = config.get('notifications', {})
    email_enabled = notif_config.get('email_enabled', False)
    telegram_enabled = notif_config.get('telegram_enabled', False)
    
    results = {
        "email_enabled": email_enabled,
        "telegram_enabled": telegram_enabled,
        "email_sent": False,
        "telegram_sent": False
    }
    
    if not email_enabled and not telegram_enabled:
        logger.info("No notifications configured (Email and Telegram are both disabled).")
        return results

    if email_enabled:
        results["email_sent"] = send_email_digest(jobs, config)
        
    if telegram_enabled:
        results["telegram_sent"] = send_telegram_alert(jobs, config)
        
    return results

if __name__ == "__main__":
    # Test block
    from dotenv import load_dotenv
    load_dotenv()
    
    print("Running standalone test for notifier.py...")
    
    # Create dummy data
    test_jobs = pd.DataFrame([
        {
            'title': 'Senior Python Developer',
            'company': 'Tech Corp',
            'job_url': 'https://example.com/job1',
            'min_amount': 120000,
            'max_amount': 150000,
            'currency': 'USD',
            'matched_skills': ['python', 'django', 'aws'],
            'ai_score': 85
        },
        {
            'title': 'Backend Engineer',
            'company': 'Startup Inc',
            'job_url': 'https://example.com/job2',
            'min_amount': 90000,
            'max_amount': 110000,
            'currency': 'USD',
            'matched_skills': ['python', 'fastapi', 'postgresql'],
            'ai_score': 72
        },
        {
            'title': 'Junior Dev',
            'company': 'Old School Co',
            'job_url': 'https://example.com/job3',
            'min_amount': None,
            'max_amount': None,
            'currency': 'USD',
            'matched_skills': ['python'],
            'ai_score': 45
        }
    ])
    
    test_config = {
        'notifications': {
            'email_enabled': True,
            'telegram_enabled': True
        }
    }
    
    # Test format_salary
    print(f"Salary test 1: {format_salary(80000, 120000, 'USD')}")
    print(f"Salary test 2: {format_salary(None, None, 'USD')}")
    
    # Test unified notification function
    # Note: Requires .env to have credentials to actually send
    print("Testing unified notifications (will attempt Email and Telegram)...")
    send_notifications(test_jobs, test_config)
