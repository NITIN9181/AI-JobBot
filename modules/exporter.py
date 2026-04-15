from __future__ import annotations
import pandas as pd
import os
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from modules.utils import retry

# Google Sheets Integration Imports
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

"""
GOOGLE SHEETS SETUP INSTRUCTIONS:
1. Create a Google Cloud Project: https://console.cloud.google.com/
2. Enable APIs: Search for and enable "Google Sheets API" and "Google Drive API".
3. Create a Service Account: 
   - Go to "APIs & Services" > "Credentials" > "Create Credentials" > "Service Account".
4. Generate Key:
   - Go to the "Keys" tab in the service account settings.
   - Click "Add Key" > "Create new key" > "JSON".
   - Save the downloaded file as 'credentials.json' in your project root.
5. Create a Google Sheet and share it with the service account email.
6. Update .env with GOOGLE_SHEETS_CRED_FILE and GOOGLE_SHEET_NAME.
"""

@retry(max_attempts=3, delay=5)
def setup_google_sheets(sheet_name: str) -> tuple[Optional[gspread.Spreadsheet], Optional[gspread.Worksheet]]:
    """
    Connects to Google Sheets and ensures the 'Job Listings' worksheet exists.
    """
    if not GSHEETS_AVAILABLE:
        return None, None

    cred_file = os.getenv("GOOGLE_SHEETS_CRED_FILE", "credentials.json")
    if not os.path.exists(cred_file):
        logger.warning(f"Google credentials file '{cred_file}' not found.")
        return None, None

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(cred_file, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(sheet_name)
        
        try:
            worksheet = spreadsheet.worksheet("Job Listings")
        except gspread.WorksheetNotFound:
            logger.info("Worksheet 'Job Listings' not found. Creating it now...")
            worksheet = spreadsheet.add_worksheet(title="Job Listings", rows="1000", cols="20")
            
        return spreadsheet, worksheet
    except Exception as e:
        logger.error(f"Error connecting to Google Sheets: {e}")
        raise # Rethrow for retry decorator

@retry(max_attempts=3, delay=2)
def check_sheet_duplicates(worksheet, job_url: str) -> bool:
    """Checks if a job URL already exists in the 'Job URL' column (Col 10)."""
    try:
        urls = worksheet.col_values(10)
        return job_url in urls
    except Exception as e:
        logger.error(f"Error checking duplicates in sheet: {e}")
        raise

def _clear_existing_rules(worksheet, sheet_id):
    """Clears conditional format rules and banding to prevent duplicates on re-runs."""
    try:
        metadata = worksheet.spreadsheet.fetch_sheet_metadata()
        target_sheet = None
        for sheet in metadata.get('sheets', []):
            if sheet.get('properties', {}).get('sheetId') == sheet_id:
                target_sheet = sheet
                break
        if not target_sheet:
            return

        delete_requests = []

        # Delete conditional format rules in reverse order (keeps indices valid)
        cond_rules = target_sheet.get('conditionalFormats', [])
        for i in range(len(cond_rules) - 1, -1, -1):
            delete_requests.append(
                {"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": i}}
            )

        # Delete banded ranges
        for band in target_sheet.get('bandedRanges', []):
            delete_requests.append(
                {"deleteBanding": {"bandedRangeId": band['bandedRangeId']}}
            )

        if delete_requests:
            worksheet.spreadsheet.batch_update({"requests": delete_requests})
            logger.debug(f"Cleared {len(delete_requests)} existing formatting rules.")
    except Exception as e:
        logger.debug(f"Could not clear existing formatting (may be first run): {e}")


@retry(max_attempts=3, delay=5)
def update_sheet_formatting(worksheet):
    """Applies comprehensive premium formatting to the Google Sheet.

    Formatting includes:
    - Dark header row with white bold text (frozen)
    - Optimized column widths for each data type
    - AI Score color coding: Green (>=80), Yellow (60-79), Red (<60), Gray (0)
    - Status column color coding with dropdown validation
    - URL column styled as clickable links
    - Alternating row colors (banded rows) for easy scanning
    - AI Reason column text wrapping
    - Notes column highlighted as user-editable
    """
    try:
        sheet_id = worksheet.id

        # ── Step 0: Clear existing rules to keep formatting idempotent ──
        _clear_existing_rules(worksheet, sheet_id)

        # ── Step 1: Header row — dark navy, white bold text, frozen ──
        worksheet.format("A1:L1", {
            "backgroundColor": {"red": 0.13, "green": 0.17, "blue": 0.31},
            "textFormat": {
                "bold": True,
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                "fontSize": 11
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
        })
        worksheet.freeze(rows=1)

        # ── Step 2: Build all batch requests ──
        requests = []

        # --- Column widths (pixels) ---
        #       A     B     C     D     E     F     G     H    I     J     K     L
        col_widths = [110, 260, 170, 170, 130, 110, 220, 85, 320, 280, 120, 200]
        for i, width in enumerate(col_widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                              "startIndex": i, "endIndex": i + 1},
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize"
                }
            })

        # --- Header row height ---
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS",
                          "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 40},
                "fields": "pixelSize"
            }
        })

        # --- Date column (A): Center aligned ---
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                          "startColumnIndex": 0, "endColumnIndex": 1},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment"
            }
        })

        # --- Job Type column (F): Center aligned ---
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                          "startColumnIndex": 5, "endColumnIndex": 6},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment"
            }
        })

        # --- AI Score column (H): Center + Bold + larger font ---
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                          "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {
                    "horizontalAlignment": "CENTER",
                    "textFormat": {"bold": True, "fontSize": 12}
                }},
                "fields": "userEnteredFormat(horizontalAlignment,textFormat)"
            }
        })

        # --- AI Reason column (I): Text wrap for long explanations ---
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                          "startColumnIndex": 8, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat.wrapStrategy"
            }
        })

        # --- Job URL column (J): Blue underlined text (link style) ---
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                          "startColumnIndex": 9, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {"textFormat": {
                    "foregroundColor": {"red": 0.06, "green": 0.33, "blue": 0.80},
                    "underline": True
                }}},
                "fields": "userEnteredFormat.textFormat"
            }
        })

        # --- Status column (K): Center aligned ---
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                          "startColumnIndex": 10, "endColumnIndex": 11},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment"
            }
        })

        # --- Notes column (L): Light cream background (user-editable hint) ---
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                          "startColumnIndex": 11, "endColumnIndex": 12},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 1.0, "green": 0.98, "blue": 0.88}
                }},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

        # ── Conditional Formatting: AI Score (Column H) ──

        # Gray: exactly 0 (not scored / AI disabled)
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                                "startColumnIndex": 7, "endColumnIndex": 8}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_EQ",
                                      "values": [{"userEnteredValue": "0"}]},
                        "format": {
                            "backgroundColor": {"red": 0.90, "green": 0.90, "blue": 0.90},
                            "textFormat": {"foregroundColor": {"red": 0.6, "green": 0.6, "blue": 0.6}}
                        }
                    }
                },
                "index": 0
            }
        })

        # Green: >= 80 (great match)
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                                "startColumnIndex": 7, "endColumnIndex": 8}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER_THAN_EQ",
                                      "values": [{"userEnteredValue": "80"}]},
                        "format": {"backgroundColor": {"red": 0.72, "green": 0.91, "blue": 0.72}}
                    }
                },
                "index": 1
            }
        })

        # Yellow: 60-79 (decent match)
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                                "startColumnIndex": 7, "endColumnIndex": 8}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_BETWEEN",
                                      "values": [{"userEnteredValue": "60"},
                                                  {"userEnteredValue": "79"}]},
                        "format": {"backgroundColor": {"red": 1.0, "green": 0.93, "blue": 0.65}}
                    }
                },
                "index": 2
            }
        })

        # Red: < 60 (weak match)
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                                "startColumnIndex": 7, "endColumnIndex": 8}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS",
                                      "values": [{"userEnteredValue": "60"}]},
                        "format": {"backgroundColor": {"red": 0.96, "green": 0.78, "blue": 0.78}}
                    }
                },
                "index": 3
            }
        })

        # ── Conditional Formatting: Status (Column K) ──
        status_colors = [
            ("Not Applied", {"red": 0.88, "green": 0.88, "blue": 0.88}),    # Gray
            ("Applied",     {"red": 0.71, "green": 0.84, "blue": 0.96}),    # Blue
            ("Interview",   {"red": 0.72, "green": 0.91, "blue": 0.72}),    # Green
            ("Rejected",    {"red": 0.96, "green": 0.78, "blue": 0.78}),    # Red
            ("Offer",       {"red": 1.0,  "green": 0.87, "blue": 0.40}),    # Gold
        ]
        for idx, (status_text, bg_color) in enumerate(status_colors):
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                                    "startColumnIndex": 10, "endColumnIndex": 11}],
                        "booleanRule": {
                            "condition": {"type": "TEXT_EQ",
                                          "values": [{"userEnteredValue": status_text}]},
                            "format": {"backgroundColor": bg_color, "textFormat": {"bold": True}}
                        }
                    },
                    "index": 4 + idx
                }
            })

        # ── Data Validation: Status dropdown (Column K) ──
        requests.append({
            "setDataValidation": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                          "startColumnIndex": 10, "endColumnIndex": 11},
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": "Not Applied"},
                            {"userEnteredValue": "Applied"},
                            {"userEnteredValue": "Interview"},
                            {"userEnteredValue": "Rejected"},
                            {"userEnteredValue": "Offer"},
                        ]
                    },
                    "showCustomUi": True,
                    "strict": False
                }
            }
        })

        # ── Banded rows for easy scanning (auto-filled columns A-J) ──
        requests.append({
            "addBanding": {
                "bandedRange": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                              "startColumnIndex": 0, "endColumnIndex": 10},
                    "rowProperties": {
                        "firstBandColor":  {"red": 1.0,  "green": 1.0,  "blue": 1.0},
                        "secondBandColor": {"red": 0.94, "green": 0.95, "blue": 0.97}
                    }
                }
            }
        })

        # ── Tab color (green accent) ──
        requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "tabColor": {"red": 0.20, "green": 0.66, "blue": 0.33}
                },
                "fields": "tabColor"
            }
        })

        # ── Execute all formatting in one batch ──
        worksheet.spreadsheet.batch_update({"requests": requests})
        logger.info("Applied premium formatting to Google Sheet.")

    except Exception as e:
        logger.warning(f"Could not apply formatting: {e}")
        raise

@retry(max_attempts=3, delay=5)
def get_application_stats(worksheet) -> dict:
    """Reads the Status column and returns counts."""
    stats = {"total": 0, "not_applied": 0, "applied": 0, "interview": 0, "rejected": 0, "offer": 0}
    try:
        statuses = worksheet.col_values(11)[1:] # Status is column 11
        stats["total"] = len(statuses)
        for s in statuses:
            s_low = str(s).lower().replace(" ", "_").strip()
            if s_low in stats:
                stats[s_low] += 1
            else:
                stats["not_applied"] += 1
        return stats
    except Exception as e:
        logger.error(f"Error getting sheet stats: {e}")
        raise

def display_application_stats(stats: dict):
    """Prints a premium ASCII box with application statistics."""
    total = stats.get("total", 0)
    applied = stats.get("applied", 0)
    interview = stats.get("interview", 0)
    response_rate = (interview / applied * 100) if applied > 0 else 0
    
    print("\n" + "═"*40)
    print(f"║ {'TRACKER STATISTICS':^36} ║")
    print("═"*40)
    print(f"║ Total Jobs:          {total:<17} ║")
    print(f"║ Applied:             {applied:<17} ║")
    print(f"║ Interviewing:        {interview:<17} ║")
    print(f"║ Response Rate:       {response_rate:>6.1f}%           ║")
    print("═"*40 + "\n")

def export_to_google_sheets(df: pd.DataFrame, config: dict) -> Dict[str, Any]:
    """Appends new jobs to Google Sheets."""
    status = {"success": False, "url": "", "count": 0}
    if df.empty: return status

    sheet_name = os.getenv("GOOGLE_SHEET_NAME", "JobBot_Output")
    spreadsheet, worksheet = setup_google_sheets(sheet_name)
    if not spreadsheet or not worksheet:
        return status
    
    status["url"] = spreadsheet.url
    try:
        # Ensure headers - more robust check
        first_row = worksheet.row_values(1)
        expected_headers = ["Date Found", "Title", "Company", "Location", "Salary Range", "Job Type", "Skills Matched", "AI Score", "AI Reason", "Job URL", "Status", "Notes"]
        
        if not first_row or first_row[0] != "Date Found":
            logger.info("Headers missing or incorrect. Inserting at the top.")
            worksheet.insert_row(expected_headers, index=1)
            # Re-fetch values if we inserted
            existing_urls = worksheet.col_values(10)
        else:
            existing_urls = worksheet.col_values(10)
        new_rows = []
        current_date = datetime.now().strftime("%Y-%m-%d")

        for _, row in df.iterrows():
            job_url = str(row.get('job_url', ''))
            if job_url in existing_urls: continue
            
            salary_str = f"{row.get('min_amount', 'N/A')} - {row.get('max_amount', 'N/A')}"
            matched_skills = ", ".join(row.get('matched_skills', [])) if isinstance(row.get('matched_skills'), list) else str(row.get('matched_skills', 'N/A'))

            # Sanitize row data for JSON compliance (Google Sheets API)
            row_data = [
                current_date, 
                str(row.get('title', 'N/A')), 
                str(row.get('company', 'N/A')),
                str(row.get('location', 'N/A')), 
                salary_str, 
                str(row.get('job_type', 'N/A')),
                matched_skills, 
                row.get('ai_match_score', 0) if pd.notnull(row.get('ai_match_score')) else 0, 
                str(row.get('ai_match_reason', 'N/A')) if pd.notnull(row.get('ai_match_reason')) else "N/A",
                job_url, 
                "Not Applied", 
                ""
            ]
            
            # Final safety check for NaN elements
            row_data = [("" if pd.isna(item) else item) for item in row_data]
            new_rows.append(row_data)

        if new_rows:
            @retry(max_attempts=3, delay=10)
            def do_append():
                worksheet.append_rows(new_rows)
            
            do_append()
            logger.info(f"Appended {len(new_rows)} new jobs to Google Sheets.")
            status["success"] = True
            status["count"] = len(new_rows)
            update_sheet_formatting(worksheet)
        else:
            logger.info("No new jobs to add to Google Sheets.")
            status["success"] = True

    except Exception as e:
        logger.error(f"Error in export_to_google_sheets: {e}")
        status["success"] = False
        
    return status

def export_to_csv(df: pd.DataFrame, output_dir: str = "output") -> str:
    """Exports to a timestamped CSV."""
    if df.empty: return ""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filepath = os.path.join(output_dir, f"jobs_{timestamp}.csv")
    
    cols = [
        "title", "company", "location", "job_url", "job_type", 
        "min_amount", "max_amount", "currency", "date_posted", "description", 
        "source_board", "matched_skills", "ai_match_score", "ai_match_reason",
        "verified", "verification_confidence", "legitimacy_status", 
        "india_verified", "fresher_verified", "verification_notes"
    ]
    existing_cols = [c for c in cols if c in df.columns]
    
    try:
        df[existing_cols].to_csv(filepath, index=False)
        logger.info(f"Exported {len(df)} jobs to {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to export CSV: {e}")
        return ""

def export_latest_csv(df: pd.DataFrame, output_dir: str = "output") -> str:
    """Exports both timestamped and latest_jobs.csv."""
    filepath = export_to_csv(df, output_dir)
    if filepath and not df.empty:
        latest_path = os.path.join(output_dir, "latest_jobs.csv")
        try:
            df.to_csv(latest_path, index=False)
            logger.info(f"Updated latest results at {latest_path}")
        except Exception as e:
            logger.error(f"Failed to update latest CSV: {e}")
    return filepath

def display_terminal_summary(df: pd.DataFrame, top_n: int = 5):
    """Prints a clean summary to the terminal."""
    print(f"\n{'='*50}\n{'RUN SUMMARY':^50}\n{'='*50}")
    print(f"Total jobs: {len(df)}")
    
    if df.empty:
        print("\nNo jobs found.")
        return

    is_scored = "ai_match_score" in df.columns
    sort_col = "ai_match_score" if is_scored else "skill_match_count"
    display_df = df.sort_values(by=sort_col, ascending=False).head(top_n)

    print("\nTop Matches:")
    for i, (_, row) in enumerate(display_df.iterrows(), 1):
        score = f"{int(row.get(sort_col, 0))}%" if is_scored else "N/A"
        print(f"[{i}] {score} - {row['title']} @ {row['company']}")

def generate_run_summary(scraped: int, matched: int, new: int, time_sec: float, ai_stats: dict = None, gs_status: dict = None, v_stats: dict = None) -> str:
    """Generates a text summary of the run."""
    summary = f"""
---------------------------------------
JobBot Run Results
---------------------------------------
Scraped: {scraped} | Matched: {matched} | New: {new}
Time:    {time_sec:.1f}s
"""
    if v_stats:
        status = "OK" if v_stats.get("rejected", 0) == 0 else "FLT"
        summary += f"Verify:  [{status}] Legitimate: {v_stats.get('legitimate', 0)}/{v_stats.get('total_verified', 0)} | Rejected: {v_stats.get('rejected', 0)}\n"
        
    if ai_stats:
        summary += f"AI Match: Top Score {ai_stats.get('top_score', 0)}% ({ai_stats.get('top_job', 'N/A')})\n"
        
    if gs_status:
        summary += f"Sheets:  {'[OK]' if gs_status.get('success') else '[FAIL]'} {gs_status.get('count', 0)} added\n"
    
    return summary.strip()

if __name__ == "__main__":
    from modules.logger_setup import setup_logging
    setup_logging()
    logger.info("Exporter test session.")
