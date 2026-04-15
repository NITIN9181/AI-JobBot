import os
import yaml
import logging
import sys
from typing import Any, Dict, List
from dotenv import load_dotenv
from modules.logger_setup import setup_logging

# Initialize logging
logger = setup_logging()

def validate_config(config: Dict[str, Any]):
    """
    Validates configuration values with helpful error messages.
    """
    errors = []
    
    # Structure of rules: (key, expected_type, helpful_fix)
    rules = [
        ("search_terms", list, "Set search_terms to a list like ['Python Developer']"),
        ("skills", list, "Set skills to a list like ['python', 'aws']"),
        ("min_salary", int, "Set min_salary to a number like 60000"),
        ("job_type", str, "Set job_type to a string like 'full-time'"),
        ("country", str, "Set country to a country code like 'USA' or 'UK'"),
        ("results_per_site", int, "Set results_per_site to a number like 50"),
        ("hours_old", int, "Set hours_old to a number like 24"),
    ]
    
    for key, expected_type, fix in rules:
        val = config.get(key)
        if val is None:
            errors.append(f"❌ Config Error: Missing required field '{key}'\n💡 Fix: {fix}")
        elif not isinstance(val, expected_type):
            errors.append(f"❌ Config Error: '{key}' must be a {expected_type.__name__}, got '{type(val).__name__}'\n💡 Fix: {fix}")
            
    # Check if empty lists
    if isinstance(config.get("search_terms"), list) and not config.get("search_terms"):
        errors.append("❌ Config Error: 'search_terms' list is empty\n💡 Fix: Add at least one job title to search for")
        
    if errors:
        print("\n" + "!"*40)
        print("   CONFIGURATION VALIDATION FAILED")
        print("!"*40)
        for err in errors:
            print(f"\n{err}")
        print("\n" + "!"*40)
        sys.exit(1)


def validate_optional_config(config: Dict[str, Any]) -> None:
    """
    Validates optional Phase 7 configuration fields.
    Issues warnings for invalid values and auto-corrects to sensible defaults.
    Does NOT raise errors — missing optional fields are perfectly fine.

    Args:
        config: The loaded configuration dictionary (mutated in-place).
    """
    valid_levels = {"fresher", "junior", "mid", "senior", "any"}

    # ── target_country ────────────────────────────────────────────────────────
    tc = config.get("target_country")
    if tc is None:
        config["target_country"] = "India"
    elif not isinstance(tc, str):
        logger.warning(
            "'target_country' must be a string — resetting to 'India'. Got: %s", type(tc).__name__
        )
        config["target_country"] = "India"

    # ── experience ────────────────────────────────────────────────────────────
    exp = config.get("experience")
    if exp is None:
        config["experience"] = {"level": "fresher", "max_years": 1}
        logger.debug("'experience' not set — using defaults (level=fresher, max_years=1).")
    else:
        if not isinstance(exp, dict):
            logger.warning(
                "'experience' must be a dict — resetting to defaults. Got: %s", type(exp).__name__
            )
            config["experience"] = {"level": "fresher", "max_years": 1}
        else:
            level = exp.get("level", "fresher")
            if level not in valid_levels:
                logger.warning(
                    "'experience.level' must be one of %s — got '%s', resetting to 'fresher'.",
                    sorted(valid_levels), level,
                )
                config["experience"]["level"] = "fresher"

            max_years = exp.get("max_years", 1)
            if not isinstance(max_years, int) or max_years < 0:
                logger.warning(
                    "'experience.max_years' must be a non-negative int — got '%s', resetting to 1.",
                    max_years,
                )
                config["experience"]["max_years"] = 1

    # ── extended_sources ──────────────────────────────────────────────────────
    ext = config.get("extended_sources")
    if ext is not None:
        if not isinstance(ext, dict):
            logger.warning(
                "'extended_sources' must be a dict — disabling it. Got: %s", type(ext).__name__
            )
            config["extended_sources"] = {"enabled": False}
        else:
            for k, v in ext.items():
                if not isinstance(v, bool):
                    logger.warning(
                        "'extended_sources.%s' must be a bool — got '%s', setting to False.", k, v
                    )
                    config["extended_sources"][k] = False

    # ── verification ──────────────────────────────────────────────────────────
    verif = config.get("verification")
    if verif is not None:
        if not isinstance(verif, dict):
            logger.warning(
                "'verification' must be a dict — disabling it. Got: %s", type(verif).__name__
            )
            config["verification"] = {"enabled": False}
        else:
            min_conf = verif.get("min_confidence", 70)
            if not isinstance(min_conf, int) or not (0 <= min_conf <= 100):
                logger.warning(
                    "'verification.min_confidence' must be an int between 0-100 — got '%s', "
                    "resetting to 70.",
                    min_conf,
                )
                config["verification"]["min_confidence"] = 70


def get_config() -> Dict[str, Any]:
    """
    Loads configuration from config.yaml and environments variables from .env.
    Validates required fields and returns a unified configuration dictionary.
    """
    # Load .env file
    load_dotenv()
    
    config_path = "config.yaml"
    if not os.path.exists(config_path):
        logger.critical(f"Configuration file {config_path} not found.")
        print(f"❌ Critical Error: Missing {config_path}")
        sys.exit(1)

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.critical(f"Error parsing {config_path}: {e}")
        print(f"❌ Error Parsing {config_path}: {e}")
        sys.exit(1)

    # Perform thorough validation of required fields
    validate_config(config)

    # Validate optional Phase 7 fields (warnings only, with auto-correction)
    validate_optional_config(config)

    # Load sensitive data from environment
    config['groq_api_key'] = os.getenv("GROQ_API_KEY")
    config['nvidia_api_key'] = os.getenv("NVIDIA_API_KEY")
    config['gmail_address'] = os.getenv("GMAIL_ADDRESS")
    config['gmail_app_password'] = os.getenv("GMAIL_APP_PASSWORD")
    config['telegram_bot_token'] = os.getenv("TELEGRAM_BOT_TOKEN")
    config['telegram_chat_id'] = os.getenv("TELEGRAM_CHAT_ID")
    config['google_sheets_cred_file'] = os.getenv("GOOGLE_SHEETS_CRED_FILE")
    config['google_sheet_name'] = os.getenv("GOOGLE_SHEET_NAME")

    logger.info("Configuration loaded successfully")
    return config

if __name__ == "__main__":
    try:
        cfg = get_config()
        print("\nConfig loaded successfully.")
        print(f"Target Country:    {cfg.get('target_country', 'Not set')}")
        print(f"Experience Level:  {cfg.get('experience', {}).get('level', 'Not set')}")
        print(f"Max Years Exp:     {cfg.get('experience', {}).get('max_years', 'Not set')}")
        print(f"Extended Sources:  {cfg.get('extended_sources', {})}")
        print(f"Verification:      {cfg.get('verification', {})}")
    except Exception as e:
        print(f"Failed to load config: {e}")
