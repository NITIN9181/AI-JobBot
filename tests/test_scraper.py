"""
Unit tests for modules/scraper.py (scrape_all_jobs).

NOTE: Phase 7 updated scraper.py so that when target_country='India' (the
default), base search terms are tripled by appending ' fresher' and
' entry level' variants.  These tests patch scrape_jobs and time.sleep so
they never hit the network.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from modules.scraper import scrape_all_jobs


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_scrape_jobs():
    with patch("modules.scraper.scrape_jobs") as mock:
        yield mock


@pytest.fixture
def mock_sleep():
    with patch("modules.scraper.time.sleep") as mock:
        yield mock


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_df(*titles):
    """Return a DataFrame with one job per title, including a 'site' column."""
    return pd.DataFrame([
        {"title": t, "company": "Acme", "site": "indeed"} for t in titles
    ])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_scrape_all_jobs_returns_df(sample_config, mock_scrape_jobs, mock_sleep):
    """scrape_all_jobs should return a DataFrame combining all search results.

    sample_config has 2 base search terms.  In India mode (default) each is
    expanded ×3, so scrape_jobs is called 6 times (3 per base term).
    """
    # scrape_jobs always returns one job
    mock_scrape_jobs.return_value = _make_df("Job 1")

    results = scrape_all_jobs(sample_config)

    assert isinstance(results, pd.DataFrame)
    assert not results.empty
    # source_search_term is added per job
    assert "source_search_term" in results.columns
    # source_platform is derived from 'site' column via jobspy
    assert "source_platform" in results.columns


def test_scrape_all_jobs_empty(sample_config, mock_scrape_jobs, mock_sleep):
    """When every scrape_jobs call returns empty, result should be empty."""
    mock_scrape_jobs.return_value = pd.DataFrame()

    results = scrape_all_jobs(sample_config)

    assert results.empty
    assert isinstance(results, pd.DataFrame)


def test_scrape_all_jobs_one_site_fails(sample_config, mock_scrape_jobs, mock_sleep):
    """Pipeline should continue even if some searches throw exceptions."""
    # First call succeeds, all remaining raise
    mock_scrape_jobs.side_effect = [
        _make_df("Job 1"),
        Exception("Network error"),
        Exception("Network error"),
        Exception("Network error"),
        Exception("Network error"),
        Exception("Network error"),
    ]

    results = scrape_all_jobs(sample_config)

    # Should still contain the first job
    assert len(results) == 1
    assert results.iloc[0]["title"] == "Job 1"


def test_scrape_all_jobs_delay(sample_config, mock_scrape_jobs, mock_sleep):
    """A 3-second delay must be inserted after every search term call."""
    mock_scrape_jobs.return_value = pd.DataFrame()

    scrape_all_jobs(sample_config)

    # India mode: 2 base terms × 3 variants = 6 calls → 6 sleep(3) calls
    expected_calls = len(sample_config["search_terms"]) * 3
    assert mock_sleep.call_count == expected_calls
    mock_sleep.assert_called_with(3)


def test_scrape_all_jobs_no_country_expansion():
    """When target_country='any', search terms should not be expanded."""
    config = {
        "search_terms": ["Software Engineer"],
        "results_per_site": 5,
        "hours_old": 24,
        "target_country": "any",   # <-- no expansion expected
    }
    with patch("modules.scraper.scrape_jobs") as mock_scrape, \
         patch("modules.scraper.time.sleep"):
        mock_scrape.return_value = pd.DataFrame()

        scrape_all_jobs(config)

        # Only 1 base term, no expansion → scrape_jobs called once
        assert mock_scrape.call_count == 1


def test_scrape_all_jobs_india_expansion():
    """India mode should triple the number of search_terms."""
    config = {
        "search_terms": ["ML Engineer", "Data Scientist"],
        "results_per_site": 5,
        "hours_old": 24,
        "target_country": "India",
    }
    with patch("modules.scraper.scrape_jobs") as mock_scrape, \
         patch("modules.scraper.time.sleep"):
        mock_scrape.return_value = pd.DataFrame()

        scrape_all_jobs(config)

        # 2 base terms × 3 variants = 6
        assert mock_scrape.call_count == 6
