"""
Unit tests for modules/web_scraper.py
Tests all scraper functions with mocked HTTP calls.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, PropertyMock
import requests

from modules.web_scraper import (
    scrape_remoteok,
    scrape_himalayas,
    scrape_jobicy,
    scrape_weworkremotely_rss,
    scrape_all_sources,
    strip_html_tags,
    extract_experience_keywords,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

REMOTEOK_SAMPLE = [
    {"legal": "test"},  # first item is always metadata – skipped by the scraper
    {
        "position": "Junior ML Engineer",
        "company": "RemoteCo",
        "url": "https://remoteok.com/jobs/1",
        "location": "Remote",
        "description": "<p>Entry level <b>machine learning</b> role. Python, TensorFlow.</p>",
        "date": "2024-04-10",
        "salary_min": 60000,
        "salary_max": 90000,
        "tags": ["machine-learning", "python"],
    },
    {
        "position": "Data Scientist",
        "company": "AICorp",
        "url": "https://remoteok.com/jobs/2",
        "location": "Worldwide",
        "description": "SQL and Python required.",
        "date": "2024-04-11",
        "salary_min": None,
        "salary_max": None,
        "tags": ["data-science"],
    },
    {
        "position": "Frontend Dev",
        "company": "WebCo",
        "url": "https://remoteok.com/jobs/3",
        "location": "Remote",
        "description": "React and JavaScript.",
        "date": "2024-04-12",
        "salary_min": None,
        "salary_max": None,
        "tags": ["javascript"],
    },
]

HIMALAYAS_SAMPLE = {
    "jobs": [
        {
            "id": "101",
            "title": "AI Research Engineer",
            "companyName": "DeepThink",
            "locationRestrictions": "India",
            "description": "<p>Looking for junior AI engineers.</p>",
            "pubDate": "2024-04-10",
            "salaryMin": 50000,
            "salaryMax": 80000,
            "salaryCurrency": "USD",
            "seniority": "junior",
        }
    ]
}

JOBICY_SAMPLE = {
    "jobs": [
        {
            "jobTitle": "Python Backend Developer",
            "companyName": "RemoteStartup",
            "url": "https://jobicy.com/job/1",
            "jobGeo": "Worldwide",
            "jobDescription": "<p>Python and Django experience required. 0-2 years OK.</p>",
            "pubDate": "2024-04-09",
            "annualSalaryMin": 55000,
            "annualSalaryMax": 75000,
            "salaryCurrency": "USD",
            "jobLevel": "Entry",
        }
    ]
}


def _make_feedparser_entry(title: str, link: str, summary: str) -> MagicMock:
    """Helper to build a mock feedparser entry."""
    entry = MagicMock()
    entry.title = title
    entry.link = link
    entry.summary = summary
    entry.published = "Mon, 10 Apr 2024 12:00:00 +0000"
    return entry


def _make_mock_response(json_data, status_code=200):
    """Helper to build a mock requests.Response."""
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    mock_resp.raise_for_status = MagicMock()
    if status_code >= 400:
        http_error = requests.exceptions.HTTPError(response=mock_resp)
        mock_resp.raise_for_status.side_effect = http_error
    return mock_resp


# ---------------------------------------------------------------------------
# Tests: strip_html_tags
# ---------------------------------------------------------------------------

class TestStripHtmlTags:
    """Tests for the strip_html_tags utility function."""

    def test_removes_bold_and_paragraph_tags(self):
        result = strip_html_tags("<p>Hello <b>World</b></p>")
        assert result == "Hello World"

    def test_no_tags_passthrough(self):
        result = strip_html_tags("No tags here")
        assert result == "No tags here"

    def test_empty_string(self):
        result = strip_html_tags("")
        assert result == ""

    def test_none_input(self):
        result = strip_html_tags(None)
        assert result == ""

    def test_html_entities_decoded(self):
        result = strip_html_tags("&amp; &lt; &gt;")
        assert "&amp;" not in result
        assert "&" in result

    def test_nested_tags(self):
        result = strip_html_tags("<div><span>Nested</span> content</div>")
        assert result == "Nested content"


# ---------------------------------------------------------------------------
# Tests: extract_experience_keywords
# ---------------------------------------------------------------------------

class TestExtractExperienceKeywords:
    """Tests for the extract_experience_keywords utility function."""

    def test_junior_keyword_detected(self):
        result = extract_experience_keywords("We are looking for a junior Python developer.")
        assert any("entry level" in kw or "junior" in kw.lower() for kw in result), \
            f"Expected entry-level signal, got: {result}"

    def test_year_pattern_detected(self):
        result = extract_experience_keywords("Requires 5+ years of experience in machine learning.")
        # Should return something like "5+ years"
        assert len(result) > 0

    def test_no_keywords_returns_empty(self):
        result = extract_experience_keywords("We are hiring someone passionate about technology.")
        assert isinstance(result, list)
        # No experience keywords → should return [] or minimal matches
        assert result == [] or all("level" not in kw for kw in result)

    def test_senior_keyword_detected(self):
        result = extract_experience_keywords("This is a senior software engineer position.")
        assert any("senior" in kw for kw in result)

    def test_empty_string(self):
        result = extract_experience_keywords("")
        assert result == []

    def test_range_years_detected(self):
        result = extract_experience_keywords("Looking for candidate with 3-5 years experience.")
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Tests: scrape_remoteok
# ---------------------------------------------------------------------------

class TestScrapeRemoteok:
    """Tests for the scrape_remoteok() function."""

    @patch("modules.web_scraper.fetch_url")
    def test_returns_dataframe_with_correct_columns(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response(REMOTEOK_SAMPLE)

        df = scrape_remoteok()

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        for col in ["title", "company", "job_url", "source_platform"]:
            assert col in df.columns, f"Column '{col}' missing from DataFrame"

    @patch("modules.web_scraper.fetch_url")
    def test_source_platform_is_remoteok(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response(REMOTEOK_SAMPLE)

        df = scrape_remoteok()

        assert (df["source_platform"] == "RemoteOK").all(), \
            "All rows must have source_platform == 'RemoteOK'"

    @patch("modules.web_scraper.fetch_url")
    def test_correct_number_of_jobs(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response(REMOTEOK_SAMPLE)

        df = scrape_remoteok()

        # 3 real job items (first item is metadata and is skipped)
        assert len(df) == 3

    @patch("modules.web_scraper.fetch_url")
    def test_empty_response_returns_empty_df(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response([{"legal": "metadata"}])

        df = scrape_remoteok()

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @patch("modules.web_scraper.fetch_url")
    def test_http_403_returns_empty_df(self, mock_fetch):
        mock_fetch.side_effect = Exception("403 Forbidden")

        df = scrape_remoteok()

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @patch("modules.web_scraper.fetch_url")
    def test_html_stripped_from_description(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response(REMOTEOK_SAMPLE)

        df = scrape_remoteok()

        first_desc = df.iloc[0]["description"]
        assert "<p>" not in first_desc
        assert "<b>" not in first_desc


# ---------------------------------------------------------------------------
# Tests: scrape_himalayas
# ---------------------------------------------------------------------------

class TestScrapeHimalayas:
    """Tests for the scrape_himalayas() function."""

    @patch("modules.web_scraper.fetch_url")
    def test_returns_dataframe_with_jobs(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response(HIMALAYAS_SAMPLE)

        df = scrape_himalayas(country="India")

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert df.iloc[0]["title"] == "AI Research Engineer"

    @patch("modules.web_scraper.fetch_url")
    def test_country_parameter_included_in_url(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response({"jobs": []})

        scrape_himalayas(country="Germany")

        called_url = mock_fetch.call_args[0][0]
        assert "Germany" in called_url

    @patch("modules.web_scraper.fetch_url")
    def test_pagination_stops_when_no_jobs(self, mock_fetch):
        """Pagination should stop after the first empty page."""
        mock_fetch.return_value = _make_mock_response({"jobs": []})

        df = scrape_himalayas()

        # Only 1 call because first page was empty
        assert mock_fetch.call_count == 1
        assert df.empty

    @patch("modules.web_scraper.time")
    @patch("modules.web_scraper.fetch_url")
    def test_pagination_max_pages(self, mock_fetch, mock_time):
        """Should not call fetch more than max_pages (5) times."""
        # Always return 20 items (full page) to keep paginating
        full_page = {"jobs": [HIMALAYAS_SAMPLE["jobs"][0]] * 20}
        mock_fetch.return_value = _make_mock_response(full_page)

        df = scrape_himalayas()

        assert mock_fetch.call_count <= 5
        assert not df.empty

    @patch("modules.web_scraper.fetch_url")
    def test_rate_limit_429_stops_pagination(self, mock_fetch):
        """On 429 response, should stop early and return partial results."""
        first_response = _make_mock_response(HIMALAYAS_SAMPLE)
        http_error = requests.exceptions.HTTPError()
        rate_limit_resp = MagicMock(spec=requests.Response)
        rate_limit_resp.status_code = 429
        http_error.response = rate_limit_resp

        rate_limit_response = MagicMock(spec=requests.Response)
        rate_limit_response.raise_for_status.side_effect = http_error

        mock_fetch.side_effect = [first_response, rate_limit_response]

        df = scrape_himalayas()

        # Should have at least the first page's results
        assert isinstance(df, pd.DataFrame)
        assert len(df) >= 1

    @patch("modules.web_scraper.fetch_url")
    def test_source_platform_is_himalayas(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response(HIMALAYAS_SAMPLE)

        df = scrape_himalayas()

        assert (df["source_platform"] == "Himalayas").all()


# ---------------------------------------------------------------------------
# Tests: scrape_jobicy
# ---------------------------------------------------------------------------

class TestScrapeJobicy:
    """Tests for the scrape_jobicy() function."""

    @patch("modules.web_scraper.fetch_url")
    def test_returns_dataframe(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response(JOBICY_SAMPLE)

        df = scrape_jobicy()

        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    @patch("modules.web_scraper.fetch_url")
    def test_correct_field_mapping(self, mock_fetch):
        """jobTitle → title, companyName → company, url → job_url."""
        mock_fetch.return_value = _make_mock_response(JOBICY_SAMPLE)

        df = scrape_jobicy()

        assert df.iloc[0]["title"] == "Python Backend Developer"
        assert df.iloc[0]["company"] == "RemoteStartup"
        assert df.iloc[0]["job_url"] == "https://jobicy.com/job/1"

    @patch("modules.web_scraper.fetch_url")
    def test_source_platform_is_jobicy(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response(JOBICY_SAMPLE)

        df = scrape_jobicy()

        assert (df["source_platform"] == "Jobicy").all()

    @patch("modules.web_scraper.fetch_url")
    def test_empty_jobs_key_returns_empty_df(self, mock_fetch):
        mock_fetch.return_value = _make_mock_response({"jobs": []})

        df = scrape_jobicy()

        assert df.empty

    @patch("modules.web_scraper.fetch_url")
    def test_api_error_returns_empty_df(self, mock_fetch):
        mock_fetch.side_effect = Exception("Network error")

        df = scrape_jobicy()

        assert isinstance(df, pd.DataFrame)
        assert df.empty


# ---------------------------------------------------------------------------
# Tests: scrape_weworkremotely_rss
# ---------------------------------------------------------------------------

class TestScrapeWeWorkRemotelyRss:
    """Tests for the scrape_weworkremotely_rss() function."""

    @patch("modules.web_scraper.feedparser.parse")
    def test_returns_dataframe_with_jobs(self, mock_parse):
        mock_feed = MagicMock()
        mock_feed.entries = [
            _make_feedparser_entry(
                "TechCorp: Senior Python Developer",
                "https://weworkremotely.com/jobs/1",
                "Python, Django, REST APIs. 3+ years experience."
            )
        ]
        mock_parse.return_value = mock_feed

        df = scrape_weworkremotely_rss()

        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    @patch("modules.web_scraper.feedparser.parse")
    def test_title_split_on_colon_extracts_company(self, mock_parse):
        """Title format 'Company: Job Title' should be split correctly."""
        mock_feed = MagicMock()
        mock_feed.entries = [
            _make_feedparser_entry(
                "Acme Inc: Data Engineer",
                "https://weworkremotely.com/jobs/1",
                "Data engineering role."
            )
        ]
        mock_parse.return_value = mock_feed

        df = scrape_weworkremotely_rss()

        assert df.iloc[0]["company"] == "Acme Inc"
        assert df.iloc[0]["title"] == "Data Engineer"

    @patch("modules.web_scraper.feedparser.parse")
    def test_title_without_colon_uses_full_title(self, mock_parse):
        """If no colon, full title is used and company = 'Unknown'."""
        mock_feed = MagicMock()
        mock_feed.entries = [
            _make_feedparser_entry(
                "ML Engineer",
                "https://weworkremotely.com/jobs/2",
                "Machine learning role."
            )
        ]
        mock_parse.return_value = mock_feed

        df = scrape_weworkremotely_rss()

        assert df.iloc[0]["title"] == "ML Engineer"
        assert df.iloc[0]["company"] == "Unknown"

    @patch("modules.web_scraper.feedparser.parse")
    def test_source_platform_is_weworkremotely(self, mock_parse):
        mock_feed = MagicMock()
        mock_feed.entries = [
            _make_feedparser_entry(
                "Corp: Dev", "https://example.com", "Description."
            )
        ]
        mock_parse.return_value = mock_feed

        df = scrape_weworkremotely_rss()

        assert (df["source_platform"] == "WeWorkRemotely").all()

    @patch("modules.web_scraper.feedparser.parse")
    def test_malformed_rss_returns_empty_df(self, mock_parse):
        """Exception during parse should result in an empty DataFrame, not a crash."""
        mock_parse.side_effect = Exception("Malformed XML")

        df = scrape_weworkremotely_rss()

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @patch("modules.web_scraper.feedparser.parse")
    def test_empty_feed_returns_empty_df(self, mock_parse):
        mock_feed = MagicMock()
        mock_feed.entries = []
        mock_parse.return_value = mock_feed

        df = scrape_weworkremotely_rss()

        assert df.empty


# ---------------------------------------------------------------------------
# Tests: scrape_all_sources
# ---------------------------------------------------------------------------

class TestScrapeAllSources:
    """Tests for the scrape_all_sources() orchestrator."""

    BASE_CONFIG = {
        "extended_sources": {
            "enabled": True,
            "remoteok": True,
            "himalayas": True,
            "jobicy": True,
            "weworkremotely": True,
        }
    }

    @patch("modules.web_scraper.time.sleep")
    @patch("modules.web_scraper.scrape_weworkremotely_rss")
    @patch("modules.web_scraper.scrape_jobicy")
    @patch("modules.web_scraper.scrape_himalayas")
    @patch("modules.web_scraper.scrape_remoteok")
    def test_all_sources_enabled_calls_all_four(
        self, mock_rok, mock_him, mock_job, mock_wwr, mock_sleep
    ):
        """When all sources enabled, all 4 scrape functions must be called."""
        sample_df = pd.DataFrame([
            {"title": "Job", "company": "Co", "job_url": "http://x.com",
             "description": "AI job", "source_platform": "TestSource"}
        ])
        mock_rok.return_value = sample_df
        mock_him.return_value = sample_df
        mock_job.return_value = sample_df
        mock_wwr.return_value = sample_df

        result = scrape_all_sources(self.BASE_CONFIG)

        mock_rok.assert_called_once()
        mock_him.assert_called_once()
        mock_job.assert_called_once()
        mock_wwr.assert_called_once()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_extended_sources_disabled_returns_empty_df(self):
        """When extended_sources.enabled is False, must return empty DataFrame."""
        config = {"extended_sources": {"enabled": False}}

        result = scrape_all_sources(config)

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_no_extended_sources_key_returns_empty_df(self):
        """Missing extended_sources config should return empty DataFrame."""
        result = scrape_all_sources({})

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch("modules.web_scraper.time.sleep")
    @patch("modules.web_scraper.scrape_weworkremotely_rss")
    @patch("modules.web_scraper.scrape_jobicy")
    @patch("modules.web_scraper.scrape_himalayas")
    @patch("modules.web_scraper.scrape_remoteok")
    def test_individual_source_disabled_is_skipped(
        self, mock_rok, mock_him, mock_job, mock_wwr, mock_sleep
    ):
        """Disabling a specific source should skip only that scraper."""
        config = {
            "extended_sources": {
                "enabled": True,
                "remoteok": False,  # Disabled
                "himalayas": True,
                "jobicy": True,
                "weworkremotely": True,
            }
        }
        sample_df = pd.DataFrame([
            {"title": "Job", "company": "Co", "job_url": "http://x.com",
             "description": "SDE job", "source_platform": "TestSource"}
        ])
        mock_him.return_value = sample_df
        mock_job.return_value = sample_df
        mock_wwr.return_value = sample_df

        scrape_all_sources(config)

        mock_rok.assert_not_called()
        mock_him.assert_called_once()

    @patch("modules.web_scraper.time.sleep")
    @patch("modules.web_scraper.scrape_weworkremotely_rss")
    @patch("modules.web_scraper.scrape_jobicy")
    @patch("modules.web_scraper.scrape_himalayas")
    @patch("modules.web_scraper.scrape_remoteok")
    def test_all_sources_return_empty_gives_empty_df(
        self, mock_rok, mock_him, mock_job, mock_wwr, mock_sleep
    ):
        """If all sources return empty DataFrames, result should be empty."""
        for m in [mock_rok, mock_him, mock_job, mock_wwr]:
            m.return_value = pd.DataFrame()

        result = scrape_all_sources(self.BASE_CONFIG)

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch("modules.web_scraper.time.sleep")
    @patch("modules.web_scraper.scrape_weworkremotely_rss")
    @patch("modules.web_scraper.scrape_jobicy")
    @patch("modules.web_scraper.scrape_himalayas")
    @patch("modules.web_scraper.scrape_remoteok")
    def test_result_contains_ai_ml_relevant_column(
        self, mock_rok, mock_him, mock_job, mock_wwr, mock_sleep
    ):
        sample_df = pd.DataFrame([
            {"title": "ML Engineer", "company": "Co", "job_url": "http://x.com",
             "description": "Deep learning and pytorch", "source_platform": "TestSource"}
        ])
        for m in [mock_rok, mock_him, mock_job, mock_wwr]:
            m.return_value = sample_df

        result = scrape_all_sources(self.BASE_CONFIG)

        assert "ai_ml_relevant" in result.columns
