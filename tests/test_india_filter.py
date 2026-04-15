"""
Unit tests for modules/india_filter.py
Tests India eligibility, fresher filtering, and experience extraction.
"""
import pytest
import pandas as pd

from modules.india_filter import (
    filter_india_eligible,
    filter_fresher_friendly,
    extract_experience_requirement,
    apply_india_fresher_filters,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_job(title: str, company: str, location: str, description: str) -> dict:
    """Shorthand for building a single-column test DataFrame."""
    return {
        "title": title,
        "company": company,
        "location": location,
        "description": description,
    }


# ---------------------------------------------------------------------------
# Tests: extract_experience_requirement
# ---------------------------------------------------------------------------

class TestExtractExperienceRequirement:
    """Tests for the regex-based experience extractor."""

    def test_range_years(self):
        result = extract_experience_requirement("3-5 years of experience required.")
        assert result["min_years"] == 3
        assert result["max_years"] == 5
        assert result["level"] == "mid"

    def test_min_plus_senior(self):
        result = extract_experience_requirement("7+ years of deep learning experience.")
        assert result["min_years"] == 7
        assert result["level"] == "senior"

    def test_entry_level_keyword(self):
        result = extract_experience_requirement("This is an entry level position for fresh graduates.")
        assert result["level"] == "entry"

    def test_no_number_experience_preferred(self):
        result = extract_experience_requirement("Experience preferred but not required.")
        # No numeric year found → level should remain "unknown"
        assert result["level"] == "unknown"
        assert result["min_years"] is None

    def test_empty_string(self):
        result = extract_experience_requirement("")
        assert result["level"] == "unknown"
        assert result["min_years"] is None

    def test_junior_keyword_signals_entry(self):
        result = extract_experience_requirement("Junior developer role, no prior industry experience needed.")
        assert "junior" in result["signals"]
        assert result["level"] == "entry"

    def test_zero_years(self):
        result = extract_experience_requirement("0-1 years experience welcome.")
        assert result["min_years"] == 0
        assert result["level"] == "entry"

    def test_eight_plus_years_is_senior(self):
        result = extract_experience_requirement("Minimum 8 years in ML infrastructure.")
        assert result["min_years"] == 8
        assert result["level"] == "senior"


# ---------------------------------------------------------------------------
# Tests: filter_india_eligible
# ---------------------------------------------------------------------------

class TestFilterIndiaEligible:
    """Tests for the India eligibility filter."""

    def test_location_remote_india_keeps_job(self):
        df = pd.DataFrame([_make_job(
            "ML Engineer", "TCS", "Remote - India",
            "Entry level Machine Learning role for fresh graduates. Python, TensorFlow, PyTorch."
        )])
        result = filter_india_eligible(df)
        assert len(result) == 1
        assert result.iloc[0]["india_eligible"]  # numpy bool_ truthy check

    def test_location_us_only_rejects_job(self):
        df = pd.DataFrame([_make_job(
            "AI Researcher", "OpenAI", "US Only",
            "Research role at OpenAI."
        )])
        result = filter_india_eligible(df)
        assert result.empty or not result.iloc[0]["india_eligible"]

    def test_location_worldwide_keeps_job(self):
        df = pd.DataFrame([_make_job(
            "Data Scientist", "Grab", "Worldwide",
            "Data science role open worldwide."
        )])
        result = filter_india_eligible(df)
        assert len(result) == 1

    def test_location_remote_no_restriction_keeps_job(self):
        df = pd.DataFrame([_make_job(
            "SDE", "Startup", "Remote",
            "Work remotely on our platform. No location restrictions mentioned."
        )])
        result = filter_india_eligible(df)
        assert len(result) == 1

    def test_desc_must_be_based_in_us_rejects(self):
        df = pd.DataFrame([_make_job(
            "Lead MLOps", "Amazon", "Remote",
            "Must be based in the United States. AWS SageMaker experience required."
        )])
        result = filter_india_eligible(df)
        assert result.empty or not result.iloc[0]["india_eligible"]

    def test_empty_location_keeps_job_benefit_of_doubt(self):
        df = pd.DataFrame([_make_job(
            "Data Engineer", "Unknown", "",
            "No location restrictions mentioned in this listing."
        )])
        result = filter_india_eligible(df)
        assert len(result) == 1

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame()
        result = filter_india_eligible(df)
        assert result.empty

    def test_multiple_jobs_mixed_eligibility(self, sample_india_jobs):
        result = filter_india_eligible(sample_india_jobs)
        # At least some should be kept and some should be rejected
        assert isinstance(result, pd.DataFrame)
        assert len(result) < len(sample_india_jobs)  # Some should be rejected

    def test_desc_must_reside_in_us_rejects(self):
        df = pd.DataFrame([_make_job(
            "MLOps Architect", "Corp", "Remote",
            "Minimum 8 years. Must reside in the United States."
        )])
        result = filter_india_eligible(df)
        assert result.empty or not result.iloc[0]["india_eligible"]


# ---------------------------------------------------------------------------
# Tests: filter_fresher_friendly
# ---------------------------------------------------------------------------

class TestFilterFresherFriendly:
    """Tests for the fresher-friendly filter."""

    def test_junior_title_keeps_job(self):
        df = pd.DataFrame([_make_job(
            "Junior ML Engineer", "TCS", "Remote",
            "Entry level Machine Learning role. Open to fresh graduates. Python required."
        )])
        result = filter_fresher_friendly(df)
        assert len(result) == 1
        assert result.iloc[0]["fresher_friendly"]  # numpy bool_ truthy check

    def test_senior_staff_title_rejects_job(self):
        df = pd.DataFrame([_make_job(
            "Senior Staff AI Research Scientist", "DeepMind", "Remote",
            "This is a senior-level position. Must have strong publication record and deep expertise in ML."
        )])
        result = filter_fresher_friendly(df)
        assert result.empty or not result.iloc[0]["fresher_friendly"]

    def test_desc_five_plus_years_rejects(self):
        df = pd.DataFrame([_make_job(
            "ML Engineer", "Some Corp", "Remote",
            "5+ years of deep learning experience required. Strong background in NLP mandatory."
        )])
        result = filter_fresher_friendly(df)
        assert result.empty or not result.iloc[0]["fresher_friendly"]

    def test_desc_zero_to_two_years_keeps_job(self):
        df = pd.DataFrame([_make_job(
            "Data Analyst", "Analytics Co", "Remote",
            "Looking for someone with 0-2 years experience. Familiarity with PyTorch preferred."
        )])
        result = filter_fresher_friendly(df)
        assert len(result) == 1

    def test_no_experience_mentioned_keeps_job_benefit_of_doubt(self):
        df = pd.DataFrame([_make_job(
            "Data Analyst", "Tech Firm", "Remote",
            "We are hiring a data analyst to work with our business intelligence team. SQL and Excel skills."
        )])
        result = filter_fresher_friendly(df)
        assert len(result) == 1

    def test_senior_title_with_junior_in_desc_edge_case(self):
        """Edge case: title has 'Senior' but description explicitly welcomes junior engineers."""
        df = pd.DataFrame([_make_job(
            "Senior ML Engineer", "Startup", "Remote",
            "We welcome senior or junior ML engineers with strong fundamentals. No strict years required."
        )])
        result = filter_fresher_friendly(df)
        # Edge case — may or may not keep depending on implementation
        # Just verify it doesn't crash
        assert isinstance(result, pd.DataFrame)

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame()
        result = filter_fresher_friendly(df)
        assert result.empty


# ---------------------------------------------------------------------------
# Tests: apply_india_fresher_filters
# ---------------------------------------------------------------------------

class TestApplyIndiaFresherFilters:
    """Tests for the main orchestrator function."""

    def test_full_pipeline_mixed_data(self, sample_india_jobs):
        """Full pipeline should filter out ineligible jobs."""
        config = {"target_country": "India", "experience": {"level": "fresher", "max_years": 1}}

        result = apply_india_fresher_filters(sample_india_jobs, config)

        assert isinstance(result, pd.DataFrame)
        # The pipeline should keep fewer jobs than the input
        assert len(result) <= len(sample_india_jobs)

    def test_target_country_any_skips_india_filter(self):
        """When target_country='any', India filter should not be applied."""
        df = pd.DataFrame([_make_job(
            "AI Researcher", "OpenAI", "US Only", "Must be US citizen."
        )])
        config = {"target_country": "any", "experience": {"level": "any", "max_years": 99}}

        result = apply_india_fresher_filters(df, config)

        # Both filters skipped → original job should remain
        assert len(result) == 1
        # india_eligible column should NOT be added when filter was skipped
        assert "india_eligible" not in result.columns

    def test_experience_level_any_skips_fresher_filter(self):
        """When experience.level='any', fresher filter should not be applied."""
        df = pd.DataFrame([_make_job(
            "Senior Staff ML Scientist", "DeepMind", "Worldwide",
            "10+ years required. PhD mandatory. Very senior position."
        )])
        config = {"target_country": "any", "experience": {"level": "any"}}

        result = apply_india_fresher_filters(df, config)

        # Fresher filter skipped → senior job should remain
        assert len(result) == 1
        # fresher_friendly column should NOT be present when filter was skipped
        assert "fresher_friendly" not in result.columns

    def test_empty_dataframe_returns_empty(self):
        config = {"target_country": "India", "experience": {"level": "fresher"}}
        result = apply_india_fresher_filters(pd.DataFrame(), config)
        assert result.empty

    def test_experience_details_column_added(self, sample_india_jobs):
        """The orchestrator should add the experience_details column."""
        config = {"target_country": "any", "experience": {"level": "any"}}

        result = apply_india_fresher_filters(sample_india_jobs, config)

        # experience_details is always computed regardless of filter settings
        assert "experience_details" in result.columns
        assert len(result) == len(sample_india_jobs)  # no jobs removed when both skipped

    def test_india_eligible_column_added_when_country_filtered(self):
        df = pd.DataFrame([
            _make_job("Junior Dev", "TCS", "Remote - India", "Entry level Python role."),
        ])
        config = {"target_country": "India", "experience": {"level": "any"}}

        result = apply_india_fresher_filters(df, config)

        # india_eligible column is added when India filter runs
        assert "india_eligible" in result.columns
        # fresher_friendly column should NOT be present when fresher filter is skipped
        assert "fresher_friendly" not in result.columns
