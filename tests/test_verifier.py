"""
Unit tests for modules/verifier.py
Tests AI verification with mocked OpenAI/NVIDIA API calls.
"""
import pytest
import json
import pandas as pd
from unittest.mock import patch, MagicMock, call

from modules.verifier import (
    verify_single_job,
    verify_all_jobs,
    get_verification_summary,
    get_cache_key,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_JOB = {
    "title": "Junior ML Engineer",
    "company": "TCS",
    "location": "Remote - India",
    "description": "Entry level machine learning role. Python, TensorFlow required. 0-1 years experience.",
    "source_platform": "Himalayas",
}

VALID_AI_RESPONSE = {
    "is_legitimate": True,
    "legitimacy_reason": "Real company with proper job description",
    "india_eligible": True,
    "india_reason": "Open to candidates in India",
    "fresher_friendly": True,
    "fresher_reason": "Entry level, 0-1 years OK",
    "estimated_experience_years": 0,
    "confidence": 90,
    "red_flags": [],
    "company_type": "enterprise",
}


def _make_mock_completion(content: str) -> MagicMock:
    """Build a mock OpenAI ChatCompletion response."""
    mock_choice = MagicMock()
    mock_choice.message.content = content

    mock_response = MagicMock()
    mock_response.choices = [mock_choice]
    return mock_response


def _make_mock_client(return_value=None, side_effect=None) -> MagicMock:
    """Build a mock OpenAI client."""
    mock_client = MagicMock()
    if side_effect:
        mock_client.chat.completions.create.side_effect = side_effect
    else:
        mock_client.chat.completions.create.return_value = return_value
    return mock_client


# ---------------------------------------------------------------------------
# Tests: verify_single_job
# ---------------------------------------------------------------------------

class TestVerifySingleJob:
    """Tests for the single-job AI verification function."""

    def test_valid_response_parsed_correctly(self):
        """A well-formed JSON response should be parsed into the expected fields."""
        content = json.dumps(VALID_AI_RESPONSE)
        mock_client = _make_mock_client(return_value=_make_mock_completion(content))

        result = verify_single_job(SAMPLE_JOB, mock_client, "test-model")

        assert result["is_legitimate"] is True
        assert result["india_eligible"] is True
        assert result["fresher_friendly"] is True
        assert result["confidence"] == 90
        assert result["company_type"] == "enterprise"
        assert isinstance(result["red_flags"], list)

    def test_malformed_json_returns_default_response(self):
        """Malformed JSON should trigger the default fallback response."""
        mock_client = _make_mock_client(
            return_value=_make_mock_completion("This is not JSON at all")
        )

        result = verify_single_job(SAMPLE_JOB, mock_client, "test-model")

        # Default response has verification_failed in red_flags
        assert "verification_failed" in result.get("red_flags", [])

    def test_api_timeout_returns_default_response(self):
        """API timeout/exception should return default response, not crash."""
        mock_client = _make_mock_client(side_effect=Exception("Connection timeout"))

        result = verify_single_job(SAMPLE_JOB, mock_client, "test-model")

        assert "verification_failed" in result.get("red_flags", [])
        assert result["is_legitimate"] is True  # Default is True (assume legitimate)

    def test_json_in_markdown_code_block_parsed(self):
        """JSON wrapped in ```json ... ``` markdown should still be parsed."""
        content = "```json\n" + json.dumps(VALID_AI_RESPONSE) + "\n```"
        mock_client = _make_mock_client(return_value=_make_mock_completion(content))

        result = verify_single_job(SAMPLE_JOB, mock_client, "test-model")

        assert result["confidence"] == 90
        assert result["is_legitimate"] is True

    def test_confidence_clamped_to_100(self):
        """Confidence values over 100 should be clamped to 100."""
        response_data = {**VALID_AI_RESPONSE, "confidence": 150}
        mock_client = _make_mock_client(
            return_value=_make_mock_completion(json.dumps(response_data))
        )

        result = verify_single_job(SAMPLE_JOB, mock_client, "test-model")

        assert result["confidence"] <= 100

    def test_confidence_clamped_to_zero(self):
        """Confidence values below 0 should be clamped to 0."""
        response_data = {**VALID_AI_RESPONSE, "confidence": -50}
        mock_client = _make_mock_client(
            return_value=_make_mock_completion(json.dumps(response_data))
        )

        result = verify_single_job(SAMPLE_JOB, mock_client, "test-model")

        assert result["confidence"] >= 0

    def test_all_expected_keys_in_result(self):
        """Result must contain all required keys."""
        content = json.dumps(VALID_AI_RESPONSE)
        mock_client = _make_mock_client(return_value=_make_mock_completion(content))

        result = verify_single_job(SAMPLE_JOB, mock_client, "test-model")

        expected_keys = [
            "is_legitimate", "legitimacy_reason", "india_eligible", "india_reason",
            "fresher_friendly", "fresher_reason", "estimated_experience_years",
            "confidence", "red_flags", "company_type"
        ]
        for key in expected_keys:
            assert key in result, f"Key '{key}' missing from verification result"


# ---------------------------------------------------------------------------
# Tests: verify_all_jobs
# ---------------------------------------------------------------------------

class TestVerifyAllJobs:
    """Tests for the batch verification function."""

    SAMPLE_JOBS_DF = pd.DataFrame([
        {
            "title": "Junior ML Engineer",
            "company": "TCS",
            "location": "Remote - India",
            "description": "Entry level ML role. Python, TensorFlow.",
            "source_platform": "Himalayas",
        },
        {
            "title": "Data Scientist",
            "company": "Flipkart",
            "location": "Worldwide",
            "description": "SQL and Python required. 0-2 years experience.",
            "source_platform": "RemoteOK",
        },
        {
            "title": "SDE - AI Platform",
            "company": "Zoho",
            "location": "Remote",
            "description": "Software engineer for AI platform. Python, Docker.",
            "source_platform": "Jobicy",
        },
    ])

    @patch("modules.verifier.save_verify_cache")
    @patch("modules.verifier.load_verify_cache")
    @patch("modules.verifier.time.sleep")
    @patch("modules.verifier.OpenAI")
    def test_returns_df_with_verification_columns(
        self, mock_openai_cls, mock_sleep, mock_load_cache, mock_save_cache, mock_env
    ):
        """verify_all_jobs should add the required verification columns to the DF."""
        mock_load_cache.return_value = {}

        content = json.dumps(VALID_AI_RESPONSE)
        mock_client = _make_mock_client(return_value=_make_mock_completion(content))
        mock_openai_cls.return_value = mock_client

        config = {"ai_scoring": {"model": "test-model"}, "verification": {"max_jobs_to_verify": 100, "min_confidence": 0}}

        result_df, stats = verify_all_jobs(self.SAMPLE_JOBS_DF.copy(), config)

        required_cols = [
            "verified", "verification_confidence", "verification_red_flags",
            "india_verified", "fresher_verified"
        ]
        for col in required_cols:
            assert col in result_df.columns, f"Column '{col}' missing from result"

    @patch("modules.verifier.save_verify_cache")
    @patch("modules.verifier.load_verify_cache")
    @patch("modules.verifier.time.sleep")
    @patch("modules.verifier.OpenAI")
    def test_stats_dict_has_expected_keys(
        self, mock_openai_cls, mock_sleep, mock_load_cache, mock_save_cache, mock_env
    ):
        mock_load_cache.return_value = {}
        content = json.dumps(VALID_AI_RESPONSE)
        mock_client = _make_mock_client(return_value=_make_mock_completion(content))
        mock_openai_cls.return_value = mock_client

        config = {"ai_scoring": {}, "verification": {"max_jobs_to_verify": 100, "min_confidence": 0}}

        _, stats = verify_all_jobs(self.SAMPLE_JOBS_DF.copy(), config)

        expected_stats_keys = [
            "total_verified", "legitimate", "suspicious", "rejected",
            "india_eligible", "fresher_friendly", "cached", "avg_confidence", "enabled"
        ]
        for key in expected_stats_keys:
            assert key in stats, f"Stats key '{key}' missing"

    def test_no_api_key_adds_default_columns(self, monkeypatch):
        """Without NVIDIA_API_KEY, should skip verification and add default columns."""
        monkeypatch.delenv("NVIDIA_API_KEY", raising=False)

        config = {}
        result_df, stats = verify_all_jobs(self.SAMPLE_JOBS_DF.copy(), config)

        assert "verified" in result_df.columns
        assert "india_verified" in result_df.columns
        assert "fresher_verified" in result_df.columns
        assert stats["enabled"] is False

    def test_empty_dataframe_returns_empty_and_default_stats(self):
        """Empty input should return empty DF and zero stats."""
        config = {}
        result_df, stats = verify_all_jobs(pd.DataFrame(), config)

        assert result_df.empty
        assert stats["total_verified"] == 0

    @patch("modules.verifier.save_verify_cache")
    @patch("modules.verifier.load_verify_cache")
    @patch("modules.verifier.time.sleep")
    @patch("modules.verifier.OpenAI")
    def test_caching_skips_duplicate_api_calls(
        self, mock_openai_cls, mock_sleep, mock_load_cache, mock_save_cache, mock_env
    ):
        """Second call with the same jobs should use cache, not make new API calls."""
        # Pre-populate cache with both jobs
        job1 = self.SAMPLE_JOBS_DF.iloc[0].to_dict()
        job2 = self.SAMPLE_JOBS_DF.iloc[1].to_dict()
        job3 = self.SAMPLE_JOBS_DF.iloc[2].to_dict()

        cached_result = {**VALID_AI_RESPONSE, "cached_date": "2026-04-15"}
        mock_load_cache.return_value = {
            get_cache_key(job1): cached_result,
            get_cache_key(job2): cached_result,
            get_cache_key(job3): cached_result,
        }

        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        config = {"ai_scoring": {}, "verification": {"max_jobs_to_verify": 100, "min_confidence": 0}}

        _, stats = verify_all_jobs(self.SAMPLE_JOBS_DF.copy(), config)

        # All jobs should be served from cache
        assert stats["cached"] == 3
        # No API calls should be made
        mock_client.chat.completions.create.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: get_verification_summary
# ---------------------------------------------------------------------------

class TestGetVerificationSummary:
    """Tests for the formatted summary string function."""

    def test_disabled_returns_disabled_message(self):
        stats = {
            "enabled": False, "total_verified": 0, "legitimate": 0,
            "suspicious": 0, "rejected": 0, "india_eligible": 0,
            "fresher_friendly": 0, "cached": 0, "avg_confidence": 0
        }
        result = get_verification_summary(stats)

        assert "Disabled" in result or "No API Key" in result

    def test_enabled_with_data_contains_counts(self):
        stats = {
            "enabled": True,
            "total_verified": 10,
            "legitimate": 8,
            "suspicious": 2,
            "rejected": 1,
            "india_eligible": 7,
            "fresher_friendly": 6,
            "cached": 3,
            "avg_confidence": 82,
        }
        result = get_verification_summary(stats)

        assert "8" in result        # legitimate count
        assert "10" in result       # total count
        assert "82" in result       # avg confidence
        assert isinstance(result, str)

    def test_summary_contains_percentages(self):
        stats = {
            "enabled": True,
            "total_verified": 4,
            "legitimate": 4,
            "suspicious": 0,
            "rejected": 0,
            "india_eligible": 2,
            "fresher_friendly": 2,
            "cached": 0,
            "avg_confidence": 75,
        }
        result = get_verification_summary(stats)

        # Should contain percentage values
        assert "%" in result

    def test_no_jobs_to_verify(self):
        stats = {
            "enabled": True,
            "total_verified": 0,
            "legitimate": 0,
            "suspicious": 0,
            "rejected": 0,
            "india_eligible": 0,
            "fresher_friendly": 0,
            "cached": 0,
            "avg_confidence": 0,
        }
        result = get_verification_summary(stats)

        assert isinstance(result, str)
        assert len(result) > 0
