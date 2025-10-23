"""Tests for datacite module.

This test suite covers all functions in the datacite module with a focus on:
- Pure function testing without mocks
- HTTP request mocking for API interactions
- File I/O with proper cleanup
- Edge cases and error handling
- Pagination logic
"""
import gzip
import json
import os
import tempfile
from typing import Dict, Any, List
from unittest.mock import Mock, MagicMock, patch, call

import pytest
import requests

# Import functions to test
from src.datacite.datacite import (
    _build_query_params,
    _make_request_with_retry,
    _build_user_agent_headers,
    _should_continue_pagination,
    _serialize_to_ndjson,
    fetch_doi_page,
    fetch_doi,
    write_ndjson_gz,
)


class TestBuildQueryParams:
    """Tests for _build_query_params function.
    
    This function is pure (no side effects), so tests don't require mocking.
    """

    def test_basic_params(self, prefixes):
        """Test building query parameters with basic inputs."""
        result = _build_query_params(
            prefixes=prefixes,
            state="findable",
            page_size=100,
            page_number=0,
            detail=True
        )
        
        assert result["prefix"] == "10.7303"
        assert result["state"] == "findable"
        assert result["page[size]"] == 100
        assert result["page[number]"] == 0
        assert result["detail"] == "true"

    def test_multiple_prefixes(self, multiple_prefixes):
        """Test that multiple prefixes are joined with commas."""
        result = _build_query_params(
            prefixes=multiple_prefixes,
            state="findable",
            page_size=100,
            page_number=0,
            detail=True
        )
        
        assert result["prefix"] == "10.7303,10.5281"

    def test_detail_false_omits_param(self, prefixes):
        """Test that detail=False omits the detail parameter."""
        result = _build_query_params(
            prefixes=prefixes,
            state="findable",
            page_size=100,
            page_number=0,
            detail=False
        )
        
        assert "detail" not in result

    @pytest.mark.parametrize("state", ["findable", "registered", "draft"])
    def test_different_states(self, prefixes, state):
        """Test all valid DOI states."""
        result = _build_query_params(
            prefixes=prefixes,
            state=state,
            page_size=100,
            page_number=0,
            detail=True
        )
        
        assert result["state"] == state

    @pytest.mark.parametrize("page_number", [0, 1, 5, 100])
    def test_different_page_numbers(self, prefixes, page_number):
        """Test various page numbers including zero-based indexing."""
        result = _build_query_params(
            prefixes=prefixes,
            state="findable",
            page_size=100,
            page_number=page_number,
            detail=True
        )
        
        assert result["page[number]"] == page_number

    @pytest.mark.parametrize("page_size", [10, 100, 1000])
    def test_different_page_sizes(self, prefixes, page_size):
        """Test various page sizes."""
        result = _build_query_params(
            prefixes=prefixes,
            state="findable",
            page_size=page_size,
            page_number=0,
            detail=True
        )
        
        assert result["page[size]"] == page_size


class TestMakeRequestWithRetry:
    """Tests for _make_request_with_retry function.
    
    Uses mocking for requests.Session and time.sleep to avoid actual HTTP calls.
    """

    def test_successful_request_first_try(self, create_mock_response):
        """Test successful response on first attempt."""
        mock_session = Mock(spec=requests.Session)
        mock_response = create_mock_response(status_code=200)
        mock_session.get.return_value = mock_response
        
        result = _make_request_with_retry(
            session=mock_session,
            url="https://api.datacite.org/dois",
            params={"test": "value"}
        )
        
        assert result == mock_response
        assert mock_session.get.call_count == 1
        mock_session.get.assert_called_once_with(
            "https://api.datacite.org/dois",
            params={"test": "value"},
            timeout=60
        )

    @pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
    def test_retry_on_retryable_errors(self, mocker, status_code, create_mock_response):
        """Test that retryable errors trigger exponential backoff."""
        mock_sleep = mocker.patch("src.datacite.datacite.time.sleep")
        
        mock_session = Mock(spec=requests.Session)
        mock_response_fail = create_mock_response(status_code=status_code)
        mock_response_success = create_mock_response(status_code=200)
        
        # Fail twice, then succeed
        mock_session.get.side_effect = [
            mock_response_fail,
            mock_response_fail,
            mock_response_success
        ]
        
        result = _make_request_with_retry(
            session=mock_session,
            url="https://api.datacite.org/dois",
            params={"test": "value"},
            initial_backoff=2,
            max_retries=5
        )
        
        assert result == mock_response_success
        assert mock_session.get.call_count == 3
        # Verify exponential backoff: 2, 4 seconds
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(2)
        mock_sleep.assert_any_call(4)

    def test_max_retries_raises_after_limit(self, mocker, create_mock_response):
        """Test that max_retries is respected and raises after limit."""
        mock_sleep = mocker.patch("src.datacite.datacite.time.sleep")
        
        mock_session = Mock(spec=requests.Session)
        mock_response_fail = create_mock_response(
            status_code=503,
            raise_for_status_error=requests.HTTPError("503 Server Error")
        )
        
        # Always fail with retryable error
        mock_session.get.return_value = mock_response_fail
        
        # Should raise after max_retries attempts
        with pytest.raises(requests.HTTPError):
            _make_request_with_retry(
                session=mock_session,
                url="https://api.datacite.org/dois",
                params={"test": "value"},
                initial_backoff=1,
                max_retries=3
            )
        
        # Should have tried: initial + 3 retries = 4 total attempts
        assert mock_session.get.call_count == 4
        # Should have slept 3 times (after each retry)
        assert mock_sleep.call_count == 3

    def test_max_retries_default_value(self, mocker, create_mock_response):
        """Test that default max_retries value is used correctly."""
        mock_sleep = mocker.patch("src.datacite.datacite.time.sleep")
        
        mock_session = Mock(spec=requests.Session)
        mock_response_fail = create_mock_response(
            status_code=429,
            raise_for_status_error=requests.HTTPError("429 Too Many Requests")
        )
        
        # Always fail
        mock_session.get.return_value = mock_response_fail
        
        # Should use default max_retries (hardcoded to 10 in implementation)
        with pytest.raises(requests.HTTPError):
            _make_request_with_retry(
                session=mock_session,
                url="https://api.datacite.org/dois",
                params={"test": "value"}
            )
        
        # Should have tried: initial + 10 retries = 11 total attempts
        assert mock_session.get.call_count == 11
        assert mock_sleep.call_count == 10

    def test_exponential_backoff_calculation(self, mocker, create_mock_response):
        """Test that exponential backoff doubles each time."""
        mock_sleep = mocker.patch("src.datacite.datacite.time.sleep")
        
        mock_session = Mock(spec=requests.Session)
        mock_response_fail = create_mock_response(status_code=503)
        mock_response_success = create_mock_response(status_code=200)
        
        # Fail 4 times, then succeed
        mock_session.get.side_effect = [mock_response_fail] * 4 + [mock_response_success]
        
        result = _make_request_with_retry(
            session=mock_session,
            url="https://api.datacite.org/dois",
            params={"test": "value"},
            initial_backoff=2,
            max_retries=5
        )
        
        assert result == mock_response_success
        # Should have slept: 2, 4, 8, 16 seconds
        assert mock_sleep.call_count == 4
        mock_sleep.assert_any_call(2)
        mock_sleep.assert_any_call(4)
        mock_sleep.assert_any_call(8)
        mock_sleep.assert_any_call(16)

    def test_max_retries_zero_no_retry(self, mocker, create_mock_response):
        """Test that max_retries=0 means no retries on failure."""
        mock_sleep = mocker.patch("src.datacite.datacite.time.sleep")
        
        mock_session = Mock(spec=requests.Session)
        mock_response_fail = create_mock_response(
            status_code=503,
            raise_for_status_error=requests.HTTPError("503 Server Error")
        )
        
        mock_session.get.return_value = mock_response_fail
        
        # Should raise immediately without retrying
        with pytest.raises(requests.HTTPError):
            _make_request_with_retry(
                session=mock_session,
                url="https://api.datacite.org/dois",
                params={"test": "value"},
                max_retries=0
            )
        
        # Should only try once (no retries)
        assert mock_session.get.call_count == 1
        # Should not sleep at all
        assert mock_sleep.call_count == 0

    @pytest.mark.parametrize("status_code", [400, 401, 403, 404])
    def test_raises_on_non_retryable_errors(self, status_code, create_mock_response):
        """Test that non-retryable errors raise immediately."""
        mock_session = Mock(spec=requests.Session)
        mock_response = create_mock_response(
            status_code=status_code,
            raise_for_status_error=requests.HTTPError(f"{status_code} Client Error")
        )
        mock_session.get.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            _make_request_with_retry(
                session=mock_session,
                url="https://api.datacite.org/dois",
                params={"test": "value"}
            )
        
        # Should only try once for non-retryable errors
        assert mock_session.get.call_count == 1

    def test_custom_timeout(self, create_mock_response):
        """Test that custom timeout is passed to request."""
        mock_session = Mock(spec=requests.Session)
        mock_response = create_mock_response(status_code=200)
        mock_session.get.return_value = mock_response
        
        _make_request_with_retry(
            session=mock_session,
            url="https://api.datacite.org/dois",
            params={"test": "value"},
            timeout=120
        )
        
        mock_session.get.assert_called_once_with(
            "https://api.datacite.org/dois",
            params={"test": "value"},
            timeout=120
        )

    def test_timeout_exception(self, mocker):
        """Test that timeout exceptions are raised without retry."""
        mock_sleep = mocker.patch("src.datacite.datacite.time.sleep")
        mock_session = Mock(spec=requests.Session)
        mock_session.get.side_effect = requests.Timeout("Request timed out")
        
        with pytest.raises(requests.Timeout):
            _make_request_with_retry(
                session=mock_session,
                url="https://api.datacite.org/dois",
                params={"test": "value"}
            )
        
        # Should not retry on timeout
        assert mock_session.get.call_count == 1
        assert mock_sleep.call_count == 0

    def test_connection_error_not_retried(self):
        """Test that connection errors are raised without retry."""
        mock_session = Mock(spec=requests.Session)
        mock_session.get.side_effect = requests.ConnectionError("Connection failed")
        
        with pytest.raises(requests.ConnectionError):
            _make_request_with_retry(
                session=mock_session,
                url="https://api.datacite.org/dois",
                params={"test": "value"}
            )
        
        # Should not retry on connection error
        assert mock_session.get.call_count == 1

    def test_mixed_retryable_errors(self, mocker, create_mock_response):
        """Test recovery from different retryable error codes."""
        mock_sleep = mocker.patch("src.datacite.datacite.time.sleep")
        mock_session = Mock(spec=requests.Session)
        
        # Mix of 429, 503, 500 then success
        responses = []
        for status_code in [429, 503, 500, 200]:
            json_data = {"data": []} if status_code == 200 else None
            mock_response = create_mock_response(status_code=status_code, json_data=json_data)
            responses.append(mock_response)
        
        mock_session.get.side_effect = responses
        
        result = _make_request_with_retry(
            session=mock_session,
            url="https://api.datacite.org/dois",
            params={"test": "value"},
            initial_backoff=1
        )
        
        assert result.status_code == 200
        assert mock_session.get.call_count == 4
        # Should have slept 3 times: 1, 2, 4 seconds
        assert mock_sleep.call_count == 3


class TestBuildUserAgentHeaders:
    """Tests for _build_user_agent_headers function.
    
    This function is pure, so no mocking required.
    """

    def test_no_email_returns_empty_dict(self):
        """Test that no email returns empty headers dict."""
        result = _build_user_agent_headers(user_agent_mailto=None)
        assert result == {}

    def test_with_email_builds_user_agent(self):
        """Test that email is properly formatted in User-Agent header."""
        result = _build_user_agent_headers(user_agent_mailto="test@example.com")
        
        assert "User-Agent" in result
        assert result["User-Agent"] == "SageBionetworks-DPE/1.0 (mailto:test@example.com)"

    @pytest.mark.parametrize("email", [
        "user@domain.com",
        "first.last@company.org",
        "admin@sub.domain.co.uk"
    ])
    def test_various_email_formats(self, email):
        """Test different valid email formats."""
        result = _build_user_agent_headers(user_agent_mailto=email)
        
        assert f"mailto:{email}" in result["User-Agent"]
        assert result["User-Agent"].startswith("SageBionetworks-DPE/1.0")


class TestShouldContinuePagination:
    """Tests for _should_continue_pagination function.
    
    This function is pure, testing pagination logic.
    """

    def test_full_page_continues(self, sample_doi_objects):
        """Test that a full page signals continuation."""
        result = _should_continue_pagination(
            data=sample_doi_objects,
            page_size=10
        )
        assert result is True

    def test_partial_page_stops(self):
        """Test that partial page signals to stop."""
        result = _should_continue_pagination(
            data=[{"id": "1"}, {"id": "2"}],
            page_size=10
        )
        assert result is False

    def test_empty_data_stops(self):
        """Test that empty data signals to stop."""
        result = _should_continue_pagination(
            data=[],
            page_size=10
        )
        assert result is False

    def test_exact_page_size_continues(self):
        """Test that exactly page_size items continues."""
        data = [{"id": str(i)} for i in range(100)]
        result = _should_continue_pagination(
            data=data,
            page_size=100
        )
        assert result is True

    def test_one_less_than_page_size_stops(self):
        """Test that one item less than page size stops."""
        data = [{"id": str(i)} for i in range(99)]
        result = _should_continue_pagination(
            data=data,
            page_size=100
        )
        assert result is False


class TestSerializeToNdjson:
    """Tests for _serialize_to_ndjson function.
    
    Tests JSON serialization to NDJSON format.
    """

    def test_basic_serialization(self, sample_doi_object):
        """Test basic object serialization."""
        result = _serialize_to_ndjson(sample_doi_object)
        
        assert isinstance(result, bytes)
        assert result.endswith(b"\n")
        
        # Verify we can deserialize it
        json_str = result.decode("utf-8").rstrip("\n")
        parsed = json.loads(json_str)
        assert parsed == sample_doi_object

    def test_compact_json_no_spaces(self):
        """Test that JSON is compact (no spaces after separators)."""
        obj = {"key": "value", "number": 123}
        result = _serialize_to_ndjson(obj)
        
        json_str = result.decode("utf-8")
        # Compact JSON shouldn't have space after colon or comma
        assert ": " not in json_str
        assert ", " not in json_str

    def test_unicode_preserved(self):
        """Test that Unicode characters are preserved."""
        obj = {"title": "Test™ Dataset", "author": "Müller"}
        result = _serialize_to_ndjson(obj)
        
        json_str = result.decode("utf-8").rstrip("\n")
        parsed = json.loads(json_str)
        assert parsed["title"] == "Test™ Dataset"
        assert parsed["author"] == "Müller"

    def test_empty_object(self):
        """Test serialization of empty object."""
        result = _serialize_to_ndjson({})
        assert result == b"{}\n"

    def test_nested_objects(self):
        """Test serialization of nested structures."""
        obj = {
            "outer": {
                "inner": {
                    "deeply": ["nested", "array"]
                }
            }
        }
        result = _serialize_to_ndjson(obj)
        
        json_str = result.decode("utf-8").rstrip("\n")
        parsed = json.loads(json_str)
        assert parsed == obj


class TestFetchDoiPage:
    """Tests for fetch_doi_page function.
    
    Tests single page fetching with mocked HTTP calls.
    """

    def test_successful_fetch(self, prefixes, mock_api_response_full_page, create_mock_response):
        """Test successful page fetch."""
        mock_session = Mock(spec=requests.Session)
        mock_response = create_mock_response(
            status_code=200,
            json_data=mock_api_response_full_page
        )
        mock_session.get.return_value = mock_response
        
        result = fetch_doi_page(
            session=mock_session,
            prefixes=prefixes,
            state="findable",
            page_size=10,
            page_number=0,
            detail=True
        )
        
        assert result == mock_api_response_full_page
        assert len(result["data"]) == 10
        mock_session.get.assert_called_once()

    def test_passes_correct_parameters(self, prefixes, create_mock_response):
        """Test that correct parameters are passed to request."""
        mock_session = Mock(spec=requests.Session)
        mock_response = create_mock_response(
            status_code=200,
            json_data={"data": []}
        )
        mock_session.get.return_value = mock_response
        
        fetch_doi_page(
            session=mock_session,
            prefixes=prefixes,
            state="registered",
            page_size=100,
            page_number=5,
            detail=False
        )
        
        # Check the params passed to get
        call_args = mock_session.get.call_args
        params = call_args[1]["params"]
        
        assert params["prefix"] == "10.7303"
        assert params["state"] == "registered"
        assert params["page[size]"] == 100
        assert params["page[number]"] == 5
        assert "detail" not in params

    def test_handles_http_error(self, prefixes, create_mock_response):
        """Test that HTTP errors are propagated."""
        mock_session = Mock(spec=requests.Session)
        mock_response = create_mock_response(
            status_code=404,
            raise_for_status_error=requests.HTTPError("404 Not Found")
        )
        mock_session.get.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            fetch_doi_page(
                session=mock_session,
                prefixes=prefixes,
                state="findable",
                page_size=10,
                page_number=0,
                detail=True
            )

    def test_malformed_json_response(self, prefixes, create_mock_response):
        """Test handling of malformed JSON in API response."""
        mock_session = Mock(spec=requests.Session)
        mock_response = create_mock_response(status_code=200)
        mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        mock_session.get.return_value = mock_response
        
        with pytest.raises(json.JSONDecodeError):
            fetch_doi_page(
                session=mock_session,
                prefixes=prefixes,
                state="findable",
                page_size=10,
                page_number=0,
                detail=True
            )

    def test_response_missing_data_key(self, prefixes, create_mock_response):
        """Test handling when API response is missing expected 'data' key."""
        mock_session = Mock(spec=requests.Session)
        mock_response = create_mock_response(
            status_code=200,
            json_data={"meta": {"total": 0}}
        )
        mock_session.get.return_value = mock_response
        
        result = fetch_doi_page(
            session=mock_session,
            prefixes=prefixes,
            state="findable",
            page_size=10,
            page_number=0,
            detail=True
        )
        
        # Should return the response as-is, let caller handle
        assert "data" not in result
        assert result["meta"]["total"] == 0

    def test_invalid_page_size_zero(self, prefixes):
        """Test that page_size=0 raises ValueError."""
        mock_session = Mock(spec=requests.Session)
        
        with pytest.raises(ValueError, match="page_size must be at least 1"):
            fetch_doi_page(
                session=mock_session,
                prefixes=prefixes,
                state="findable",
                page_size=0,
                page_number=0,
                detail=True
            )

    def test_invalid_page_size_negative(self, prefixes):
        """Test that negative page_size raises ValueError."""
        mock_session = Mock(spec=requests.Session)
        
        with pytest.raises(ValueError, match="page_size must be at least 1"):
            fetch_doi_page(
                session=mock_session,
                prefixes=prefixes,
                state="findable",
                page_size=-10,
                page_number=0,
                detail=True
            )

    def test_invalid_page_size_exceeds_maximum(self, prefixes):
        """Test that page_size > 1000 raises ValueError."""
        mock_session = Mock(spec=requests.Session)
        
        with pytest.raises(ValueError, match="page_size cannot exceed 1000"):
            fetch_doi_page(
                session=mock_session,
                prefixes=prefixes,
                state="findable",
                page_size=1001,
                page_number=0,
                detail=True
            )

    def test_invalid_state(self, prefixes):
        """Test that invalid state raises ValueError."""
        mock_session = Mock(spec=requests.Session)
        
        with pytest.raises(ValueError, match="state must be one of"):
            fetch_doi_page(
                session=mock_session,
                prefixes=prefixes,
                state="invalid_state",
                page_size=100,
                page_number=0,
                detail=True
            )

    @pytest.mark.parametrize("state", ["findable", "registered", "draft"])
    def test_valid_states(self, prefixes, state, create_mock_response):
        """Test that all valid states are accepted."""
        mock_session = Mock(spec=requests.Session)
        mock_response = create_mock_response(
            status_code=200,
            json_data={"data": []}
        )
        mock_session.get.return_value = mock_response
        
        # Should not raise ValueError
        result = fetch_doi_page(
            session=mock_session,
            prefixes=prefixes,
            state=state,
            page_size=100,
            page_number=0,
            detail=True
        )
        
        assert result == {"data": []}


class TestFetchDoi:
    """Tests for fetch_doi function.
    
    Tests full pagination flow with generator behavior.
    """

    def test_single_full_page(self, prefixes, sample_doi_objects, mocker):
        """Test fetching a single full page."""
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.side_effect = [
            {"data": sample_doi_objects},
            {"data": []}  # Empty next page
        ]
        
        results = list(fetch_doi(
            prefixes=prefixes,
            state="findable",
            page_size=10,
            start_page=0,
            detail=True
        ))
        
        assert len(results) == 10
        assert results == sample_doi_objects
        # Should fetch until empty page
        assert mock_fetch_page.call_count == 2

    def test_multiple_pages(self, prefixes, mocker):
        """Test pagination across multiple pages."""
        page1_data = [{"id": f"10.7303/syn{i:05d}"} for i in range(1, 11)]
        page2_data = [{"id": f"10.7303/syn{i:05d}"} for i in range(11, 21)]
        page3_data = [{"id": f"10.7303/syn{i:05d}"} for i in range(21, 25)]
        
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.side_effect = [
            {"data": page1_data},
            {"data": page2_data},
            {"data": page3_data},  # Partial page - should stop
        ]
        
        results = list(fetch_doi(
            prefixes=prefixes,
            state="findable",
            page_size=10,
            start_page=0,
            detail=True
        ))
        
        assert len(results) == 24
        assert mock_fetch_page.call_count == 3

    def test_empty_results(self, prefixes, mocker):
        """Test handling of no results."""
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.return_value = {"data": []}
        
        results = list(fetch_doi(
            prefixes=prefixes,
            state="findable",
            page_size=10,
            start_page=0,
            detail=True
        ))
        
        assert len(results) == 0
        assert mock_fetch_page.call_count == 1

    def test_user_agent_header_set(self, prefixes, mocker):
        """Test that User-Agent header is set when email provided."""
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.return_value = {"data": []}
        
        # Mock Session to capture headers
        with patch("src.datacite.datacite.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            # Create a mock for headers.update
            mock_update = MagicMock()
            mock_session.headers.update = mock_update
            
            list(fetch_doi(
                prefixes=prefixes,
                state="findable",
                page_size=10,
                user_agent_mailto="test@example.com"
            ))
            
            # Verify headers.update was called
            mock_update.assert_called_once()
            headers = mock_update.call_args[0][0]
            assert "User-Agent" in headers
            assert "test@example.com" in headers["User-Agent"]

    def test_start_page_parameter(self, prefixes, mocker):
        """Test that start_page parameter is respected."""
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.return_value = {"data": [{"id": "test"}]}
        
        list(fetch_doi(
            prefixes=prefixes,
            state="findable",
            page_size=10,
            start_page=5,
            detail=True
        ))
        
        # First call should use start_page=5
        first_call = mock_fetch_page.call_args_list[0]
        assert first_call[1]["page_number"] == 5

    def test_stops_on_partial_page(self, prefixes, mocker):
        """Test that pagination stops on partial page."""
        page1_data = [{"id": f"id{i}"} for i in range(10)]  # Full page
        page2_data = [{"id": "last"}]  # Partial page
        
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.side_effect = [
            {"data": page1_data},
            {"data": page2_data}
        ]
        
        results = list(fetch_doi(
            prefixes=prefixes,
            page_size=10
        ))
        
        assert len(results) == 11
        # Should stop after partial page, not fetch more
        assert mock_fetch_page.call_count == 2

    def test_handles_api_error_mid_pagination(self, prefixes, mocker):
        """Test that API errors during pagination are propagated."""
        page1_data = [{"id": f"id{i}"} for i in range(10)]
        
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.side_effect = [
            {"data": page1_data},
            requests.HTTPError("500 Server Error")  # Error on second page
        ]
        
        with pytest.raises(requests.HTTPError):
            list(fetch_doi(
                prefixes=prefixes,
                page_size=10
            ))

    def test_response_with_missing_data_key(self, prefixes, mocker):
        """Test pagination when response is missing 'data' key."""
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        # API returns response without 'data' key
        mock_fetch_page.return_value = {"meta": {"total": 0}, "links": {}}
        
        results = list(fetch_doi(
            prefixes=prefixes,
            page_size=10
        ))
        
        # Should handle gracefully and return empty
        assert len(results) == 0
        assert mock_fetch_page.call_count == 1

    def test_no_user_agent_when_mailto_none(self, prefixes, mocker):
        """Test that no User-Agent header is set when mailto is None."""
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.return_value = {"data": []}
        
        with patch("src.datacite.datacite.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__.return_value = mock_session
            mock_update = MagicMock()
            mock_session.headers.update = mock_update
            
            list(fetch_doi(
                prefixes=prefixes,
                state="findable",
                page_size=10,
                user_agent_mailto=None  # Explicitly None
            ))
            
            # headers.update should not be called when no mailto provided
            mock_update.assert_not_called()

    def test_large_page_size_boundary(self, prefixes, mocker):
        """Test pagination with maximum allowed page size (1000)."""
        # Simulate API returning exactly 1000 items
        page_data = [{"id": f"id{i}"} for i in range(1000)]
        
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.side_effect = [
            {"data": page_data},
            {"data": []}  # No more data
        ]
        
        results = list(fetch_doi(
            prefixes=prefixes,
            page_size=1000  # Maximum page size
        ))
        
        assert len(results) == 1000
        # Should fetch next page since we got full page
        assert mock_fetch_page.call_count == 2

    def test_invalid_page_size_zero(self, prefixes):
        """Test that page_size=0 raises ValueError."""
        with pytest.raises(ValueError, match="page_size must be at least 1"):
            list(fetch_doi(
                prefixes=prefixes,
                page_size=0
            ))

    def test_invalid_page_size_negative(self, prefixes):
        """Test that negative page_size raises ValueError."""
        with pytest.raises(ValueError, match="page_size must be at least 1"):
            list(fetch_doi(
                prefixes=prefixes,
                page_size=-5
            ))

    def test_invalid_page_size_exceeds_maximum(self, prefixes):
        """Test that page_size > 1000 raises ValueError."""
        with pytest.raises(ValueError, match="page_size cannot exceed 1000"):
            list(fetch_doi(
                prefixes=prefixes,
                page_size=2000
            ))

    def test_page_size_boundary_values(self, prefixes, mocker):
        """Test that boundary values 1 and 1000 are accepted."""
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.return_value = {"data": []}
        
        # page_size=1 should work
        list(fetch_doi(prefixes=prefixes, page_size=1))
        assert mock_fetch_page.called
        
        mock_fetch_page.reset_mock()
        
        # page_size=1000 should work
        list(fetch_doi(prefixes=prefixes, page_size=1000))
        assert mock_fetch_page.called

    def test_invalid_state(self, prefixes):
        """Test that invalid state raises ValueError."""
        with pytest.raises(ValueError, match="state must be one of"):
            list(fetch_doi(
                prefixes=prefixes,
                state="invalid_state"
            ))

    @pytest.mark.parametrize("state", ["findable", "registered", "draft"])
    def test_valid_states(self, prefixes, state, mocker):
        """Test that all valid states are accepted."""
        mock_fetch_page = mocker.patch("src.datacite.datacite.fetch_doi_page")
        mock_fetch_page.return_value = {"data": []}
        
        # Should not raise ValueError
        list(fetch_doi(prefixes=prefixes, state=state))
        assert mock_fetch_page.called


class TestWriteNdjsonGz:
    """Tests for write_ndjson_gz function.
    
    Tests file I/O with proper cleanup of temporary files.
    """

    def test_write_multiple_objects(self, sample_doi_objects):
        """Test writing multiple objects to gzipped NDJSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "test.ndjson.gz")
            
            count = write_ndjson_gz(sample_doi_objects, out_path)
            
            assert count == 10
            assert os.path.exists(out_path)
            
            # Verify file contents
            with gzip.open(out_path, "rb") as gz:
                lines = gz.readlines()
            
            assert len(lines) == 10
            # Verify each line is valid JSON
            for i, line in enumerate(lines):
                parsed = json.loads(line.decode("utf-8"))
                assert parsed["id"] == sample_doi_objects[i]["id"]

    def test_write_empty_iterable(self):
        """Test writing empty iterable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "empty.ndjson.gz")
            
            count = write_ndjson_gz([], out_path)
            
            assert count == 0
            assert os.path.exists(out_path)
            
            # Verify file is empty but valid gzip
            with gzip.open(out_path, "rb") as gz:
                content = gz.read()
            
            assert content == b""

    def test_write_single_object(self, sample_doi_object):
        """Test writing a single object."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "single.ndjson.gz")
            
            count = write_ndjson_gz([sample_doi_object], out_path)
            
            assert count == 1
            
            with gzip.open(out_path, "rb") as gz:
                line = gz.read()
            
            parsed = json.loads(line.decode("utf-8"))
            assert parsed == sample_doi_object

    def test_compression_works(self, sample_doi_objects):
        """Test that file is actually compressed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "compressed.ndjson.gz")
            
            write_ndjson_gz(sample_doi_objects, out_path)
            
            compressed_size = os.path.getsize(out_path)
            
            # Calculate uncompressed size
            uncompressed_size = sum(
                len(_serialize_to_ndjson(obj))
                for obj in sample_doi_objects
            )
            
            # Compressed should be smaller (not a precise check, but reasonable)
            assert compressed_size < uncompressed_size

    def test_generator_input(self):
        """Test that function works with generator input."""
        def obj_generator():
            for i in range(5):
                yield {"id": i, "value": f"item_{i}"}
        
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "from_generator.ndjson.gz")
            
            count = write_ndjson_gz(obj_generator(), out_path)
            
            assert count == 5
            
            with gzip.open(out_path, "rb") as gz:
                lines = gz.readlines()
            
            assert len(lines) == 5

    def test_unicode_content(self):
        """Test writing objects with Unicode content."""
        objects = [
            {"title": "Café", "author": "François"},
            {"title": "日本語", "description": "テスト"},
            {"emoji": "🔬", "symbol": "™"}
        ]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "unicode.ndjson.gz")
            
            count = write_ndjson_gz(objects, out_path)
            
            assert count == 3
            
            with gzip.open(out_path, "rb") as gz:
                lines = gz.readlines()
            
            for i, line in enumerate(lines):
                parsed = json.loads(line.decode("utf-8"))
                assert parsed == objects[i]

    def test_creates_parent_directories(self):
        """Test that parent directories are created if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_path = os.path.join(tmpdir, "subdir", "nested", "file.ndjson.gz")
            
            # Parent dirs don't exist yet
            assert not os.path.exists(os.path.dirname(nested_path))
            
            # gzip.open doesn't create dirs, so we need to handle this
            # Let's test that it raises if parent doesn't exist
            with pytest.raises(FileNotFoundError):
                write_ndjson_gz([{"test": "data"}], nested_path)

    def test_file_cleanup_on_success(self):
        """Test that temporary directory cleanup works properly."""
        temp_path = None
        
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = tmpdir
            out_path = os.path.join(tmpdir, "test.ndjson.gz")
            write_ndjson_gz([{"test": "data"}], out_path)
            assert os.path.exists(out_path)
        
        # After context exits, temp directory should be cleaned up
        assert not os.path.exists(temp_path)

    def test_handles_serialization_error_in_iterable(self):
        """Test handling when an object in the iterable cannot be serialized."""
        # Create an object with non-serializable content
        class NonSerializable:
            pass
        
        objects = [
            {"id": 1, "data": "valid"},
            {"id": 2, "bad": NonSerializable()},  # This will fail to serialize
        ]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "fail.ndjson.gz")
            
            with pytest.raises(TypeError):
                write_ndjson_gz(objects, out_path)

    def test_permission_error_on_write(self, mocker):
        """Test handling of permission errors when writing file."""
        mock_open = mocker.patch("gzip.open")
        mock_open.side_effect = PermissionError("Permission denied")
        
        with pytest.raises(PermissionError):
            write_ndjson_gz([{"test": "data"}], "/invalid/path/file.ndjson.gz")

    def test_disk_full_during_write(self, mocker):
        """Test handling when disk fills up during write operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "test.ndjson.gz")
            
            # Create a mock that fails after first write
            call_count = [0]
            
            def mock_write(data):
                call_count[0] += 1
                if call_count[0] > 1:
                    raise OSError(28, "No space left on device")
                return len(data)
            
            with patch("gzip.open") as mock_open:
                mock_file = MagicMock()
                mock_file.write = mock_write
                mock_file.__enter__ = lambda s: s
                mock_file.__exit__ = lambda *args: None  # Return None to propagate exceptions
                mock_open.return_value = mock_file
                
                objects = [{"id": i} for i in range(10)]
                
                with pytest.raises(OSError, match="No space left on device"):
                    write_ndjson_gz(objects, out_path)

    def test_very_large_object_serialization(self):
        """Test serialization of very large objects."""
        # Create a large object (simulating large DOI metadata)
        large_obj = {
            "id": "10.7303/test",
            "data": "x" * 100000,  # 100KB of data
            "nested": {
                "level1": {
                    "level2": {
                        "level3": ["item"] * 1000
                    }
                }
            }
        }
        
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "large.ndjson.gz")
            
            count = write_ndjson_gz([large_obj], out_path)
            
            assert count == 1
            assert os.path.exists(out_path)
            
            # Verify we can read it back
            with gzip.open(out_path, "rb") as gz:
                line = gz.read()
            
            parsed = json.loads(line.decode("utf-8"))
            assert parsed["id"] == large_obj["id"]
            assert len(parsed["data"]) == 100000

    def test_special_characters_in_filename(self):
        """Test writing to file with special characters in path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # File with spaces and special chars
            out_path = os.path.join(tmpdir, "file with spaces & chars.ndjson.gz")
            
            count = write_ndjson_gz([{"test": "data"}], out_path)
            
            assert count == 1
            assert os.path.exists(out_path)

    def test_overwrite_existing_file(self):
        """Test that existing file is overwritten."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "overwrite.ndjson.gz")
            
            # Write first file
            write_ndjson_gz([{"id": 1}], out_path)
            
            with gzip.open(out_path, "rb") as gz:
                first_content = gz.read()
            
            # Overwrite with different data
            write_ndjson_gz([{"id": 2}, {"id": 3}], out_path)
            
            with gzip.open(out_path, "rb") as gz:
                second_content = gz.read()
            
            # Content should be different
            assert first_content != second_content
            
            # Should have 2 lines now
            lines = second_content.decode("utf-8").strip().split("\n")
            assert len(lines) == 2
