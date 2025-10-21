"""DataCite DOI fetching and storage utilities.

This module provides functionality for retrieving DOI (Digital Object Identifier)
metadata from the DataCite REST API and saving it to compressed NDJSON files.
It handles pagination, retry logic, and efficient streaming to minimize memory usage.

Primary Entrypoint:
    fetch_doi: Iterator that automatically fetches all DOI objects matching the
        specified criteria. This is the main function most users will need.

Public Functions:
    fetch_doi: Fetch all DOI objects from DataCite API with automatic pagination.
        Returns an iterator yielding individual DOI objects. Handles session
        management and pagination transparently.

    fetch_doi_page: Fetch a single page of DOI objects. Useful for manual
        pagination control or when you only need a specific page of results.

    write_ndjson_gz: Write an iterable of objects to a gzipped newline-delimited
        JSON file. Streams data to disk without loading everything into memory.

Example:
    Basic usage to fetch and save Synapse DOIs:

        >>> from datacite import fetch_doi, write_ndjson_gz
        >>> 
        >>> # Fetch all findable DOIs with prefix 10.7303
        >>> dois = fetch_doi(
        ...     prefixes=["10.7303"],
        ...     state="findable",
        ...     user_agent_mailto="user@example.com"
        ... )
        >>> 
        >>> # Save to compressed NDJSON file
        >>> count = write_ndjson_gz(dois, "synapse_dois.ndjson.gz")
        >>> print(f"Saved {count} DOI records")

    Advanced usage with custom pagination:

        >>> import requests
        >>> from datacite import fetch_doi_page
        >>> 
        >>> with requests.Session() as session:
        ...     # Fetch only the first page
        ...     result = fetch_doi_page(
        ...         session=session,
        ...         prefixes=["10.7303"],
        ...         state="findable",
        ...         page_size=100,
        ...         page_number=0,
        ...         detail=True
        ...     )
        ...     print(f"Got {len(result['data'])} DOIs")

Notes:
    - DataCite provides higher rate limits (1000 requests per 5 minutes) when
      you include an email address in the User-Agent header via user_agent_mailto.
    - The API uses zero-based pagination.
    - Maximum page size is 1000 items.
    - All `fetch_*` functions use exponential backoff for retryable errors.
"""
import gzip
import json
import time
from typing import Iterable, Dict, Any, Optional, List

import requests

DATACITE_API = "https://api.datacite.org/dois"


def _build_query_params(
    prefixes: List[str],
    state: str,
    page_size: int,
    page_number: int,
    detail: bool,
) -> Dict[str, Any]:
    """Build query parameters for DataCite API request.

    Args:
        prefixes: List of DataCite DOI prefixes to query (e.g., ["10.7303"]).
        state: DOI state to filter by ("findable", "registered", or "draft").
        page_size: Number of results to return per page.
        page_number: Zero-based page number for pagination.
        detail: Whether to include detailed metadata in the response.

    Returns:
        Dictionary of query parameters formatted for the DataCite API.
    """
    params = {
        "prefix": ",".join(prefixes),
        "state": state,
        "page[size]": page_size,
        # Design doc example shows 0-based; we follow it.
        "page[number]": page_number,
    }
    if detail:
        params["detail"] = "true"
    return params


def _make_request_with_retry(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: int = 60,
    initial_backoff: int = 2,
    max_backoff: int = 60,
) -> requests.Response:
    """Make HTTP GET request with exponential backoff for retryable errors.

    Automatically retries requests on rate limiting (429) and server errors
    (500, 502, 503, 504) using exponential backoff strategy.

    Args:
        session: Requests session object for making HTTP calls.
        url: URL to send the GET request to.
        params: Query parameters to include in the request.
        timeout: Request timeout in seconds. Defaults to 60.
        initial_backoff: Initial backoff delay in seconds. Defaults to 2.
        max_backoff: Maximum backoff delay in seconds. Defaults to 60.

    Returns:
        Successful HTTP response object with status code 200.

    Raises:
        requests.HTTPError: For non-retryable HTTP errors (e.g., 400, 401, 404).
    """
    backoff = initial_backoff
    while True:
        resp = session.get(url, params=params, timeout=timeout)
        if resp.status_code == 200:
            return resp
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            continue
        resp.raise_for_status()


def _build_user_agent_headers(user_agent_mailto: Optional[str] = None) -> Dict[str, str]:
    """Build HTTP headers with User-Agent for DataCite API.

    DataCite provides higher rate limits (1000 requests per 5 minutes) for
    identified unauthenticated users who include an email in the User-Agent.

    Args:
        user_agent_mailto: Email address to include in User-Agent header
            for increased rate limits. If None, no User-Agent is set.

    Returns:
        Dictionary of HTTP headers. Empty if no email provided, otherwise
        contains User-Agent header with format:
        "SageBionetworks-DPE/1.0 (mailto:email@example.com)".
    """
    headers = {}
    # Identified unauthenticated users get 1000 req/5min when User-Agent includes mailto.
    # Example: "Sage-DPE/1.0 (mailto:phil.snyder@sagebase.org)"
    if user_agent_mailto:
        headers["User-Agent"] = f"SageBionetworks-DPE/1.0 (mailto:{user_agent_mailto})"
    return headers


def _should_continue_pagination(data: List[Dict[str, Any]], page_size: int) -> bool:
    """Determine if pagination should continue based on returned data.

    Pagination stops when either no data is returned or a short page
    (fewer items than page_size) is received, indicating the last page.

    Args:
        data: List of data items returned from the current page.
        page_size: Expected number of items per full page.

    Returns:
        True if pagination should continue (full page received),
        False if this is the last page or no data was returned.
    """
    return bool(data) and len(data) >= page_size


def _serialize_to_ndjson(obj: Dict[str, Any]) -> bytes:
    """Serialize a single object to newline-delimited JSON bytes.

    Converts a Python dictionary to a compact JSON string (no spaces),
    encodes to UTF-8, and appends a newline character for NDJSON format.

    Args:
        obj: Dictionary object to serialize.

    Returns:
        UTF-8 encoded bytes containing JSON data followed by newline.
    """
    line = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    return line.encode("utf-8") + b"\n"


def fetch_doi_page(
    session: requests.Session,
    prefixes: List[str],
    state: str,
    page_size: int,
    page_number: int,
    detail: bool,
) -> Dict[str, Any]:
    """Fetch a single page of DOI objects from DataCite API.

    Args:
        session: Requests session object with any required headers pre-configured.
        prefixes: List of DataCite DOI prefixes to query (e.g., ["10.7303"]).
        state: DOI state to filter by ("findable", "registered", or "draft").
        page_size: Number of results to return per page.
        page_number: Zero-based page number for pagination.
        detail: Whether to include detailed metadata in the response.

    Returns:
        Dictionary containing the API response with "data" key holding list
        of DOI objects and pagination metadata.

    Raises:
        requests.HTTPError: If the API request fails with non-retryable error.
    """
    params = _build_query_params(prefixes, state, page_size, page_number, detail)
    resp = _make_request_with_retry(session, DATACITE_API, params)
    return resp.json()


def fetch_doi(
    prefixes: List[str],
    state: str = "findable",
    page_size: int = 1000,
    start_page: int = 0,
    detail: bool = True,
    user_agent_mailto: Optional[str] = None,
) -> Iterable[Dict[str, Any]]:
    """Iterate through all pages of DOI objects from DataCite API.

    Creates a session and automatically handles pagination, yielding individual
    DOI objects one at a time. Stops when all pages have been retrieved.

    Args:
        prefixes: List of DataCite DOI prefixes to query (e.g., ["10.7303"]).
        state: DOI state to filter by. Defaults to "findable".
        page_size: Number of results per page. Defaults to 1000 (API maximum).
        start_page: Zero-based page number to start from. Defaults to 0.
        detail: Whether to include detailed metadata. Defaults to True.
        user_agent_mailto: Email address for User-Agent header to get
            higher rate limits. Defaults to None.

    Yields:
        Individual DOI objects as dictionaries, one at a time from all pages.

    Raises:
        requests.HTTPError: If any API request fails with non-retryable error.

    Example:
        >>> for doi in fetch_doi(["10.7303"], user_agent_mailto="user@example.com"):
        ...     print(doi["id"])
    """
    headers = _build_user_agent_headers(user_agent_mailto)

    with requests.Session() as s:
        if headers:
            s.headers.update(headers)

        page_number = start_page
        while True:
            payload = fetch_doi_page(
                s,
                prefixes=prefixes,
                state=state,
                page_size=page_size,
                page_number=page_number,
                detail=detail,
            )
            data = payload.get("data", [])
            if not data:
                break
            yield from data
            # Stop when we returned a short page
            if not _should_continue_pagination(data, page_size):
                break
            page_number += 1


def write_ndjson_gz(objs: Iterable[Dict[str, Any]], out_path: str) -> int:
    """Write objects to gzipped newline-delimited JSON file.

    Streams objects to a gzip-compressed NDJSON file without loading all
    data into memory. Each object is serialized to JSON on a separate line.

    Args:
        objs: Iterable of dictionary objects to write.
        out_path: File path for the output .ndjson.gz file.

    Returns:
        Total count of objects written to the file.

    Example:
        >>> data = [{"id": 1}, {"id": 2}]
        >>> count = write_ndjson_gz(data, "output.ndjson.gz")
        >>> print(f"Wrote {count} objects")
        Wrote 2 objects
    """
    count = 0
    # Stream to gzip without holding all in memory
    with gzip.open(out_path, "wb") as gz:
        for obj in objs:
            gz.write(_serialize_to_ndjson(obj))
            count += 1
    return count
