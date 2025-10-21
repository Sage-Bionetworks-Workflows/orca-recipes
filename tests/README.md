# Test Suites

This directory contains test suites for various modules in the orca-recipes project.

## All Test Suites

<details>
<summary><strong>DataCite Test Suite</strong></summary>

## Overview
Comprehensive pytest test suite for the `datacite` module covering all functions, including extensive real-world edge case scenarios.

## Test Structure

### Files
```
tests/
├── __init__.py                          # Tests package init
├── test_datacite.py                     # Main test file
└── fixtures/
    ├── __init__.py                      # Fixtures package init
    └── datacite_fixtures.py             # All test fixtures

src/
└── datacite/
    ├── __init__.py                      # DataCite package init
    └── datacite.py                      # Main module
```

## Test Organization

Tests are organized into classes by function:

- **TestBuildQueryParams** - Query parameter building logic
- **TestMakeRequestWithRetry** - HTTP retry logic with exponential backoff, network exceptions
- **TestBuildUserAgentHeaders** - User-Agent header formatting
- **TestShouldContinuePagination** - Pagination decision logic
- **TestSerializeToNdjson** - JSON serialization to NDJSON format
- **TestFetchDoiPage** - Single page fetching from API, malformed responses
- **TestFetchDoi** - Full pagination flow, generator behavior, mid-pagination errors
- **TestWriteNdjsonGz** - File I/O with gzip compression, file system errors

### Real-World Edge Cases Covered

The test suite includes comprehensive coverage of real-world scenarios that may occur in production:

**Network Layer:**
- Timeout exceptions during API requests
- Connection errors (DNS failures, network unreachable)
- Mixed retryable HTTP errors (429, 503, 500) with recovery
- Exponential backoff retry behavior

**API Response Variations:**
- Malformed JSON responses
- Missing expected keys in API responses
- API errors occurring mid-pagination
- Empty and partial page responses
- Maximum page size boundaries (1000 items)

**File System Issues:**
- Permission denied when writing files
- Disk full during write operations
- Serialization errors for non-JSON-serializable objects
- Very large object serialization (100KB+ objects)
- Special characters in file paths
- Overwriting existing files

**Pagination Edge Cases:**
- Stopping on partial pages
- Starting pagination from arbitrary page numbers
- Handling missing data keys during iteration
- User-Agent header behavior with and without mailto parameter

## Fixtures 

All fixtures are in `tests/fixtures/datacite_fixtures.py`.

### Base Fixtures (Reusable Components)
- `base_doi_attributes`: Common DOI attribute fields shared across fixtures
- `base_doi_relationships`: Common relationship structure (client, provider, references)

### DOI Object Fixtures
- `sample_doi_object`: Complete DOI object with all fields (uses base fixtures)
- `sample_doi_objects`: List of minimal DOI objects for pagination testing

### Prefix Fixtures
- `prefixes`: Single prefix for testing
- `multiple_prefixes`: Multiple prefixes for testing

### API Response Fixtures
- `mock_api_response_full_page`: Full page response
- `mock_api_response_partial_page`: Partial page response
- `mock_api_response_empty`: Empty response

**Note:** Fixtures are designed to reuse common components to reduce redundancy. The `_create_minimal_doi()` helper function is used internally for creating test DOI objects.

## Running Tests

```bash
# Run all DataCite tests
pytest tests/test_datacite.py -v

# Run specific test class
pytest tests/test_datacite.py::TestBuildQueryParams -v

# Run specific test
pytest tests/test_datacite.py::TestBuildQueryParams::test_basic_params -v

# Run with coverage
pytest tests/test_datacite.py --cov=src.datacite --cov-report=term-missing
```
</details>

---

## Running All Tests

```bash
# Run all tests in the tests directory
pytest tests/ -v

# Run with coverage for all modules
pytest tests/ --cov=src --cov-report=term-missing

# Run tests in parallel (if pytest-xdist is installed)
pytest tests/ -n auto
```
