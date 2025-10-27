"""Fixtures for datacite module tests."""
import pytest
from typing import Dict, Any, List, Callable, Optional
from unittest.mock import Mock
import requests


@pytest.fixture
def base_doi_attributes() -> Dict[str, Any]:
    """Base DOI attributes template shared across fixtures.
    
    Returns:
        Dictionary with common DOI attribute fields.
    """
    return {
        "identifiers": [],
        "alternateIdentifiers": [],
        "container": {},
        "subjects": [],
        "contributors": [],
        "dates": [{"date": "2024", "dateType": "Issued"}],
        "language": None,
        "relatedIdentifiers": [],
        "relatedItems": [],
        "sizes": [],
        "formats": [],
        "version": None,
        "rightsList": [],
        "descriptions": [],
        "geoLocations": [],
        "fundingReferences": [],
        "contentUrl": None,
        "metadataVersion": 0,
        "schemaVersion": "http://datacite.org/schema/kernel-4",
        "source": "mds",
        "isActive": True,
        "reason": None,
        "viewCount": 0,
        "viewsOverTime": [],
        "downloadCount": 0,
        "downloadsOverTime": [],
        "referenceCount": 0,
        "citationCount": 0,
        "citationsOverTime": [],
        "partCount": 0,
        "partOfCount": 0,
        "versionCount": 0,
        "versionOfCount": 0,
    }


@pytest.fixture
def base_doi_relationships() -> Dict[str, Any]:
    """Base DOI relationships template.
    
    Returns:
        Dictionary with common DOI relationship fields.
    """
    return {
        "client": {"data": {"id": "sagebio.synapse", "type": "clients"}},
        "provider": {"data": {"id": "sagebio", "type": "providers"}},
        "references": {"data": []},
        "citations": {"data": []},
        "parts": {"data": []},
        "partOf": {"data": []},
        "versions": {"data": []},
        "versionOf": {"data": []}
    }


@pytest.fixture
def sample_doi_object(base_doi_attributes, base_doi_relationships) -> Dict[str, Any]:
    """Single DOI object matching DataCite API structure.
    
    Args:
        base_doi_attributes: Base attribute fields shared across DOI objects.
        base_doi_relationships: Base relationship fields.
    
    Returns:
        Dictionary representing a single DOI object from DataCite API.
    """
    return {
        "id": "10.7303/syn12345",
        "type": "dois",
        "attributes": {
            **base_doi_attributes,
            "doi": "10.7303/syn12345",
            "prefix": "10.7303",
            "suffix": "syn12345",
            "creators": [
                {
                    "name": "Test Author",
                    "nameType": "Personal",
                    "givenName": "Test",
                    "familyName": "Author",
                    "affiliation": [],
                    "nameIdentifiers": []
                }
            ],
            "titles": [{"title": "Test Dataset"}],
            "publisher": "Synapse",
            "publicationYear": 2024,
            "types": {
                "ris": "GEN",
                "bibtex": "misc",
                "citeproc": "article",
                "schemaOrg": "Dataset",
                "resourceTypeGeneral": "Dataset"
            },
            "url": "https://repo-prod.prod.sagebase.org/repo/v1/doi/locate?id=syn12345&type=ENTITY",
            "state": "findable",
            "created": "2024-01-15T10:30:00Z",
            "registered": "2024-01-15T10:30:00Z",
            "published": None,
            "updated": "2024-01-15T10:30:00Z"
        },
        "relationships": base_doi_relationships
    }


def _create_minimal_doi(doi_id: str, suffix: str, title: str, author: str) -> Dict[str, Any]:
    """Helper function to create minimal DOI object for testing.
    
    Args:
        doi_id: Full DOI identifier.
        suffix: DOI suffix (after prefix).
        title: Dataset title.
        author: Creator name.
        
    Returns:
        Minimal DOI object with essential fields for testing.
    """
    return {
        "id": doi_id,
        "type": "dois",
        "attributes": {
            "doi": doi_id,
            "prefix": "10.7303",
            "suffix": suffix,
            "creators": [{"name": author, "nameType": "Personal"}],
            "titles": [{"title": title}],
            "publisher": "Synapse",
            "publicationYear": 2024,
            "types": {"resourceTypeGeneral": "Dataset"},
            "state": "findable",
            "created": "2024-01-15T10:30:00Z",
            "updated": "2024-01-15T10:30:00Z"
        }
    }


@pytest.fixture
def sample_doi_objects() -> List[Dict[str, Any]]:
    """Multiple DOI objects for pagination testing.
    
    Returns:
        List of DOI objects with varying IDs.
    """
    return [
        _create_minimal_doi(
            doi_id=f"10.7303/syn{i:05d}",
            suffix=f"syn{i:05d}",
            title=f"Dataset {i}",
            author=f"Author {i}"
        )
        for i in range(1, 11)
    ]


@pytest.fixture
def mock_api_response_full_page(sample_doi_objects) -> Dict[str, Any]:
    """Full page API response (page size matches data length).
    
    Args:
        sample_doi_objects: Fixture providing sample DOI objects.
        
    Returns:
        Dictionary representing DataCite API response with full page.
    """
    return {
        "data": sample_doi_objects,
        "meta": {
            "total": 25,
            "totalPages": 3,
            "page": 0,
            "pageSize": 10
        },
        "links": {
            "self": "https://api.datacite.org/dois?page[number]=0",
            "next": "https://api.datacite.org/dois?page[number]=1"
        }
    }


@pytest.fixture
def mock_api_response_partial_page() -> Dict[str, Any]:
    """Partial page API response (fewer items than page size).
    
    Returns:
        Dictionary representing DataCite API response with partial page.
    """
    return {
        "data": [
            _create_minimal_doi(
                doi_id="10.7303/syn00001",
                suffix="syn00001",
                title="Last Dataset",
                author="Last Author"
            ),
            _create_minimal_doi(
                doi_id="10.7303/syn00002",
                suffix="syn00002",
                title="Final Dataset",
                author="Final Author"
            )
        ],
        "meta": {
            "total": 12,
            "totalPages": 2,
            "page": 1,
            "pageSize": 10
        },
        "links": {
            "self": "https://api.datacite.org/dois?page[number]=1"
        }
    }


@pytest.fixture
def mock_api_response_empty() -> Dict[str, Any]:
    """Empty API response.
    
    Returns:
        Dictionary representing DataCite API response with no results.
    """
    return {
        "data": [],
        "meta": {
            "total": 0,
            "totalPages": 0,
            "page": 0,
            "pageSize": 10
        }
    }


@pytest.fixture
def prefixes() -> List[str]:
    """Sample DOI prefixes for testing.
    
    Returns:
        List of DataCite DOI prefixes.
    """
    return ["10.7303"]


@pytest.fixture
def multiple_prefixes() -> List[str]:
    """Multiple DOI prefixes for testing.
    
    Returns:
        List of multiple DataCite DOI prefixes.
    """
    return ["10.7303", "10.5281"]


@pytest.fixture
def create_mock_response() -> Callable:
    """Factory fixture for creating mock Response objects with url attribute.
    
    Returns:
        Function that creates properly configured mock Response objects.
    """
    def _create_mock_response(
        status_code: int = 200,
        url: str = "https://api.datacite.org/dois",
        json_data: Optional[Dict[str, Any]] = None,
        raise_for_status_error: Optional[Exception] = None,
        **kwargs: Any
    ) -> Mock:
        """Create a mock Response object with url attribute.
        
        Args:
            status_code: HTTP status code to return.
            url: URL to assign to the response.
            json_data: Optional data to return from .json() call.
            raise_for_status_error: Optional exception to raise from raise_for_status().
            **kwargs: Additional attributes to set on the mock Response object.
            
        Returns:
            Mock Response object with url attribute and configured behavior.
        
        Example:
            # Add custom attributes like headers, text, content, etc.
            mock_response = create_mock_response(
                status_code=200,
                headers={"Content-Type": "application/json"},
                text="response text",
                elapsed=timedelta(seconds=1.5)
            )
        """
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = status_code
        if mock_response.status_code >= 200 and mock_response.status_code < 400:
            mock_response.ok = True
        else:
            mock_response.ok = False
        mock_response.url = url
        
        if json_data is not None:
            mock_response.json.return_value = json_data
            
        if raise_for_status_error is not None:
            mock_response.raise_for_status.side_effect = raise_for_status_error
        
        # Set any additional attributes passed via kwargs
        for key, value in kwargs.items():
            setattr(mock_response, key, value)
        
        return mock_response
    
    return _create_mock_response
