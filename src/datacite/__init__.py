"""DataCite utilities package."""
from .datacite import (
    fetch_doi,
    fetch_doi_page,
    write_ndjson_gz,
)

__all__ = [
    "fetch_doi",
    "fetch_doi_page",
    "write_ndjson_gz",
]
