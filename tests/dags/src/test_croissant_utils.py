"""Tests for dags/src/croissant_utils.py."""

from typing import Dict, List, Optional

from pandas import DataFrame
from synapseclient import Entity
from synapseclient.models import File
from synapseclient.models.file import FileHandle

from dags.src.croissant_utils import (
    construct_bibtex_citation,
    construct_creators,
    construct_distribution_section_for_files,
    construct_record_set_section_for_files,
    construct_usage_info,
    build_croissant_metadata,
)


def _make_file(
    file_id: str,
    name: str,
    version_number: int = 1,
    description: Optional[str] = None,
    annotations: Optional[Dict[str, List[str]]] = None,
    file_handle: Optional[FileHandle] = None,
) -> File:
    return File(
        id=file_id,
        name=name,
        version_number=version_number,
        description=description,
        annotations=annotations or {},
        file_handle=file_handle,
    )


class TestConstructDistributionSectionForFiles:
    """Tests for construct_distribution_section_for_files."""

    def test_uses_md5_and_content_type_from_dataframe(self) -> None:
        """A known ID/version in the DataFrame supplies the md5 and content type."""
        file = _make_file(file_id="syn123", name="data.csv", version_number=2)
        file_md5_and_types = DataFrame(
            {"ID": [123], "CONTENTMD5": ["abc123"],
                "CONTENT_TYPE": ["text/csv"]}
        )

        # When building the distribution section for a file with matching Snowflake data
        result = construct_distribution_section_for_files(
            [file], file_md5_and_types)

        # Then the md5 and content type from the DataFrame are used
        assert result == [
            {
                "@type": "cr:FileObject",
                "@id": "syn123.2",
                "name": "data.csv",
                "description": "Data file associated with data.csv",
                "contentUrl": "https://www.synapse.org/Synapse:syn123.2",
                "encodingFormat": "text/csv",
                "md5": "abc123",
            }
        ]

    def test_missing_md5_omits_md5_key(self) -> None:
        """When the file is not present in the DataFrame, no md5 key is emitted."""
        file = _make_file(file_id="syn999", name="data.csv")
        file_md5_and_types = DataFrame(
            {"ID": [123], "CONTENTMD5": ["abc123"],
                "CONTENT_TYPE": ["text/csv"]}
        )

        # When the file's ID has no matching row in the DataFrame
        result = construct_distribution_section_for_files(
            [file], file_md5_and_types)

        # Then the md5 key is absent rather than a placeholder value
        assert "md5" not in result[0]

    def test_empty_dataframe_omits_md5_key(self) -> None:
        """An empty DataFrame is handled without a lookup error."""
        file = _make_file(file_id="syn123", name="data.csv")

        # When there is no Snowflake data to look up
        result = construct_distribution_section_for_files([file], DataFrame())

        # Then no md5 key is emitted
        assert "md5" not in result[0]

    def test_not_set_content_type_falls_back_to_file_handle_name(self) -> None:
        """A 'NOT_SET' content type falls back to guessing from the file handle name."""
        file = _make_file(
            file_id="syn123",
            name="data",
            file_handle=FileHandle(file_name="data.json"),
        )
        file_md5_and_types = DataFrame(
            {"ID": [123], "CONTENTMD5": ["abc123"],
                "CONTENT_TYPE": ["NOT_SET"]}
        )

        # When the Snowflake content type is the sentinel "NOT_SET" value
        result = construct_distribution_section_for_files(
            [file], file_md5_and_types)

        # Then the content type is guessed from the file handle's file name
        assert result[0]["encodingFormat"] == "application/json"

    def test_falls_back_to_file_name_without_file_handle(self) -> None:
        """Without a file handle, the content type is guessed from the file's own name."""
        file = _make_file(file_id="syn123", name="data.json")

        # When there is no file handle and no Snowflake data
        result = construct_distribution_section_for_files([file], DataFrame())

        # Then the content type is guessed from the file's name
        assert result[0]["encodingFormat"] == "application/json"

    def test_unknown_extension_defaults_to_octet_stream(self) -> None:
        """An unrecognized extension defaults to application/octet-stream."""
        file = _make_file(file_id="syn123", name="data.unknownext")

        # When the file's extension can't be resolved to a content type
        result = construct_distribution_section_for_files([file], DataFrame())

        # Then it falls back to the generic binary content type
        assert result[0]["encodingFormat"] == "application/octet-stream"

    def test_uses_default_description_when_missing(self) -> None:
        """A file without a description gets a generated one."""
        file = _make_file(file_id="syn123", name="data.csv", description=None)

        # When the file has no description
        result = construct_distribution_section_for_files([file], DataFrame())

        # Then a default description referencing the file name is used
        assert result[0]["description"] == "Data file associated with data.csv"


class TestConstructRecordSetSectionForFiles:
    """Tests for construct_record_set_section_for_files."""

    def test_collects_unique_annotation_keys_sorted_case_insensitively(self) -> None:
        """Annotation keys across files are deduplicated and sorted ignoring case."""
        file_a = _make_file(file_id="syn1", name="a", annotations={"Zebra": ["1"]})
        file_b = _make_file(file_id="syn2", name="b", annotations={
                            "apple": ["2"], "Zebra": ["3"]})

        # When building the record set for files with overlapping annotation keys
        result = construct_record_set_section_for_files([file_a, file_b])

        # Then each unique key appears once, sorted case-insensitively, case preserved
        field_names = [field["name"] for field in result["field"]]
        assert field_names == ["apple", "Zebra"]

    def test_no_files_produces_empty_field_list(self) -> None:
        """No files attached means an empty field list."""
        # When there are no files to derive fields from
        result = construct_record_set_section_for_files([])

        # Then the record set has no fields
        assert not result["field"]


class TestConstructBibtexCitation:
    """Tests for construct_bibtex_citation."""

    def test_uses_title_when_present(self) -> None:
        """The dataset's title is preferred over its name."""
        dataset = Entity(
            name="fallback-name", title=["My Dataset"], createdOn="2025-02-01T12:00:00.000Z")

        # When the dataset has a title
        result = construct_bibtex_citation(dataset, "syn123", "1")

        # Then the title is used, not the name
        assert "title = {My Dataset}" in result
        assert "fallback-name" not in result

    def test_falls_back_to_name_without_title(self) -> None:
        """Without a title, the dataset's name is used instead."""
        dataset = Entity(name="my-dataset")

        # When the dataset has no title
        result = construct_bibtex_citation(dataset, "syn123", "1")

        # Then the name is used as the title
        assert "title = {my-dataset}" in result

    def test_omits_fields_that_are_absent(self) -> None:
        """Fields with no value (e.g. no createdOn, no creator) are left out entirely."""
        dataset = Entity(name="my-dataset")

        # When the dataset has no createdOn or creator
        result = construct_bibtex_citation(dataset, "syn123", "1")

        # Then no year or author line is emitted
        assert "year" not in result
        assert "author" not in result

    def test_creator_list_uses_first_entry(self) -> None:
        """A list-valued creator uses the first entry as the BibTeX author."""
        dataset = Entity(name="my-dataset",
                         creator=["Ada Lovelace", "Alan Turing"])

        # When the dataset's creator is a list
        result = construct_bibtex_citation(dataset, "syn123", "1")

        # Then the first creator is used as the author
        assert "author = {Ada Lovelace}" in result


class TestConstructUsageInfo:
    """Tests for construct_usage_info."""

    def test_no_modifiers_returns_none(self) -> None:
        """No DUO modifiers returns None so the property is dropped."""
        # When there are no DUO modifiers to convert
        result = construct_usage_info(None)

        # Then None is returned
        assert result is None

    def test_single_string_modifier_is_expanded(self) -> None:
        """A single DUO CURIE string (not a list) is handled."""
        # When passing a single DUO code as a bare string
        result = construct_usage_info("DUO:0000042")

        # Then it is expanded into a one-item DefinedTerm list
        assert result == [
            {
                "@type": "DefinedTerm",
                "name": "general research use",
                "termCode": "DUO_0000042",
                "url": "duo:0000042",
            }
        ]

    def test_underscore_separator_is_normalized(self) -> None:
        """A DUO code using an underscore separator resolves the same as one using a colon."""
        # When the DUO code uses an underscore instead of a colon
        result = construct_usage_info(["DUO_0000042"])

        # Then it is normalized and resolves the mapped name
        assert result[0]["name"] == "general research use"

    def test_unmapped_code_falls_back_to_raw_value(self) -> None:
        """A DUO code not in DUO_CODE_TO_NAME falls back to the raw code as the name."""
        # When the DUO code has no entry in the map
        result = construct_usage_info(["DUO:9999999"])

        # Then the raw code is used as the name
        assert result[0]["name"] == "DUO:9999999"


class TestConstructCreators:
    """Tests for construct_creators."""

    def test_no_creator_returns_none(self) -> None:
        """A dataset without a creator returns None."""
        dataset = Entity(name="my-dataset")

        # When the dataset has no creator
        result = construct_creators(dataset)

        # Then None is returned
        assert result is None

    def test_string_creator_becomes_single_item_list(self) -> None:
        """A single string creator is wrapped in a one-item list."""
        dataset = Entity(name="my-dataset", creator="Ada Lovelace")

        # When the creator is a bare string rather than a list
        result = construct_creators(dataset)

        # Then it is wrapped as a single Person entry
        assert result == [{"@type": "sc:Person", "name": "Ada Lovelace"}]

    def test_list_creator_becomes_multiple_persons(self) -> None:
        """A list of creators becomes one Person entry per creator."""
        dataset = Entity(name="my-dataset",
                         creator=["Ada Lovelace", "Alan Turing"])

        # When the dataset has multiple creators
        result = construct_creators(dataset)

        # Then each creator becomes its own Person entry
        assert result == [
            {"@type": "sc:Person", "name": "Ada Lovelace"},
            {"@type": "sc:Person", "name": "Alan Turing"},
        ]

    def test_empty_entries_are_filtered_and_all_empty_returns_none(self) -> None:
        """Falsy entries in the creator list are dropped; an all-empty list returns None."""
        dataset = Entity(name="my-dataset", creator=["", None])

        # When every creator entry is falsy
        result = construct_creators(dataset)

        # Then they are filtered out and None is returned rather than an empty list
        assert result is None


class TestBuildCroissantMetadata:
    """Tests for build_croissant_metadata."""

    def test_builds_expected_structure(self) -> None:
        """The assembled document contains the context, metadata FileObject, and per-file entries."""
        dataset = Entity(
            name="my-dataset",
            createdOn="2025-02-01T12:00:00.000Z",
            modifiedOn="2025-02-02T12:00:00.000Z",
        )
        file = _make_file(file_id="syn123", name="data.csv")

        # When building the full Croissant document for a dataset with one file
        result = build_croissant_metadata(
            dataset=dataset,
            dataset_id="syn999",
            dataset_version=1,
            files_attached_to_dataset=[file],
            file_md5_and_types=DataFrame(),
        )

        # Then the context is set and the distribution starts with the metadata
        # FileObject followed by the per-file entries
        assert result["@context"]["cr"] == "http://mlcommons.org/croissant/"
        assert result["distribution"][0]["@id"] == "metadata"
        assert result["distribution"][1]["@id"] == "syn123.1"

    def test_missing_optional_fields_are_dropped(self) -> None:
        """Optional dataset properties that are absent do not appear in the output at all."""
        dataset = Entity(name="my-dataset", createdOn="2025-02-01T12:00:00.000Z",
                         modifiedOn="2025-02-01T12:00:00.000Z")

        # When the dataset has none of the optional PortalDataset fields set
        result = build_croissant_metadata(
            dataset=dataset,
            dataset_id="syn999",
            dataset_version=1,
            files_attached_to_dataset=[],
            file_md5_and_types=DataFrame(),
        )

        # Then those keys are absent rather than present with a None value
        for key in ("alternateName", "countryOfOrigin", "diseaseFocus", "keywords", "usageInfo"):
            assert key not in result

    def test_missing_license_gets_default(self) -> None:
        """A dataset without a license gets a default 'License not specified' entry."""
        dataset = Entity(name="my-dataset", createdOn="2025-02-01T12:00:00.000Z",
                         modifiedOn="2025-02-01T12:00:00.000Z")

        # When the dataset has no license set
        result = build_croissant_metadata(
            dataset=dataset,
            dataset_id="syn999",
            dataset_version=1,
            files_attached_to_dataset=[],
            file_md5_and_types=DataFrame(),
        )

        # Then a default "License not specified" entry is used
        assert result["license"]["name"] == "License not specified"
