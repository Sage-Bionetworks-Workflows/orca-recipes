import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from genie import main_genie_elt


@pytest.mark.parametrize(
    "folder_name, expected",
    [
        ("19.3-consortium", "consortium_19_3"),
        ("20-public", "public_20"),
    ],
)
def test_get_table_schema_name_valid(folder_name, expected):
    mock_syn = MagicMock()
    mock_syn.get.return_value.name = folder_name

    result = main_genie_elt.get_table_schema_name(mock_syn, "syn1")

    assert result == expected


@pytest.mark.parametrize(
    "folder_name",
    [
        "0.1.0",  # Missing expected "-" separator
        "20-3-something",  # Unexpected extra segments or format
    ],
)
def test_get_table_schema_name_invalid(folder_name):
    mock_syn = MagicMock()
    mock_syn.get.return_value.name = folder_name

    with pytest.raises(
        ValueError, match=f"Unexpected folder name format: {folder_name}"
    ):
        main_genie_elt.get_table_schema_name(mock_syn, "syn1")


@pytest.mark.parametrize(
    "release_file_key, expected",
    [
        ("genie_combined.bed", "genomic_information"),
        ("genie_cna_hg19.seg", "cna_hg19"),
        ("data_clinical_patient.txt", "clinical_patient"),
        ("mutations.csv", "mutations.csv"),
    ],
)
def test_get_table_name_standardizes_correctly(release_file_key, expected):
    result = main_genie_elt.get_table_name(release_file_key)
    assert result == expected


@pytest.mark.parametrize(
    "children, expected_keys",
    [
        # Case 1: valid structured file
        (
            [
                {"name": "data_clinical_patient.txt", "id": "syn123"},
                {"name": "random_notes.docx", "id": "syn456"},
            ],
            ["data_clinical_patient.txt"],
        ),
        # Case 2: multiple valid structured files
        (
            [
                {"name": "data_mutations_extended.txt", "id": "syn111"},
                {"name": "genie_combined.bed", "id": "syn222"},
                {"name": "notes.pdf", "id": "syn333"},
            ],
            ["data_mutations_extended.txt", "genie_combined.bed"],
        ),
        # Case 3: no valid files (none match STRUCTURED_DATA or extensions)
        (
            [
                {"name": "random_file.csv", "id": "syn999"},
                {"name": "unrelated.txt", "id": "syn888"},
            ],
            [],
        ),
    ],
    ids=["some_valid_files", "all_valid_files", "no_valid_files"],
)
def test_get_cbio_file_map_returns_expected_structure(children, expected_keys):
    mock_syn = MagicMock()
    mock_syn.getChildren.return_value = children
    mock_syn.get.side_effect = lambda x, followLink=True: {
        "id": x,
        "entity": f"entity_{x}",
    }

    result = main_genie_elt.get_cbio_file_map(mock_syn, "synParent")

    result_keys = list(result.keys())
    assert sorted(result_keys) == sorted(expected_keys)

    # Verify that syn.get() was only called for valid structured files
    expected_ids = [c["id"] for c in children if c["name"] in expected_keys]
    actual_ids = [call.args[0] for call in mock_syn.get.call_args_list]
    assert sorted(actual_ids) == sorted(expected_ids)


def test_push_cbio_files_to_snowflake_success():
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = MagicMock()
    mock_syn = MagicMock()

    with patch.object(
        main_genie_elt, "get_table_schema_name", return_value="schema"
    ), patch.object(
        main_genie_elt,
        "get_cbio_file_map",
        return_value={"data_clinical.txt": MagicMock(path="file.txt")},
    ), patch.object(
        main_genie_elt.pd, "read_csv", return_value=MagicMock()
    ), patch.object(
        main_genie_elt, "write_to_snowflake"
    ) as mock_write:

        main_genie_elt.push_cbio_files_to_snowflake(
            mock_syn, mock_conn, "syn1", overwrite=True, database="test"
        )
        mock_write.assert_called_once()


@pytest.mark.parametrize(
    "read_csv_side_effect, expected_log, expected_calls",
    [
        # Case 1: Normal — CSV loads fine and Snowflake write called
        (None, "Using schema: test_schema", 1),
        # Case 2: EmptyDataError — should log a warning, skip write
        (pd.errors.EmptyDataError(), "is empty — skipping.", 0),
        # Case 3: Unexpected error — should log an error, skip write
        (Exception("File read failed"), "Failed to read", 0),
    ],
)
def test_push_cbio_files_to_snowflake_has_expected_data_error(
    read_csv_side_effect, expected_log, expected_calls, caplog
):
    mock_syn = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock dependent functions
    with patch.object(
        main_genie_elt, "get_table_schema_name", return_value="test_schema"
    ), patch.object(main_genie_elt, "get_cbio_file_map") as mock_get_map, patch.object(
        main_genie_elt, "get_table_name", return_value="clinical_patient"
    ), patch.object(
        main_genie_elt, "write_to_snowflake"
    ) as mock_write, patch.object(
        pd, "read_csv"
    ) as mock_read_csv:

        # Mock the cbio file map to simulate one Synapse file
        mock_entity = MagicMock()
        mock_entity.path = "/tmp/fake_file.txt"
        mock_get_map.return_value = {"data_clinical_patient.txt": mock_entity}

        # Simulate pandas.read_csv behavior per test case
        if read_csv_side_effect is not None:
            mock_read_csv.side_effect = read_csv_side_effect
        else:
            mock_read_csv.return_value = pd.DataFrame({"col1": [1, 2, 3]})

        main_genie_elt.push_cbio_files_to_snowflake(
            syn=mock_syn,
            conn=mock_conn,
            synid="syn123",
            overwrite=True,
            database="FAKE_DB",
        )

    # Assert Snowflake commands are always called
    mock_cursor.execute.assert_any_call("USE DATABASE FAKE_DB;")
    mock_cursor.execute.assert_any_call(
        "CREATE SCHEMA IF NOT EXISTS test_schema WITH MANAGED ACCESS;"
    )
    mock_cursor.execute.assert_any_call("USE SCHEMA test_schema")

    # Assert expected write calls (0 if exception, 1 if normal)
    assert mock_write.call_count == expected_calls

    # Assert expected log text was emitted
    assert any(expected_log in rec.message for rec in caplog.records)


@pytest.mark.parametrize(
    "release_path, expected",
    [
        # valid path (2 levels, not skipped)
        (("Releases/Release 05", "syn214120"), True),
        # invalid path - too many levels
        (("Releases/Release 0/Extra", "syn214120"), False),
        # invalid path - not enough levels
        (("Release 0", "syn214120"), False),
        # invalid path - skipped release
        (("Releases/Release 01", "syn214120"), False),
        # valid path - similar name but not in skip list
        (("Releases/Release 10", "syn214120"), True),
    ],
)
def test_is_valid_release_path_returns_expected(release_path, expected):
    result = main_genie_elt.is_valid_release_path(release_path)
    assert result == expected
