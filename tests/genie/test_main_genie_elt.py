import pytest
from unittest.mock import patch, MagicMock

from genie import main_genie_elt


def test_get_table_schema_name_valid():
    mock_syn = MagicMock()
    mock_syn.get.return_value.name = "19.3-consortium"
    result = main_genie_elt.get_table_schema_name(mock_syn, "syn1")
    assert result == "consortium_19_3"


def test_push_cbio_files_to_snowflake():
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = MagicMock()
    mock_syn = MagicMock()

    with patch.object(main_genie_elt, "get_table_schema_name", return_value="schema"), \
         patch.object(main_genie_elt, "get_cbio_file_map", return_value={"data_clinical.txt": MagicMock(path="file.txt")}), \
         patch.object(main_genie_elt.pd, "read_csv", return_value=MagicMock()), \
         patch.object(main_genie_elt, "write_to_snowflake") as mock_write:

        main_genie_elt.push_cbio_files_to_snowflake(mock_syn, mock_conn, "syn1", overwrite=True, database = "test")
        mock_write.assert_called_once()
