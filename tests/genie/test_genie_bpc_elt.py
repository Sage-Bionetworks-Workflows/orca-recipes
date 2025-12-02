import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, Mock

from genie import genie_bpc_elt


def test_upload_clinical_tables():
    conn = MagicMock()
    mock_syn = MagicMock()
    mock_syn.getChildren.return_value = [{"name": "clinical.csv", "id": "syn1"}]

    with patch.object(
        genie_bpc_elt, "read_synapse_table", return_value=pd.DataFrame({"a": [1]})
    ), patch.object(genie_bpc_elt, "write_to_snowflake") as mock_write:
        genie_bpc_elt._upload_clinical_tables(conn, mock_syn, "syn1", overwrite=True)
        mock_write.assert_called_once()


def test_upload_cbioportal_tables_exclude():
    conn = MagicMock()
    mock_syn = MagicMock()
    mock_syn.getChildren.return_value = [
        {"name": "meta_study.txt", "id": "syn111"},
        {"name": "data_mutations.txt", "id": "syn222"},
    ]

    with patch.object(
        genie_bpc_elt, "read_synapse_table", return_value=pd.DataFrame({"a": [1]})
    ), patch.object(genie_bpc_elt, "write_to_snowflake") as mock_write:
        genie_bpc_elt._upload_cbioportal_tables(
            conn, mock_syn, "cohortA", "syn123", overwrite=True
        )
        mock_write.assert_called_once()  # one excluded, one written
