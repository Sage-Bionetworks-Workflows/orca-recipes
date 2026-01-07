import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# Adjust import path/module name to match your repo
# e.g. from src.genie import main_genie_ingest
from src.genie import main_genie_ingestion as ingest


# ----------------------------
# parse_release_folder
# ----------------------------
@pytest.mark.parametrize(
    "folder_name, expected_release, expected_type, major, minor",
    [
        ("19.3-consortium", "19_3_CONSORTIUM", "CONSORTIUM", 19, 3),
        ("20.0-public", "20_0_PUBLIC", "PUBLIC", 20, 0),
        ("01.2-public", "1_2_PUBLIC", "PUBLIC", 1, 2),
        ("19.3-CoNsOrTiUm", "19_3_CONSORTIUM", "CONSORTIUM", 19, 3),
    ],
)
def test_parse_release_folder_valid(folder_name, expected_release, expected_type, major, minor):
    info = ingest.parse_release_folder(folder_name)
    assert info.release == expected_release
    assert info.release_type == expected_type
    assert info.major_version == major
    assert info.minor_version == minor


@pytest.mark.parametrize(
    "folder_name",
    [
        "19.3consortium",      # missing "-"
        "19.3-consortium-x",   # too many segments
        "v19.3-consortium",    # version part not numeric
        "19-consortium",       # version missing minor
        "19.3-",               # missing release type
        "-consortium",         # missing version
        "",                    # empty
    ],
)
def test_parse_release_folder_invalid(folder_name):
    with pytest.raises(ValueError):
        ingest.parse_release_folder(folder_name)


# ----------------------------
# get_release_file_map
# ----------------------------
def test_get_release_file_map_filters_to_configured_fileformats_only():
    mock_syn = MagicMock()
    mock_syn.getChildren.return_value = [
        {"name": "data_clinical_sample.txt", "id": "syn_ok"},
        {"name": "meta_study.txt", "id": "syn_skip"},
        {"name": "random.docx", "id": "syn_skip2"},
    ]

    ent_ok = MagicMock(name="ent_ok")
    mock_syn.get.return_value = ent_ok

    result = ingest.get_release_file_map(mock_syn, "synReleaseFolder")

    assert list(result.keys()) == ["data_clinical_sample.txt"]
    assert result["data_clinical_sample.txt"] is ent_ok

    # syn.get should only be called for wanted filename(s)
    mock_syn.get.assert_called_once_with("syn_ok", followLink=True)


def test_get_release_file_map_returns_empty_when_no_wanted_files():
    mock_syn = MagicMock()
    mock_syn.getChildren.return_value = [
        {"name": "meta_study.txt", "id": "syn1"},
        {"name": "data_mutations.txt", "id": "syn2"},
    ]

    result = ingest.get_release_file_map(mock_syn, "synReleaseFolder")
    assert result == {}
    mock_syn.get.assert_not_called()


# ----------------------------
# ensure_schema / delete_existing_partition / append_df
# ----------------------------
def test_ensure_schema_executes_expected_sql_and_logs():
    conn = MagicMock(name="conn")
    cs = MagicMock(name="cursor")
    conn.cursor.return_value.__enter__.return_value = cs
    conn.cursor.return_value.__exit__.return_value = None

    with patch.object(ingest, "logger") as mock_logger:
        ingest.ensure_schema(conn, database="GENIE_DEV", schema="MAIN")

    cs.execute.assert_any_call("USE DATABASE GENIE_DEV;")
    cs.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS MAIN WITH MANAGED ACCESS;")
    cs.execute.assert_any_call("USE SCHEMA MAIN;")
    mock_logger.info.assert_any_call("Using schema: MAIN")
    conn.cursor.assert_called_once()


def test_delete_existing_partition_uses_parameterized_delete():
    conn = MagicMock(name="conn")
    cs = MagicMock(name="cursor")
    conn.cursor.return_value.__enter__.return_value = cs
    conn.cursor.return_value.__exit__.return_value = None

    ingest.delete_existing_partition(conn, table="CLINICAL_SAMPLE", release="19_3_PUBLIC")

    cs.execute.assert_called_once_with(
        "DELETE FROM CLINICAL_SAMPLE WHERE RELEASE = %s",
        ("19_3_PUBLIC",),
    )


def test_append_df_raises_if_write_pandas_unsuccessful():
    conn = MagicMock()
    df = pd.DataFrame({"x": [1]})

    with patch.object(ingest, "write_pandas", return_value=(False, 1, 1, None)):
        with pytest.raises(RuntimeError, match="write_pandas failed for table TBL"):
            ingest.append_df(conn, df=df, table="TBL")


def test_append_df_logs_success_rowcount_and_chunks():
    conn = MagicMock()
    df = pd.DataFrame({"x": [1, 2, 3]})

    with patch.object(ingest, "write_pandas", return_value=(True, 2, 3, None)), \
         patch.object(ingest, "logger") as mock_logger:
        ingest.append_df(conn, df=df, table="TBL")

    mock_logger.info.assert_any_call("Appended 3 rows to 'TBL' (2 chunks).")


# ----------------------------
# is_valid_release_path
# ----------------------------
@pytest.mark.parametrize(
    "release_path, expected",
    [
        (("Releases/Release 17", "synX"), True),
        (("Releases/Release 16", "synX"), False),   # skipped list
        (("Releases/Release 00", "synX"), False),   # skipped list
        (("Release 17", "synX"), False),            # not enough levels
        (("Releases/Release 17/Extra", "synX"), False),  # too many levels
    ],
)
def test_is_valid_release_path_returns_expected(release_path, expected):
    assert ingest.is_valid_release_path(release_path) == expected


# ----------------------------
# push_release_to_snowflake
# ----------------------------
def test_push_release_to_snowflake_skips_when_no_configured_fileformats_found():
    syn = MagicMock()
    conn = MagicMock()

    with patch.object(ingest, "get_release_file_map", return_value={}), \
         patch.object(ingest, "ensure_schema") as mock_ensure, \
         patch.object(ingest, "logger") as mock_logger:

        ingest.push_release_to_snowflake(
            syn=syn,
            conn=conn,
            release_folder_synid="synFolder",
            release_folder_name="19.3-consortium",
            database="GENIE_DEV",
            overwrite_partition=False,
        )

    mock_ensure.assert_not_called()
    mock_logger.info.assert_any_call(
        "[19.3-consortium] No configured fileformats found; skipping."
    )


def test_push_release_to_snowflake_success_appends_and_adds_metadata():
    syn = MagicMock()
    conn = MagicMock()

    ent = MagicMock()
    ent.path = "file.txt"

    file_map = {"data_clinical_sample.txt": ent}

    with patch.object(ingest, "get_release_file_map", return_value=file_map), \
         patch.object(ingest, "ensure_schema") as mock_ensure, \
         patch.object(ingest.pd, "read_csv", return_value=pd.DataFrame({"a": [1]})) as mock_read, \
         patch.object(ingest, "append_df") as mock_append, \
         patch.object(ingest, "delete_existing_partition") as mock_delete, \
         patch.object(ingest, "logger") as mock_logger:

        ingest.push_release_to_snowflake(
            syn=syn,
            conn=conn,
            release_folder_synid="synFolder",
            release_folder_name="19.3-consortium",
            database="GENIE_DEV",
            overwrite_partition=False,
        )

    mock_ensure.assert_called_once_with(conn, database="GENIE_DEV", schema="MAIN")
    mock_read.assert_called_once()
    assert mock_read.call_args.kwargs["sep"] == "\t"

    mock_delete.assert_not_called()
    mock_append.assert_called_once()

    appended_df = mock_append.call_args.kwargs["df"]
    assert appended_df.loc[0, "RELEASE"] == "19_3_CONSORTIUM"
    assert appended_df.loc[0, "RELEASE_TYPE"] == "CONSORTIUM"
    assert appended_df.loc[0, "MAJOR_VERSION"] == 19
    assert appended_df.loc[0, "MINOR_VERSION"] == 3
    assert "INGESTED_AT" in appended_df.columns

    mock_logger.info.assert_any_call(
        "[19.3-consortium] Loaded data_clinical_sample.txt into MAIN.CLINICAL_SAMPLE (RELEASE=19_3_CONSORTIUM)."
    )


def test_push_release_to_snowflake_skips_empty_df_and_does_not_append():
    syn = MagicMock()
    conn = MagicMock()

    ent = MagicMock()
    ent.path = "file.txt"
    file_map = {"data_clinical_sample.txt": ent}

    with patch.object(ingest, "get_release_file_map", return_value=file_map), \
         patch.object(ingest, "ensure_schema"), \
         patch.object(ingest.pd, "read_csv", return_value=pd.DataFrame()), \
         patch.object(ingest, "append_df") as mock_append, \
         patch.object(ingest, "logger") as mock_logger:

        ingest.push_release_to_snowflake(
            syn=syn,
            conn=conn,
            release_folder_synid="synFolder",
            release_folder_name="19.3-consortium",
            database="GENIE_DEV",
            overwrite_partition=False,
        )

    mock_append.assert_not_called()
    mock_logger.warning.assert_called_once()
    assert "Empty file data_clinical_sample.txt; skipping." in mock_logger.warning.call_args[0][0]


def test_push_release_to_snowflake_overwrite_partition_deletes_then_appends():
    syn = MagicMock()
    conn = MagicMock()

    ent = MagicMock()
    ent.path = "file.txt"
    file_map = {"data_clinical_sample.txt": ent}

    with patch.object(ingest, "get_release_file_map", return_value=file_map), \
         patch.object(ingest, "ensure_schema"), \
         patch.object(ingest.pd, "read_csv", return_value=pd.DataFrame({"a": [1]})), \
         patch.object(ingest, "append_df") as mock_append, \
         patch.object(ingest, "delete_existing_partition") as mock_delete:

        ingest.push_release_to_snowflake(
            syn=syn,
            conn=conn,
            release_folder_synid="synFolder",
            release_folder_name="20.0-public",
            database="GENIE_DEV",
            overwrite_partition=True,
        )

    mock_delete.assert_called_once_with(conn, table="CLINICAL_SAMPLE", release="20_0_PUBLIC")
    mock_append.assert_called_once()


# ----------------------------
# main
# ----------------------------
def test_main_walks_releases_skips_invalid_paths_and_closes_conn_when_not_injected():
    mock_syn = MagicMock()
    mock_conn_obj = MagicMock()

    # synu.walk yields: (dirpath, dirnames, filenames)
    # Here dirpath is a tuple: (path_str, synId)
    walk_output = [
        (("Releases/Release 16", "synOld"), [("19.3-consortium", "synR1")], []),  # skipped
        (("Releases/Release 17", "synNew"), [("19.3-consortium", "synR2")], []),  # processed
        (("Releases/Release 17/Extra", "synBad"), [("20.0-public", "synR3")], []),  # invalid
    ]

    with patch.object(ingest.synapseclient, "login", return_value=mock_syn), \
         patch.object(ingest, "get_connection", return_value=mock_conn_obj), \
         patch.object(ingest.synu, "walk", return_value=walk_output), \
         patch.object(ingest, "push_release_to_snowflake") as mock_push:

        ingest.main(database="GENIE_DEV", overwrite_partition=False, conn=None)

    # Only the valid dirpath should trigger push for each dirnames entry
    mock_push.assert_called_once_with(
        syn=mock_syn,
        conn=mock_conn_obj,
        release_folder_synid="synR2",
        release_folder_name="19.3-consortium",
        database="GENIE_DEV",
        overwrite_partition=False,
    )

    mock_conn_obj.close.assert_called_once()


def test_main_does_not_close_conn_when_injected():
    mock_syn = MagicMock()
    injected_conn = MagicMock(name="injected_conn")
    conn_obj = MagicMock(name="conn_obj")

    walk_output = [
        (("Releases/Release 17", "synNew"), [("19.3-consortium", "synR2")], []),
    ]

    with patch.object(ingest.synapseclient, "login", return_value=mock_syn), \
         patch.object(ingest, "get_connection", return_value=conn_obj) as mock_get_conn, \
         patch.object(ingest.synu, "walk", return_value=walk_output), \
         patch.object(ingest, "push_release_to_snowflake") as mock_push:

        ingest.main(database="GENIE_DEV", overwrite_partition=True, conn=injected_conn)

    mock_get_conn.assert_called_once_with(conn=injected_conn)
    mock_push.assert_called_once()
    conn_obj.close.assert_not_called()
