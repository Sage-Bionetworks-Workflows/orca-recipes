import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.genie import genie_bpc_ingestion as ingest


@pytest.mark.parametrize(
    "cohort, version, expected_release, expected_type, major, minor",
    [
        ("bpc", "public_02_2", "2_2_PUBLIC", "PUBLIC", 2, 2),
        ("bpc", "consortium_03_1", "3_1_CONSORTIUM", "CONSORTIUM", 3, 1),
        # case-insensitive
        ("bpc", "PuBlIc_10_0", "10_0_PUBLIC", "PUBLIC", 10, 0),
    ],
    ids=["public", "consortium", "mixed_case"],
)
def test_parse_bpc_version_success(
    cohort, version, expected_release, expected_type, major, minor
):
    info = ingest.parse_bpc_version(cohort=cohort, version=version)
    assert info.cohort == cohort
    assert info.release == expected_release
    assert info.release_type == expected_type
    assert info.major_version == major
    assert info.minor_version == minor


@pytest.mark.parametrize(
    "bad_version",
    [
        "public-02-2",
        "public_2",
        "consortium_02_x",
        "random_02_2",
        "02_2_public",
        "",
    ],
)
def test_parse_bpc_version_raises_on_bad_format(bad_version):
    with pytest.raises(
        ValueError, match=f"Unexpected BPC version format: {bad_version}"
    ):
        ingest.parse_bpc_version(cohort="bpc", version=bad_version)


def test_ensure_schema_executes_expected_sql_and_logs():
    conn = MagicMock(name="conn")
    cs = MagicMock(name="cursor")
    conn.cursor.return_value.__enter__.return_value = cs
    conn.cursor.return_value.__exit__.return_value = None

    with patch.object(ingest, "logger") as mock_logger:
        ingest.ensure_schema(conn, database="GENIE_DEV", schema="SOME_SCHEMA")

    cs.execute.assert_any_call("USE DATABASE GENIE_DEV;")
    cs.execute.assert_any_call(
        "CREATE SCHEMA IF NOT EXISTS SOME_SCHEMA WITH MANAGED ACCESS;"
    )
    cs.execute.assert_any_call("USE SCHEMA SOME_SCHEMA;")
    mock_logger.info.assert_any_call("Using schema: GENIE_DEV.SOME_SCHEMA")
    conn.cursor.assert_called_once()


def test_partition_exists_when_exists_is_true():
    conn = MagicMock(name="conn")
    cs = MagicMock(name="cursor")
    conn.cursor.return_value.__enter__.return_value = cs
    conn.cursor.return_value.__exit__.return_value = None
    cs.fetchone.return_value = (1,)  # any non-None value means exists

    out = ingest.partition_exists(conn, "MY_TABLE", "BRCA", "2026_01_01")

    assert out is True
    cs.fetchone.assert_called_once()


def test_partition_exists_when_exists_is_false():
    conn = MagicMock(name="conn")
    cs = MagicMock(name="cursor")
    conn.cursor.return_value.__enter__.return_value = cs
    conn.cursor.return_value.__exit__.return_value = None
    cs.fetchone.return_value = None

    out = ingest.partition_exists(conn, "MY_TABLE", "BRCA", "2026_01_01")

    assert out is False
    cs.fetchone.assert_called_once()


def test_delete_existing_partition_uses_parameterized_delete():
    conn = MagicMock(name="conn")
    cs = MagicMock(name="cursor")
    conn.cursor.return_value.__enter__.return_value = cs
    conn.cursor.return_value.__exit__.return_value = None

    ingest.delete_existing_partition(
        conn, table="S.TBL", cohort="BPC", release="2_2_PUBLIC"
    )

    cs.execute.assert_called_once_with(
        "DELETE FROM S.TBL WHERE COHORT = %s AND RELEASE = %s",
        ("BPC", "2_2_PUBLIC"),
    )


def test_append_df_raises_runtime_error_if_write_pandas_unsuccessful():
    conn = MagicMock()
    df = pd.DataFrame({"x": [1]})

    with patch.object(ingest, "write_pandas", return_value=(False, 1, 1, None)):
        with pytest.raises(RuntimeError, match="write_pandas failed for table TBL"):
            ingest.append_df(conn, df=df, table="TBL")


def test_append_df_logs_success_rowcount_and_chunks():
    conn = MagicMock()
    df = pd.DataFrame({"x": [1, 2, 3]})

    with patch.object(
        ingest, "write_pandas", return_value=(True, 2, 3, None)
    ), patch.object(ingest, "logger") as mock_logger:
        ingest.append_df(conn, df=df, table="TBL")

    mock_logger.info.assert_any_call("Appended 3 rows to TBL (2 chunks).")


def test_add_bpc_metadata_adds_expected_columns_and_does_not_mutate_original():
    df = pd.DataFrame({"a": [1]})
    info = ingest.ReleaseInfo(
        cohort="bpc",
        release="2_2_PUBLIC",
        release_type="PUBLIC",
        major_version=2,
        minor_version=2,
    )

    out = ingest.add_bpc_metadata(df, release_info=info, source_file="file.csv")

    # original unchanged
    assert "COHORT" not in df.columns

    # metadata present
    assert out.loc[0, "COHORT"] == "BPC"
    assert out.loc[0, "RELEASE"] == "2_2_PUBLIC"
    assert out.loc[0, "RELEASE_TYPE"] == "PUBLIC"
    assert out.loc[0, "MAJOR_VERSION"] == 2
    assert out.loc[0, "MINOR_VERSION"] == 2
    assert out.loc[0, "SOURCE_FILE"] == "file.csv"
    assert "INGESTED_AT" in out.columns


def test_upload_clinical_tables_stacked_skips_non_csv_and_not_in_allowlist():
    conn = MagicMock()
    syn = MagicMock()

    syn.getChildren.return_value = [
        {"name": "not_csv.txt", "id": "synA"},
        {"name": "some_other.csv", "id": "synB"},
        {"name": "cancer_panel_test_level_dataset.csv", "id": "synC"},
    ]

    entC = MagicMock()
    entC.path = "/tmp/c.csv"
    syn.get.return_value = entC

    release_info = MagicMock()
    release_info.cohort = "bpc"
    release_info.release = "2_2_PUBLIC"

    with patch.object(ingest, "ensure_schema") as mock_ensure, patch.object(
        ingest, "add_bpc_metadata", side_effect=lambda df, **kwargs: df
    ) as mock_add_meta, patch.object(
        pd, "read_csv", return_value=pd.DataFrame({"a": [1]})
    ) as mock_read, patch.object(
        ingest, "partition_exists", return_value=False
    ) as mock_partition_exists, patch.object(
        ingest, "append_df"
    ) as mock_append, patch.object(
        ingest, "delete_existing_partition"
    ) as mock_delete:

        ingest.upload_clinical_tables_stacked(
            conn=conn,
            syn=syn,
            database="GENIE_DEV",
            release_info=release_info,
            clinical_synid="synFolder",
            overwrite_partition=False,
        )

    mock_ensure.assert_called_once_with(
        conn, database="GENIE_DEV", schema=ingest.CLINICAL_SCHEMA
    )

    # read_csv called only for allowlisted csv
    mock_read.assert_called_once()
    assert mock_read.call_args.kwargs["sep"] == ","
    assert mock_read.call_args.kwargs["comment"] == "#"
    assert mock_read.call_args.kwargs["low_memory"] is False

    # partition check + append should happen once
    mock_partition_exists.assert_called_once()
    mock_append.assert_called_once()
    mock_delete.assert_not_called()


def test_upload_clinical_tables_stacked_skips_empty_df_and_logs_warning():
    conn = MagicMock()
    syn = MagicMock()
    syn.getChildren.return_value = [
        {"name": "cancer_panel_test_level_dataset.csv", "id": "syn1"}
    ]

    ent = MagicMock()
    ent.path = "/tmp/clinical.csv"
    syn.get.return_value = ent

    release_info = MagicMock()
    release_info.cohort = "bpc"
    release_info.release = "2_2_PUBLIC"

    with patch.object(ingest, "ensure_schema"), patch.object(
        pd, "read_csv", return_value=pd.DataFrame()
    ), patch.object(ingest, "partition_exists") as mock_partition_exists, patch.object(
        ingest, "append_df"
    ) as mock_append, patch.object(
        ingest, "logger"
    ) as mock_logger:

        ingest.upload_clinical_tables_stacked(
            conn=conn,
            syn=syn,
            database="GENIE_DEV",
            release_info=release_info,
            clinical_synid="synFolder",
            overwrite_partition=False,
        )

    mock_partition_exists.assert_not_called()
    mock_append.assert_not_called()

    mock_logger.warning.assert_called_once()
    msg = mock_logger.warning.call_args[0][0]
    assert (
        "[bpc 2_2_PUBLIC] Empty clinical file: cancer_panel_test_level_dataset.csv"
        in msg
    )


def test_upload_clinical_tables_stacked_drops_lowercase_cohort_column_before_metadata():
    conn = MagicMock()
    syn = MagicMock()
    syn.getChildren.return_value = [
        {"name": "cancer_panel_test_level_dataset.csv", "id": "syn1"}
    ]

    ent = MagicMock()
    ent.path = "/tmp/clinical.csv"
    syn.get.return_value = ent

    df_in = pd.DataFrame({"cohort": ["bpc"], "a": [1]})

    release_info = MagicMock()
    release_info.cohort = "bpc"
    release_info.release = "2_2_PUBLIC"

    def _assert_no_lowercase_cohort(df, **kwargs):
        # verify the function dropped "cohort" before metadata is added
        assert "cohort" not in df.columns
        return df

    with patch.object(ingest, "ensure_schema"), patch.object(
        pd, "read_csv", return_value=df_in
    ), patch.object(
        ingest, "add_bpc_metadata", side_effect=_assert_no_lowercase_cohort
    ) as mock_add_meta, patch.object(
        ingest, "partition_exists", return_value=False
    ), patch.object(
        ingest, "append_df"
    ) as mock_append, patch.object(
        ingest, "delete_existing_partition"
    ) as mock_delete:

        ingest.upload_clinical_tables_stacked(
            conn=conn,
            syn=syn,
            database="GENIE_DEV",
            release_info=release_info,
            clinical_synid="synFolder",
            overwrite_partition=False,
        )

    mock_add_meta.assert_called_once()
    mock_delete.assert_not_called()
    mock_append.assert_called_once()


def test_upload_clinical_tables_stacked_overwrite_partition_deletes_then_appends():
    conn = MagicMock()
    syn = MagicMock()
    syn.getChildren.return_value = [
        {"name": "cancer_panel_test_level_dataset.csv", "id": "syn1"}
    ]

    ent = MagicMock()
    ent.path = "/tmp/clinical.csv"
    syn.get.return_value = ent

    release_info = MagicMock()
    release_info.cohort = "bpc"
    release_info.release = "2_2_PUBLIC"

    with patch.object(ingest, "ensure_schema"), patch.object(
        ingest, "add_bpc_metadata", side_effect=lambda df, **kwargs: df
    ), patch.object(
        pd, "read_csv", return_value=pd.DataFrame({"a": [1]})
    ), patch.object(
        ingest, "partition_exists"
    ) as mock_partition_exists, patch.object(
        ingest, "append_df"
    ) as mock_append, patch.object(
        ingest, "delete_existing_partition"
    ) as mock_delete:

        ingest.upload_clinical_tables_stacked(
            conn=conn,
            syn=syn,
            database="GENIE_DEV",
            release_info=release_info,
            clinical_synid="synFolder",
            overwrite_partition=True,
        )

    mock_delete.assert_called_once_with(
        conn,
        table=f"{ingest.CLINICAL_SCHEMA}.{ingest.CLINICAL_FILES['cancer_panel_test_level_dataset.csv']}",
        cohort="bpc",
        release="2_2_PUBLIC",
    )
    mock_partition_exists.assert_not_called()
    mock_append.assert_called_once()


def test_upload_clinical_tables_stacked_skips_when_partition_exists_and_overwrite_false():
    conn = MagicMock()
    syn = MagicMock()
    syn.getChildren.return_value = [
        {"name": "cancer_panel_test_level_dataset.csv", "id": "syn1"}
    ]

    ent = MagicMock()
    ent.path = "/tmp/clinical.csv"
    syn.get.return_value = ent

    release_info = MagicMock()
    release_info.cohort = "bpc"
    release_info.release = "2_2_PUBLIC"

    with patch.object(ingest, "ensure_schema"), patch.object(
        ingest, "add_bpc_metadata", side_effect=lambda df, **kwargs: df
    ), patch.object(
        pd, "read_csv", return_value=pd.DataFrame({"a": [1]})
    ), patch.object(
        ingest, "partition_exists", return_value=True
    ) as mock_partition_exists, patch.object(
        ingest, "append_df"
    ) as mock_append, patch.object(
        ingest, "logger"
    ) as mock_logger:

        ingest.upload_clinical_tables_stacked(
            conn=conn,
            syn=syn,
            database="GENIE_DEV",
            release_info=release_info,
            clinical_synid="synFolder",
            overwrite_partition=False,
        )

    mock_partition_exists.assert_called_once_with(
        conn,
        table=f"{ingest.CLINICAL_SCHEMA}.{ingest.CLINICAL_FILES['cancer_panel_test_level_dataset.csv']}",
        cohort="bpc",
        release="2_2_PUBLIC",
    )
    mock_append.assert_not_called()
    mock_logger.info.assert_any_call(
        "[bpc 2_2_PUBLIC] Partition already exists in "
        f"{ingest.CLINICAL_FILES['cancer_panel_test_level_dataset.csv']}; skipping (overwrite_partition=False)."
    )


def test_upload_cbioportal_tables_stacked_excludes_prefixes_and_only_ingests_allowlist():
    conn = MagicMock()
    syn = MagicMock()

    syn.getChildren.return_value = [
        {"name": "meta_study.txt", "id": "syn_meta"},  # excluded by prefix "meta"
        {"name": "data_gene_panel.txt", "id": "syn_gp"},  # excluded by prefix
        {"name": "data_clinical_sample.txt", "id": "syn_ok"},  # allowlisted
        {
            "name": "data_mutations_extended.txt",
            "id": "syn_nope",
        },  # not in allowlist => skip
    ]

    ent_ok = MagicMock()
    ent_ok.path = "/tmp/data_clinical_sample.txt"
    syn.get.side_effect = lambda synid: (
        ent_ok if synid == "syn_ok" else MagicMock(path="/tmp/other")
    )

    release_info = MagicMock()
    release_info.cohort = "bpc"
    release_info.release = "2_2_PUBLIC"

    with patch.object(ingest, "ensure_schema") as mock_ensure, patch.object(
        ingest, "add_bpc_metadata", side_effect=lambda df, **kwargs: df
    ), patch.object(
        pd, "read_csv", return_value=pd.DataFrame({"a": [1]})
    ) as mock_read, patch.object(
        ingest, "append_df"
    ) as mock_append, patch.object(
        ingest, "delete_existing_partition"
    ) as mock_delete:

        ingest.upload_cbioportal_tables_stacked(
            conn=conn,
            syn=syn,
            database="GENIE_DEV",
            release_info=release_info,
            cbioportal_synid="synFolder",
            overwrite_partition=False,
        )

    mock_ensure.assert_called_once_with(
        conn, database="GENIE_DEV", schema=ingest.CBIOPORTAL_SCHEMA
    )

    # read_csv called only once (for allowlisted file)
    mock_read.assert_called_once()
    assert mock_read.call_args.kwargs["sep"] == "\t"
    assert mock_read.call_args.kwargs["comment"] == "#"
    assert mock_read.call_args.kwargs["low_memory"] is False

    mock_append.assert_called_once()
    mock_delete.assert_not_called()


def test_upload_cbioportal_tables_stacked_skips_empty_df_and_logs_warning():
    conn = MagicMock()
    syn = MagicMock()
    syn.getChildren.return_value = [{"name": "data_clinical_sample.txt", "id": "syn1"}]

    ent = MagicMock()
    ent.path = "/tmp/cbio.txt"
    syn.get.return_value = ent

    release_info = MagicMock()
    release_info.cohort = "bpc"
    release_info.release = "2_2_PUBLIC"

    with patch.object(ingest, "ensure_schema"), patch.object(
        pd, "read_csv", return_value=pd.DataFrame()
    ), patch.object(ingest, "append_df") as mock_append, patch.object(
        ingest, "logger"
    ) as mock_logger:

        ingest.upload_cbioportal_tables_stacked(
            conn=conn,
            syn=syn,
            database="GENIE_DEV",
            release_info=release_info,
            cbioportal_synid="synFolder",
            overwrite_partition=False,
        )

    mock_append.assert_not_called()
    mock_logger.warning.assert_called_once()
    msg = mock_logger.warning.call_args[0][0]
    assert "[bpc 2_2_PUBLIC] Empty cbioportal file: data_clinical_sample.txt" in msg


def test_upload_cbioportal_tables_stacked_overwrite_partition_deletes_then_appends():
    conn = MagicMock()
    syn = MagicMock()
    syn.getChildren.return_value = [{"name": "data_clinical_sample.txt", "id": "syn1"}]

    ent = MagicMock()
    ent.path = "/tmp/cbio.txt"
    syn.get.return_value = ent

    release_info = MagicMock()
    release_info.cohort = "bpc"
    release_info.release = "2_2_PUBLIC"

    with patch.object(ingest, "ensure_schema"), patch.object(
        ingest, "add_bpc_metadata", side_effect=lambda df, **kwargs: df
    ), patch.object(
        pd, "read_csv", return_value=pd.DataFrame({"a": [1]})
    ), patch.object(
        ingest, "append_df"
    ) as mock_append, patch.object(
        ingest, "delete_existing_partition"
    ) as mock_delete:

        ingest.upload_cbioportal_tables_stacked(
            conn=conn,
            syn=syn,
            database="GENIE_DEV",
            release_info=release_info,
            cbioportal_synid="synFolder",
            overwrite_partition=True,
        )

    mock_delete.assert_called_once_with(
        conn,
        table=f"{ingest.CBIOPORTAL_SCHEMA}.{ingest.CBIOPORTAL_FILES['data_clinical_sample.txt']}",
        cohort="bpc",
        release="2_2_PUBLIC",
    )
    mock_append.assert_called_once()


def test_push_bpc_release_to_snowflake_parses_version_and_calls_uploads():
    syn = MagicMock()
    conn = MagicMock()

    with patch.object(
        ingest, "upload_clinical_tables_stacked"
    ) as mock_clinical, patch.object(
        ingest, "upload_cbioportal_tables_stacked"
    ) as mock_cbio, patch.object(
        ingest, "logger"
    ) as mock_logger:

        ingest.push_bpc_release_to_snowflake(
            syn=syn,
            conn=conn,
            database="GENIE_DEV",
            cohort="bpc",
            version="public_02_2",
            clinical_synid="synClin",
            cbioportal_synid="synCbio",
            overwrite_partition=True,
        )

    mock_logger.info.assert_any_call("Processing cohort=bpc, release=2_2_PUBLIC")
    mock_clinical.assert_called_once()
    mock_cbio.assert_called_once()


def test_main_skips_public_preview_and_closes_conn_when_conn_not_injected(
    tmp_path, monkeypatch
):
    """
    - main() reads yaml; we patch open/yaml.safe_load to return our controlled cohorts
    - if conn is NOT injected (conn=None), it should close the connection in finally
    - skip any cohort where version startswith 'public_preview'
    """
    fake_cohorts = [
        {
            "cohort": "bpc",
            "version": "public_preview_02_2",
            "clinical_synid": "synClinA",
            "cbioportal_synid": "synCbioA",
        },
        {
            "cohort": "bpc",
            "version": "public_02_2",
            "clinical_synid": "synClinB",
            "cbioportal_synid": "synCbioB",
        },
    ]

    mock_syn = MagicMock()
    mock_conn_obj = MagicMock()

    # patch login, yaml, connection getter, and the push call
    with patch.object(
        ingest.synapseclient, "login", return_value=mock_syn
    ), patch.object(ingest.yaml, "safe_load", return_value=fake_cohorts), patch(
        "builtins.open", new_callable=MagicMock
    ), patch.object(
        ingest, "get_connection", return_value=mock_conn_obj
    ), patch.object(
        ingest, "push_bpc_release_to_snowflake"
    ) as mock_push:

        ingest.main(database="GENIE_DEV", overwrite_partition=False, conn=None)

    # only non-preview cohort processed
    mock_push.assert_called_once()
    assert mock_push.call_args.kwargs["version"] == "public_02_2"
    # connection closed because conn was not injected
    mock_conn_obj.close.assert_called_once()


def test_main_does_not_close_when_conn_injected():
    """
    If caller passes conn=..., main() should use get_connection(conn=conn) but should NOT close
    because the script only closes when conn is None.
    """
    fake_cohorts = [
        {
            "cohort": "bpc",
            "version": "public_02_2",
            "clinical_synid": "synClin",
            "cbioportal_synid": "synCbio",
        }
    ]

    injected_conn = MagicMock(name="injected_conn")
    returned_conn_obj = MagicMock(name="returned_conn_obj")

    with patch.object(
        ingest.synapseclient, "login", return_value=MagicMock()
    ), patch.object(ingest.yaml, "safe_load", return_value=fake_cohorts), patch(
        "builtins.open", new_callable=MagicMock
    ), patch.object(
        ingest, "get_connection", return_value=returned_conn_obj
    ) as mock_get_conn, patch.object(
        ingest, "push_bpc_release_to_snowflake"
    ) as mock_push:

        ingest.main(database="GENIE_DEV", overwrite_partition=True, conn=injected_conn)

    mock_get_conn.assert_called_once_with(conn=injected_conn)
    mock_push.assert_called_once()
    returned_conn_obj.close.assert_not_called()
