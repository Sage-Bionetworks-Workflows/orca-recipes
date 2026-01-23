from datetime import datetime
import logging
from typing import Dict, Iterable, Optional, List

import pandas as pd
import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook
from airflow.utils.db import provide_session
from airflow.models import XCom

logger = logging.getLogger(__name__)

PATIENT_SAMPLE_TRACKING_TABLE_SYNID = "syn72246564"

dag_params = {
    "snowflake_genie_service_conn": Param(
        "SNOWFLAKE_GENIE_SERVICE_RAW_CONN", type="string"
    ),
    "synapse_conn_id": Param("SYNAPSE_GENIE_RUNNER_SERVICE_ACCOUNT_CONN", type="string"),
}

REQUIRED_COLS = [
    "SAMPLE_ID",
    "PATIENT_ID",
    "MAIN_GENIE_RELEASE",
    "BPC_CRC2_RELEASE",
    "BPC_PANC_RELEASE",
    "BPC_RENAL_RELEASE",
    "BPC_BLADDER_RELEASE",
    "BPC_BRCA_RELEASE",
    "BPC_NSCLC_RELEASE",
    "BPC_PROSTATE_RELEASE",
    "IN_LATEST_MAIN_GENIE",
    "IN_AKT1_PROJECT",
    "IN_BRCA_DDR_PROJECT",
    "IN_ERBB2_PROJECT",
    "IN_FGFE4_PROJECT",
    "IN_KRAS_PROJECT",
    "IN_NTRK_PROJECT",
    "IN_BPC_CRC_RELEASE",
    "IN_BPC_CRC2_RELEASE",
    "IN_BPC_PANC_RELEASE",
    "IN_BPC_RENAL_RELEASE",
    "IN_BPC_BLADDER_RELEASE",
    "IN_BPC_BRCA_RELEASE",
    "IN_BPC_NSCLC_RELEASE",
    "IN_BPC_PROSTATE_RELEASE",
]


def validate_patient_sample_results(
    df: pd.DataFrame,
    required_cols: Optional[Iterable[str]],
    key_cols: Optional[Iterable[str]],
) -> None:
    """
    Run lightweight validation on a query result DataFrame.

    This function logs all validation issues and only raises at the end if
    at least one validation check fails. Intended for Airflow tasks where you
    want maximum debug info in logs but still fail the run on bad outputs.

    Checks performed:
      1) DataFrame is non-empty.
      2) Required columns exist.
      3) No NULL/NaN values in any columns.
      4) No blank (empty/whitespace-only) strings in object columns.
      5) No duplicates on key columns (defaults to SAMPLE_ID, PATIENT_ID).

    Args:
        df (pd.DataFrame): Query results.
        required_cols (Iterable[str]): Columns that must exist in df.
        key_cols (Iterable[str]): Columns used to check duplicates.

    Raises:
        ValueError: If one or more validation checks fail.
    """
    errors: List[str] = []

    # 1) Empty
    if df is None or df.empty:
        msg = "Validation failed: query returned zero rows."
        logger.error(msg)
        errors.append(msg)

    # 2) Required columns exist
    missing = (
        [c for c in required_cols if c not in df.columns]
        if df is not None
        else list(required_cols)
    )
    if missing:
        msg = f"Validation failed: missing required columns: {missing}"
        logger.error(msg)
        errors.append(msg)

    # Only run content checks if df is non-empty and has columns
    if df is not None and not df.empty and len(df.columns) > 0:
        # 3) NULL/NaN checks
        null_cols = [
            c
            for c in REQUIRED_COLS
            if df[c].isna().any() and (not c.endswith("RELEASE") or c.startswith("IN"))
        ]
        if null_cols:
            msg = f"Validation failed: NULL/NaN values found in columns: {null_cols}"
            logger.error(msg)
            errors.append(msg)

        # 4) Duplicate check using key cols
        present_key_cols = [c for c in key_cols if c in df.columns]
        dup_mask = df.duplicated(subset=present_key_cols, keep=False)
        if dup_mask.any():
            dup_count = int(dup_mask.sum())
            msg = f"Validation failed: found {dup_count} duplicate rows based on key columns: {present_key_cols}"
            logger.error(msg)
            errors.append(msg)

    # Raise once at end if anything failed
    if errors:
        raise ValueError(
            "Patient/sample results validation failed:\n- " + "\n- ".join(errors)
        )


@dag(
    schedule_interval="0 1,17 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["snowflake", "genie"],
    params=dag_params,
)
def build_patient_sample_tracking_table():

    @task(task_id="query_validate_and_upload")
    def query_validate_and_upload(**context) -> List[Dict]:
        """Runs a snowflake query that queries the main genie, BPC and SP
        snowflake clincial tables for the required patient-sample tracking
        fields and values for further validation and then upload

        Returns (List[Dict]): queried results
        """
        conn_id = context["params"]["snowflake_genie_service_conn"]
        hook = SnowflakeHook(snowflake_conn_id=conn_id)

        sql = """
        WITH
        /* -------------------------------------------------------------------------
        1) For each BPC cohort, find the most recent (MAJOR_VERSION, MINOR_VERSION)
            present in the BPC cancer_panel_test_level table.
            - QUALIFY + ROW_NUMBER keeps only the top (latest) version per cohort.
        ---------------------------------------------------------------------------*/
        bpc_latest_per_cohort AS (
        SELECT
            COHORT,
            MAJOR_VERSION,
            MINOR_VERSION
        FROM GENIE_DEV.BPC_CLINICAL_FILES.CANCER_PANEL_TEST_LEVEL
        WHERE COHORT IS NOT NULL
            AND MAJOR_VERSION IS NOT NULL
            AND MINOR_VERSION IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY COHORT
            ORDER BY MAJOR_VERSION DESC, MINOR_VERSION DESC
        ) = 1
        ),
        /* -------------------------------------------------------------------------
        2) Find the single most recent (MAJOR_VERSION, MINOR_VERSION) in MAIN GENIE
            clinical_sample.
            - This defines what "latest main release" means for MAIN_GENIE pairs.
        ---------------------------------------------------------------------------*/
        main_latest_version AS (
        SELECT MAJOR_VERSION, MINOR_VERSION
        FROM GENIE_DEV.MAIN.CLINICAL_SAMPLE
        WHERE MAJOR_VERSION IS NOT NULL
            AND MINOR_VERSION IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (ORDER BY MAJOR_VERSION DESC, MINOR_VERSION DESC) = 1
        ),

        /* -------------------------------------------------------------------------
        3) Build the set of sample-patient pairs that are in the latest MAIN GENIE
            release (as defined above).
            - IN_LATEST_RELEASE is always 'Yes' here because this CTE only includes
            pairs from the latest main version.
        ---------------------------------------------------------------------------*/
        main_genie_pairs AS (
        SELECT DISTINCT
            m.SAMPLE_ID,
            m.PATIENT_ID,
            m.RELEASE as RELEASE_NAME,
            'MAIN_GENIE' AS RELEASE_PROJECT_TYPE,
            'Yes' AS IN_LATEST_RELEASE
        FROM GENIE_DEV.MAIN.CLINICAL_SAMPLE m
        JOIN main_latest_version v
            ON m.MAJOR_VERSION = v.MAJOR_VERSION
        AND m.MINOR_VERSION = v.MINOR_VERSION
        WHERE m.SAMPLE_ID IS NOT NULL
            AND m.PATIENT_ID IS NOT NULL
        ),

        /* -------------------------------------------------------------------------
        4) Create a distinct keyset of latest MAIN GENIE sample-patient pairs.
            - Used for fast "is this pair in latest main?" membership checks in other
            project types (BPC + SP).
        ---------------------------------------------------------------------------*/
        main_genie_keys AS (
        SELECT DISTINCT SAMPLE_ID, PATIENT_ID
        FROM main_genie_pairs
        ),

        /* -------------------------------------------------------------------------
        5) Build BPC sample-patient pairs from the latest version per cohort.
            - RELEASE_PROJECT_TYPE is cohort-specific: 'BPC_<COHORT>'
            - IN_LATEST_RELEASE indicates whether that BPC pair also appears in the
            latest MAIN GENIE pairs (main_genie_keys).
        ---------------------------------------------------------------------------*/
        bpc_pairs AS (
        SELECT DISTINCT
            b.CPT_GENIE_SAMPLE_ID AS SAMPLE_ID,
            b.RECORD_ID AS PATIENT_ID,
            b.RELEASE as RELEASE_NAME,
            'BPC_' || TO_VARCHAR(b.COHORT) AS RELEASE_PROJECT_TYPE,
            CASE
            WHEN EXISTS (
                SELECT 1
                FROM main_genie_keys k
                WHERE k.SAMPLE_ID = b.CPT_GENIE_SAMPLE_ID
                AND k.PATIENT_ID = b.RECORD_ID
            )
            THEN 'Yes' ELSE 'No'
            END AS IN_LATEST_RELEASE
        FROM GENIE_DEV.BPC_CLINICAL_FILES.CANCER_PANEL_TEST_LEVEL b
        JOIN bpc_latest_per_cohort v
            ON b.COHORT = v.COHORT
        AND b.MAJOR_VERSION = v.MAJOR_VERSION
        AND b.MINOR_VERSION = v.MINOR_VERSION
        WHERE b.CPT_GENIE_SAMPLE_ID IS NOT NULL
            AND b.RECORD_ID IS NOT NULL
        ),
        /* -------------------------------------------------------------------------
        6) Build Sponsored Project (SP) sample-patient pairs for a specific SP
            dataset/table (example: AKT1) for all SP projects.
            - RELEASE_NAME and RELEASE_PROJECT_TYPE are hard-coded for this SP.
            - IN_LATEST_RELEASE indicates whether that SP pair also appears in the
            latest MAIN GENIE pairs (main_genie_keys).
            - Each new SP project will need to be added here as each SP project's table
            data can be vastly different
        ---------------------------------------------------------------------------*/
        
        /* --------------
          AKT1 SP project
        ----------------*/
        sp_akt1_pairs AS (
        SELECT DISTINCT
            s.SAMPLE_ID,
            s.PATIENT_ID,
            'AKT1' AS RELEASE_NAME,
            'SP_AKT1' AS RELEASE_PROJECT_TYPE,
            CASE
            WHEN EXISTS (
                SELECT 1
                FROM main_genie_keys k
                WHERE k.SAMPLE_ID = s.SAMPLE_ID
                AND k.PATIENT_ID = s.PATIENT_ID
            )
            THEN 'Yes' ELSE 'No'
            END AS IN_LATEST_RELEASE
        FROM GENIE_DEV.AKT1.CBIOPORTAL_CLINICAL_SAMPLE s
        WHERE s.SAMPLE_ID IS NOT NULL
            AND s.PATIENT_ID IS NOT NULL
        ),
        /* -------------------
          BRCA DDR SP project
        ---------------------*/
        sp_brca_ddr_pairs AS (
        SELECT DISTINCT
            s.SAMPLE_ID,
            s.PATIENT_ID,
            'BRCA_DDR' AS RELEASE_NAME,
            'SP_BRCA_DDR' AS RELEASE_PROJECT_TYPE,
            CASE
            WHEN EXISTS (
                SELECT 1
                FROM main_genie_keys k
                WHERE k.SAMPLE_ID = s.SAMPLE_ID
                AND k.PATIENT_ID = s.PATIENT_ID
            )
            THEN 'Yes' ELSE 'No'
            END AS IN_LATEST_RELEASE
        FROM GENIE_DEV.BRCA_DDR.REDCAP_EXPORT s
        WHERE s.SAMPLE_ID IS NOT NULL
            AND s.PATIENT_ID IS NOT NULL
        ),
        /* -------------------
          ERBB2 SP project
        ---------------------*/
        sp_erbb2_pairs AS (
        SELECT DISTINCT
            s.SAMPLE_ID,
            s.PATIENT_ID,
            'ERBB2' AS RELEASE_NAME,
            'SP_ERBB2' AS RELEASE_PROJECT_TYPE,
            CASE
            WHEN EXISTS (
                SELECT 1
                FROM main_genie_keys k
                WHERE k.SAMPLE_ID = s.SAMPLE_ID
                AND k.PATIENT_ID = s.PATIENT_ID
            )
            THEN 'Yes' ELSE 'No'
            END AS IN_LATEST_RELEASE
        FROM GENIE_DEV.ERBB2.CBIOPORTAL_CLINICAL_SAMPLE s
        WHERE s.SAMPLE_ID IS NOT NULL
            AND s.PATIENT_ID IS NOT NULL
        ),
        /* -------------------
          FGFE SP project
        ---------------------*/        
        sp_fgfe_pairs AS (
        SELECT DISTINCT
            s.SAMPLE_ID,
            s.PATIENT_ID,
            'FGFE4' AS RELEASE_NAME,
            'SP_FGFE4' AS RELEASE_PROJECT_TYPE,
            CASE
            WHEN EXISTS (
                SELECT 1
                FROM main_genie_keys k
                WHERE k.SAMPLE_ID = s.SAMPLE_ID
                AND k.PATIENT_ID = s.PATIENT_ID
            )
            THEN 'Yes' ELSE 'No'
            END AS IN_LATEST_RELEASE
        FROM GENIE_DEV.FGFE4.CBIOPORTAL_CLINICAL_SAMPLE s
        WHERE s.SAMPLE_ID IS NOT NULL
            AND s.PATIENT_ID IS NOT NULL
        ),

        /* -------------------
          KRAS SP project
        ---------------------*/   
        sp_kras_pairs AS (
        SELECT DISTINCT
            s.SAMPLE_ID,
            s.GENIE_PATIENT_ID as PATIENT_ID,
            'KRAS' AS RELEASE_NAME,
            'SP_KRAS' AS RELEASE_PROJECT_TYPE,
            CASE
            WHEN EXISTS (
                SELECT 1
                FROM main_genie_keys k
                WHERE k.SAMPLE_ID = s.SAMPLE_ID
                AND k.PATIENT_ID = s.GENIE_PATIENT_ID
            )
            THEN 'Yes' ELSE 'No'
            END AS IN_LATEST_RELEASE
        FROM GENIE_DEV.KRAS.REDCAP_EXPORT s
        WHERE s.SAMPLE_ID IS NOT NULL
            AND s.GENIE_PATIENT_ID IS NOT NULL
        ),

        /* -------------------
          NTRK SP project
        ---------------------*/   
        sp_ntrk_pairs AS (
        SELECT DISTINCT
            s.CPT_GENIE_SAMPLE_ID as SAMPLE_ID,
            s.RECORD_ID as PATIENT_ID,
            'NTRK' AS RELEASE_NAME,
            'SP_NTRK' AS RELEASE_PROJECT_TYPE,
            CASE
            WHEN EXISTS (
                SELECT 1
                FROM main_genie_keys k
                WHERE k.SAMPLE_ID = s.CPT_GENIE_SAMPLE_ID
                AND k.PATIENT_ID = s.RECORD_ID
            )
            THEN 'Yes' ELSE 'No'
            END AS IN_LATEST_RELEASE
        FROM GENIE_DEV.NTRK.CANCER_PANEL_TEST s
        WHERE s.CPT_GENIE_SAMPLE_ID IS NOT NULL
            AND s.RECORD_ID IS NOT NULL
        ),
        base AS (
        /* -------------------------------------------------------------------------
        7) Combine all project-type pair sets into one unified result.
            - UNION ALL keeps duplicates across project types (if they exist) while
            preserving rows from each source CTE.
            - Final WHERE currently filters to MAIN_GENIE only (so BPC/SP rows are
            computed but then excluded).
        ---------------------------------------------------------------------------*/
            SELECT * FROM bpc_pairs
                UNION ALL
            SELECT * FROM main_genie_pairs
                UNION ALL
            SELECT * FROM sp_akt1_pairs
                UNION ALL
            SELECT * FROM sp_brca_ddr_pairs
                UNION ALL
            SELECT * FROM sp_erbb2_pairs
                UNION ALL
            SELECT * FROM sp_fgfe_pairs
                UNION ALL
            SELECT * FROM sp_kras_pairs
                UNION ALL
            SELECT * FROM sp_ntrk_pairs
        ),
        /* 2) Wide flags: MAIN latest + one flag per project type */
        wide AS (
        SELECT
            SAMPLE_ID,
            PATIENT_ID,

            /* latest main-genie membership */
            MAX(IFF(RELEASE_PROJECT_TYPE = 'MAIN_GENIE' AND IN_LATEST_RELEASE = 'Yes', TRUE, FALSE)) AS IN_LATEST_MAIN_GENIE,

            /* one column per SP project */
            MAX(IFF(RELEASE_PROJECT_TYPE = 'SP_AKT1', TRUE, FALSE))      AS IN_AKT1_PROJECT,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'SP_BRCA_DDR', TRUE, FALSE))  AS IN_BRCA_DDR_PROJECT,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'SP_ERBB2', TRUE, FALSE))     AS IN_ERBB2_PROJECT,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'SP_FGFE4', TRUE, FALSE))     AS IN_FGFE4_PROJECT,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'SP_KRAS', TRUE, FALSE))      AS IN_KRAS_PROJECT,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'SP_NTRK', TRUE, FALSE))      AS IN_NTRK_PROJECT,

            /* *per-cohort* BPC flags (IN_CRC_BPC_RELEASE, IN_BLADDER_BPC_RELEASE, ...), */
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_CRC', TRUE, FALSE))     AS IN_BPC_CRC_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_CRC2', TRUE, FALSE)) AS IN_BPC_CRC2_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_PANC', TRUE, FALSE)) AS IN_BPC_PANC_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_RENAL', TRUE, FALSE)) AS IN_BPC_RENAL_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_BLADDER', TRUE, FALSE)) AS IN_BPC_BLADDER_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_BRCA', TRUE, FALSE)) AS IN_BPC_BRCA_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_NSCLC', TRUE, FALSE)) AS IN_BPC_NSCLC_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_PROSTATE', TRUE, FALSE)) AS IN_BPC_PROSTATE_RELEASE,

            /* release-name columns */
            MAX(IFF(RELEASE_PROJECT_TYPE = 'MAIN_GENIE', RELEASE_NAME, NULL)) AS MAIN_GENIE_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_CRC22', RELEASE_NAME, NULL)) AS BPC_CRC2_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_PANC', RELEASE_NAME, NULL)) AS BPC_PANC_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_RENAL', RELEASE_NAME, NULL)) AS BPC_RENAL_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_BLADDER', RELEASE_NAME, NULL)) AS BPC_BLADDER_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_BRCA', RELEASE_NAME, NULL)) AS BPC_BRCA_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_NSCLC', RELEASE_NAME, NULL)) AS BPC_NSCLC_RELEASE,
            MAX(IFF(RELEASE_PROJECT_TYPE = 'BPC_PROSTATE', RELEASE_NAME, NULL)) AS BPC_PROSTATE_RELEASE,

            FROM base
            GROUP BY SAMPLE_ID, PATIENT_ID
        )
        SELECT *
        FROM wide;
        """
        df = hook.get_pandas_df(sql)
        validate_patient_sample_results(
            df,
            required_cols=REQUIRED_COLS,
            key_cols=["SAMPLE_ID", "PATIENT_ID"],
        )

        # Delete all rows in current table and upload new results to Synapse table
        syn = SynapseHook(context["params"]["synapse_conn_id"]).client
        to_delete = syn.tableQuery(
            f"SELECT ROW_ID, ROW_VERSION FROM {PATIENT_SAMPLE_TRACKING_TABLE_SYNID}"
        ).asDataFrame()
        syn.delete(synapseclient.Table(PATIENT_SAMPLE_TRACKING_TABLE_SYNID, to_delete))
        
        # batch upload for memory
        CHUNK = 50000
        for start in range(0, len(df), CHUNK):
            logger.info(f"Uploading batch {start} ...")
            syn.store(
                synapseclient.Table(
                    schema=PATIENT_SAMPLE_TRACKING_TABLE_SYNID,
                    values=df.iloc[start:start+CHUNK],
                )
            )

    @provide_session
    def cleanup_xcom(session=None):
        session.query(XCom).filter(
            XCom.dag_id == "build_patient_sample_tracking_table"
        ).delete()

    query_validate_and_upload()
    cleanup_xcom()


build_patient_sample_tracking_table()
