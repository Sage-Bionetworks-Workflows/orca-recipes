from datetime import datetime
from typing import List, Dict

import pandas as pd
import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook
from airflow.utils.db import provide_session
from airflow.models import XCom

PATIENT_SAMPLE_TRACKING_TABLE_SYNID = "syn71708167"

dag_params = {
    "snowflake_genie_service_conn": Param(
        "SNOWFLAKE_GENIE_SERVICE_RAW_CONN", type="string"
    ),
    "synapse_conn_id": Param("SYNAPSE_GENIE_SERVICE_ACCOUNT_CONN", type="string"),
}


@dag(
    schedule_interval="0 1,17 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["snowflake", "genie"],
    params=dag_params,
)
def build_patient_sample_tracking_table():

    @task(task_id="query_patient_sample_tracking")
    def query_patient_sample_tracking(**context) -> List[Dict]:
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
            dataset/table (example: AKT1).
            - RELEASE_NAME and RELEASE_PROJECT_TYPE are hard-coded for this SP.
            - IN_LATEST_RELEASE indicates whether that SP pair also appears in the
            latest MAIN GENIE pairs (main_genie_keys).
        ---------------------------------------------------------------------------*/
        sp_pairs AS (
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
        )

        /* -------------------------------------------------------------------------
        7) Combine all project-type pair sets into one unified result.
            - UNION ALL keeps duplicates across project types (if they exist) while
            preserving rows from each source CTE.
            - Final WHERE currently filters to MAIN_GENIE only (so BPC/SP rows are
            computed but then excluded).
        ---------------------------------------------------------------------------*/
        SELECT *
        FROM (
        SELECT * FROM bpc_pairs
        UNION ALL
        SELECT * FROM main_genie_pairs
        UNION ALL
        SELECT * FROM sp_pairs
        )
        """
        df = hook.get_pandas_df(sql)

        #return df.to_dict(orient="records")

    @task(task_id="validate_patient_sample_tracking")
    def validate_patient_sample_tracking_query(rows: List[Dict]) -> List[Dict]:
        """Do some light validation of the patient-sample tracking query

        Args:
            rows (list[dict]): 

        Raises:
            ValueError: _description_
            ValueError: _description_
            ValueError: _description_

        Returns:
            list[dict]: _description_
        """
        df = pd.DataFrame(rows)

        # check for required columns
        required_cols = [
            "SAMPLE_ID",
            "PATIENT_ID",
            "RELEASE_NAME",
            "RELEASE_PROJECT_TYPE",
            "IN_LATEST_RELEASE",
        ]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # check for any missingness
        if df.isna().any().any():
            bad_cols = df.columns[df.isna().any()].tolist()
            raise ValueError(f"Found missing values in columns: {bad_cols}")

        # enforce uniqueness of (SAMPLE_ID, PATIENT_ID, RELEASE_PROJECT_TYPE)
        key_cols = ["SAMPLE_ID", "PATIENT_ID", "RELEASE_PROJECT_TYPE"]
        dupes = df[df.duplicated(key_cols, keep=False)]
        if not dupes.empty:
            raise ValueError(f"Found duplicate keys ({key_cols}): {len(dupes)} rows")

        return rows  # pass through unchanged if valid

    @task(task_id="push_results_to_synapse_table")
    def push_results_to_synapse_table(rows: List[Dict], **context) -> None:
        """Pushes the queried patient-sample tracking rows (once validated)
        to the corresponding Synapse table for external use and analyses

        Args:
            rows (List[Dict]): the input queried rows that are now
                validated
        """
        df = pd.DataFrame(rows)

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn = syn_hook.client

        # Store dataframe into the existing Synapse table (schema already exists)
        syn.store(synapseclient.Table(PATIENT_SAMPLE_TRACKING_TABLE_SYNID, df))
        

    @provide_session
    def cleanup_xcom(session=None):
        session.query(XCom).filter(XCom.dag_id == "build_patient_sample_tracking_table").delete()


    #rows = query_patient_sample_tracking()
    #validated_rows = validate_patient_sample_tracking_query(rows)
    #push_results_to_synapse_table(rows)
    cleanup_xcom()


build_patient_sample_tracking_table()
