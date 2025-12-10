from dataclasses import dataclass
from datetime import datetime
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook


FINAL_TABLE = "MY_DB.MY_SCHEMA.ALL_PATIENT_SAMPLES"

# staging table for now
PATIENT_SAMPLE_TRACKING_TABLE_SYNID = "syn71708167"


dag_params = {
    "snowflake_developer_service_conn": Param("SNOWFLAKE_GENIE_SERVICE_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
}

dag_config = {
    "schedule_interval": "0 1,17 * * *",
    "start_date": datetime(2025, 1, 1),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake", "genie"],
    "params": dag_params,
}


@dataclass
class PatientSampleRow:
    """Dataclass to hold the patient sample row of data.

    Attributes:
        sample_id: Sample identifier for the given sample-patient pair.
        
        patient_id: Patient identifier for the given sample-patient pair.
        
        release_project_type: The type of release project type that we are 
            determining presence for, for this sample-patient pair. 
            The project type is [PROJECT]_[COHORT] or SP_[Sponsored project abbrev].
            There can only be Main genie, BPC or Sponsored project project types here.
            
        in_latest_release: Whether the sample-patient pair is present in the latest release 
            for the given release project type.
            
        release_name: The name of the release. This is project type dependent. 
            For main genie, this would be public release versions, for BPC 
                this would be cohort dependent and for sponsored projects (SP),
                this would just be the project name.
            
            E.g: 
            - main genie it has to be NN.N-public where N is a number
            - For BPC it has to be [cohort]_NN.N-consortium or [cohort]-NN.N-public
            - For SP, it has to just be one of the SP name or we can leave it blank 
                since the SP name is in the IN_LATEST_RELEASE column"

    """
    sample_id: str
    patient_id: str
    release_project_type: int
    in_latest_release: int
    release_name: float


@dag(**dag_config)
def build_patient_sample_tracking_table():
    
    @task
    def build_union_table(**context):
        snow_hook = SnowflakeHook(context["params"]["snowflake_developer_service_conn"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()
        query = f"""
        WITH
        /* ------------------------------------------------------------ */
        /* 1) LATEST CONSORTIUM RELEASE (e.g., CONSORTIUM_17_6)          */
        /* ------------------------------------------------------------ */
        consortium_candidates AS (
            SELECT
                schema_name,
                TRY_TO_NUMBER(REGEXP_SUBSTR(schema_name, 'CONSORTIUM_(\\d+)', 1, 1, 'e', 1)) AS major_v,
                TRY_TO_NUMBER(REGEXP_SUBSTR(schema_name, 'CONSORTIUM_\\d+_(\\d+)', 1, 1, 'e', 1)) AS minor_v
            FROM MY_DB.INFORMATION_SCHEMA.SCHEMATA
            WHERE schema_name ILIKE 'CONSORTIUM\\_%' ESCAPE '\\'
        ),
        latest_consortium AS (
            SELECT schema_name
            FROM consortium_candidates
            QUALIFY ROW_NUMBER() OVER (
                ORDER BY major_v DESC NULLS LAST, minor_v DESC NULLS LAST
            ) = 1
        ),
        consortium_ids AS (
            SELECT DISTINCT
                'consortium'       AS release_project_type,
                TRUE               AS in_latest_release,
                lc.schema_name     AS release_name,
                t.PATIENT_ID,
                t.SAMPLE_ID
            FROM latest_consortium lc,
            LATERAL (
                SELECT PATIENT_ID, SAMPLE_ID
                FROM IDENTIFIER('MY_DB.' || lc.schema_name || '.CLINICAL_SAMPLE')
            ) t
        ),

        /* ------------------------------------------------------------ */
        /* 2) LATEST SCHEMA PER COHORT (BRCA, BLADDER, ESOPHAGO, ...)    */
        /*    schemas like BRCA_1_1_CONSORTIUM                          */
        /* ------------------------------------------------------------ */
        cohort_list AS (
            SELECT value::string AS cohort_name
            FROM LATERAL FLATTEN(
                input => ARRAY_CONSTRUCT('BRCA','BLADDER','ESOPHAGO')
            )
        ),
        cohort_candidates AS (
            SELECT
                s.schema_name,
                cl.cohort_name,
                TRY_TO_NUMBER(
                    REGEXP_SUBSTR(s.schema_name, '^[A-Z]+_(\\d+)_', 1, 1, 'e', 1)
                ) AS major_v,
                TRY_TO_NUMBER(
                    REGEXP_SUBSTR(s.schema_name, '^[A-Z]+_\\d+_(\\d+)_CONSORTIUM', 1, 1, 'e', 1)
                ) AS minor_v
            FROM MY_DB.INFORMATION_SCHEMA.SCHEMATA s
            JOIN cohort_list cl
            ON s.schema_name ILIKE cl.cohort_name || '\\_%\\_CONSORTIUM' ESCAPE '\\'
        ),
        latest_cohort_schemas AS (
            SELECT schema_name, cohort_name
            FROM cohort_candidates
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cohort_name
                ORDER BY major_v DESC NULLS LAST, minor_v DESC NULLS LAST
            ) = 1
        ),
        cohort_ids AS (
            SELECT DISTINCT
                'cohort'           AS release_project_type,
                TRUE               AS in_latest_release,
                lcs.schema_name    AS release_name,
                t.RECORD_ID        AS PATIENT_ID,
                t.CPT_GENIE_SAMPLE_ID AS SAMPLE_ID
            FROM latest_cohort_schemas lcs,
            LATERAL (
                SELECT RECORD_ID, CPT_GENIE_SAMPLE_ID
                FROM IDENTIFIER(
                    'MY_DB.' || lcs.schema_name || '.CANCER_PANEL_TEST_LEVEL_DATASET'
                )
            ) t
        ),

        /* ------------------------------------------------------------ */
        /* 3) PROJECT-SPECIFIC SCHEMAS (template mapping)                */
        /* ------------------------------------------------------------ */
        project_list AS (
            SELECT * FROM VALUES
                --project_name, schema_name, table_name, patient_col, sample_col
                ('A', 'A_SCHEMA', 'A_TABLE', 'A_PATIENT_ID_COL', 'A_SAMPLE_ID_COL'),
                ('B', 'B_SCHEMA', 'B_TABLE', 'B_PATIENT_ID_COL', 'B_SAMPLE_ID_COL'),
                ('C', 'C_SCHEMA', 'C_TABLE', 'C_PATIENT_ID_COL', 'C_SAMPLE_ID_COL')
            AS pl(project_name, schema_name, table_name, patient_col, sample_col)
        ),
        project_ids AS (
            SELECT DISTINCT
                'project'          AS release_project_type,
                TRUE               AS in_latest_release,
                pl.schema_name     AS release_name,
                t.patient_id       AS PATIENT_ID,
                t.sample_id        AS SAMPLE_ID
            FROM project_list pl,
            LATERAL (
                SELECT
                    (IDENTIFIER(pl.patient_col))::string AS patient_id,
                    (IDENTIFIER(pl.sample_col))::string  AS sample_id
                FROM IDENTIFIER(
                    'MY_DB.' || pl.schema_name || '.' || pl.table_name
                )
            ) t
        )

        /* ------------------------------------------------------------ */
        /* FINAL CONCAT / UNION                                          */
        /* ------------------------------------------------------------ */
        SELECT * FROM consortium_ids
        UNION ALL
        SELECT * FROM cohort_ids
        UNION ALL
        SELECT * FROM project_ids;
        """

        cs.execute(query)
        top_downloaded_df = cs.fetch_pandas_all()

        patient_sample_rows = []
        for _, row in top_downloaded_df.iterrows():
            patient_sample_rows.append(
                PatientSampleRow(
                    sample_id=row["SAMPLE_ID"],
                    patient_id=row["PATIENT_ID"],
                    release_project_type=row["RELEASE_PROJECT_TYPE"],
                    in_latest_release=row[
                        "IN_LATEST_RELEASE"
                    ],
                    release_name=row["RELEASE_NAME"],
                )
            )
        return patient_sample_rows


    @task
    def push_results_to_synapse_table(patient_sample_rows: List[PatientSampleRow], **context) -> None:
        """Push the results to a Synapse table."""
        data = []
        for patient_sample_row in patient_sample_rows:
            data.append(
                [
                    patient_sample_row.sample_id,
                    patient_sample_row.patient_id,
                    patient_sample_row.release_project_type,
                    patient_sample_row.in_latest_release,
                    patient_sample_row.release_name
                ]
            )

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_hook.client.store(
            synapseclient.Table(schema=PATIENT_SAMPLE_TRACKING_TABLE_SYNID, values=data)
        )
        
    synapse_table =  build_union_table()
    push_results_to_synapse_table(synapse_table)
    
build_patient_sample_tracking_table()