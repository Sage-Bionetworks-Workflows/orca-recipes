from dataclasses import dataclass
from datetime import datetime
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook


FINAL_TABLE = "MY_DB.MY_SCHEMA.ALL_PATIENT_SAMPLES"
BPC_YAML_PATH = "/genie/genie_bpc_releases.yaml" 
SP_YAML_PATH = "/genie/genie_sp_releases.yaml" 


# staging table for now
PATIENT_SAMPLE_TRACKING_TABLE_SYNID = "syn71708167"


dag_params = {
    "snowflake_genie_service_conn": Param("SNOWFLAKE_GENIE_SERVICE_RAW_CONN", type="string"),
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


    @task(task_id="get_latest_consortium_schema")
    def get_latest_consortium_schema(**context) -> str:
        GET_LATEST_SCHEMA_SQL = r"""
        WITH consortium_candidates AS (
            SELECT
                schema_name,
                TRY_TO_NUMBER(REGEXP_SUBSTR(schema_name, 'CONSORTIUM_(\\d+)', 1, 1, 'e', 1)) AS major_v,
                TRY_TO_NUMBER(REGEXP_SUBSTR(schema_name, 'CONSORTIUM_\\d+_(\\d+)', 1, 1, 'e', 1)) AS minor_v
            FROM GENIE.INFORMATION_SCHEMA.SCHEMATA
            WHERE schema_name ILIKE 'CONSORTIUM\\_%' ESCAPE '\\'
        )
        SELECT schema_name
        FROM consortium_candidates
        QUALIFY ROW_NUMBER() OVER (
            ORDER BY major_v DESC NULLS LAST, minor_v DESC NULLS LAST
        ) = 1
        """
        hook = SnowflakeHook(snowflake_conn_id=context["params"]["snowflake_genie_service_conn"])
        row = hook.get_first(GET_LATEST_SCHEMA_SQL)  # returns a tuple like ('CONSORTIUM_12_3',)
        if not row or not row[0]:
            raise ValueError("No CONSORTIUM_* schema found/visible in GENIE.INFORMATION_SCHEMA.SCHEMATA")
        return row[0]

    @task(task_id="query_latest_clinical_sample")
    def query_latest_clinical_sample(latest_schema: str, **context):
        conn_id = context["params"]["snowflake_genie_service_conn"]
        hook = SnowflakeHook(snowflake_conn_id=conn_id)

        sql = f"""
        SELECT DISTINCT
            PATIENT_ID,
            SAMPLE_ID,
            'MAIN_GENIE' AS release_project_type,
            TRUE         AS in_latest_release,
            '{latest_schema}' AS release_name
        FROM GENIE.{latest_schema}.CLINICAL_SAMPLE
        """
        
        hook.get_pandas_df(sql)

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
        
    latest = get_latest_consortium_schema()
    query_latest_clinical_sample(latest)
    #push_results_to_synapse_table(synapse_table)
    
build_patient_sample_tracking_table()