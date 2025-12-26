

from airflow.decorators import dag, task
from datetime import datetime
from synapseclient.models import query
from synapseclient import Entity, Synapse
from synapseclient.models import Table
from airflow.models.param import Param
from orca.services.synapse import SynapseHook
import pandas as pd
from pandas import DataFrame
from urllib.parse import quote_plus
import json
import os
from io import BytesIO
from typing import Any, Dict
from types import MethodType
from airflow.models import Variable
from logging import NOTSET, Logger, getLogger
from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import \
    OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import \
    OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import (DEPLOYMENT_ENVIRONMENT, SERVICE_NAME,
                                         SERVICE_VERSION, Resource)
from opentelemetry.sdk.trace import Tracer, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from synapseclient.core.retry import with_retry

dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "push_results_to_s3": Param(True, type="boolean"),
    "aws_conn_id": Param("AWS_SYNAPSE_CROISSANT_METADATA_S3_CONN", type="string"),
    "push_links_to_synapse": Param(True, type="boolean"),
}

dag_config = {
    "schedule_interval": "0 0 * * 1",
    "start_date": datetime(2025, 2, 1),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "params": dag_params,
}

SYNAPSE_DATA_CATALOG = "syn61609402"
# AWS related constants
REGION_NAME = "us-east-1"
BUCKET_NAME="synapse-croissant-metadata-minimal"
# Open telemetry related constants
# Used to set `deployment.environment` in the telemetry data.
# Since tracing and logging is getting set up outside of the DAG, we need to set
# the deployment environment here.
MY_SERVICE_NAME = "airflow-synapse-dataset-to-minimal-croissant"
MY_DEPLOYMENT_ENVIRONMENT = "prod"
MY_SERVICE_VERSION = "1.0.0"
SYNAPSE_TABLE_FOR_CROISSANT_LINKS = "syn72041138"

def set_up_tracing() -> Tracer:
    """
    Set up the opentelemetry tracing library to export telemetry data via a BatchSpanProcessor.

    The following environment variables are used to configure the service:
    - SERVICE_NAME: The name of the service.
    - DEPLOYMENT_ENVIRONMENT: The environment in which the service is running.
    - MY_SERVICE_VERSION: The version of the service.


    These attributes are used by the OTLP exporter to tag the telemetry data. They
    should be set to something that uniquely identifies the code that is producing this
    data. Within the telemetry backend these attributes will be used to filter and
    group the data.
    """
    service_name = os.environ.get("SERVICE_NAME", MY_SERVICE_NAME)
    deployment_environment = os.environ.get(
        "DEPLOYMENT_ENVIRONMENT", MY_DEPLOYMENT_ENVIRONMENT)
    service_version = os.environ.get("MY_SERVICE_VERSION", MY_SERVICE_VERSION)

    trace.set_tracer_provider(
        TracerProvider(
            resource=Resource(
                attributes={
                    SERVICE_NAME: service_name,
                    SERVICE_VERSION: service_version,
                    DEPLOYMENT_ENVIRONMENT: deployment_environment,
                }
            )
        )
    )

    exporter = OTLPSpanExporter(endpoint="https://ingest.us.signoz.cloud:443/v1/traces", headers={
                                "signoz-ingestion-key": Variable.get("SIGNOZ_INGESTION_KEY")})
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))
    return trace.get_tracer(__name__)


def set_up_logging() -> Logger:
    """
    Set up the opentelemetry logging library to export telemetry data via a BatchLogRecordProcessor.

    The following static variables are used to configure the service:
    - SERVICE_NAME: The name of the service.
    - DEPLOYMENT_ENVIRONMENT: The environment in which the service is running.
    - MY_SERVICE_VERSION: The version of the service.


    These attributes are used by the OTLP exporter to tag the telemetry data. They
    should be set to something that uniquely identifies the code that is producing this
    data. Within the telemetry backend these attributes will be used to filter and
    group the data.
    """
    service_name = os.environ.get("SERVICE_NAME", MY_SERVICE_NAME)
    deployment_environment = os.environ.get(
        "DEPLOYMENT_ENVIRONMENT", MY_DEPLOYMENT_ENVIRONMENT)
    service_version = os.environ.get("MY_SERVICE_VERSION", MY_SERVICE_VERSION)

    resource = Resource.create(
        {
            SERVICE_NAME: service_name,
            SERVICE_VERSION: service_version,
            DEPLOYMENT_ENVIRONMENT: deployment_environment,
        }
    )

    logger_provider = LoggerProvider(resource=resource)
    set_logger_provider(logger_provider=logger_provider)

    exporter = OTLPLogExporter(endpoint="https://ingest.us.signoz.cloud:443/v1/logs", headers={
                               "signoz-ingestion-key": Variable.get("SIGNOZ_INGESTION_KEY")})
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

    handler = LoggingHandler(level=NOTSET,
                             logger_provider=logger_provider)
    logger = getLogger()
    logger.addHandler(handler)
    return logger

otel_tracer = set_up_tracing()
otel_logger = set_up_logging()


def _rest_call_replacement(
    self,
    method,
    uri,
    data,
    endpoint,
    headers,
    retryPolicy,
    requests_session,
    **kwargs,
):
    """
    See original _rest_call method in the Synapse client for more details.
    """
    self.logger.debug(f"Sending {method} request to {uri}")
    uri, headers = self._build_uri_and_headers(
        uri, endpoint=endpoint, headers=headers
    )

    retryPolicy = self._build_retry_policy(retryPolicy)
    requests_session = requests_session or self._requests_session

    auth = kwargs.pop("auth", self.credentials)
    requests_method_fn = getattr(requests_session, method)
    response = with_retry(
        lambda: requests_method_fn(
            uri,
            data=data,
            headers=headers,
            auth=auth,
            timeout=70,
            **kwargs,
        ),
        verbose=self.debug,
        **retryPolicy,
    )
    self._handle_synapse_http_error(response)
    return response

def execute_push_to_s3(dataset: Entity, dataset_id: str, s3_key: str, croissant_file: Dict[str, Any], push_to_s3: bool, **context) -> None:
    """
    Handle the push to S3 of the croissant file. This is done by using the S3Hook to
    upload the file to S3. The S3 bucket is stored in the `org-sagebase-dpe-prod` AWS
    account.

    Arguments:
        dataset: The dataset to push to S3.
        dataset_id: The ID of the dataset.
        s3_key: The S3 key to use to push the file to S3.
        croissant_file: The croissant file to push to S3.
        push_to_s3: A boolean to indicate if the results should be pushed to S3.
            When set to `False`, the results will be printed to the logs.
        context: The context of the DAG run.

    Returns:

    """
    try:
        if not push_to_s3:
            otel_logger.info(
                f"Croissant file for [dataset: {dataset.name}, id: {dataset_id}]:\n{json.dumps(croissant_file)}")
            return

        otel_logger.info(
            f"Uploading croissant file for [dataset: {dataset.name}, id: {dataset_id}]")

        croissant_metadata_bytes = json.dumps(croissant_file).encode(
            'utf-8')
        metadata_file = BytesIO(croissant_metadata_bytes)
        s3_hook = S3Hook(
            aws_conn_id=context["params"]["aws_conn_id"], region_name=REGION_NAME, extra_args={
                "ContentType": "application/ld+json"
            }
        )

        otel_logger.info(
            f"Uploading croissant file to S3: {s3_key}")
        s3_hook.load_file_obj(file_obj=metadata_file,
                              key=s3_key,
                              bucket_name=BUCKET_NAME,
                              replace=True,
                              )
    except Exception as ex:
        otel_logger.exception(
            "Failed to query snowflake and push croissant file to S3.")
        otel_tracer.span_processor.force_flush()
        otel_logger.handlers[0].flush()
        raise ex
    
def create_syn_client() -> Synapse:
    """
    Create a Synapse client that can be used to query Synapse.

    Returns:
        The Synapse client.
    """
    syn_client: Synapse = Synapse(skip_checks=True)
    syn_client._rest_call = MethodType(_rest_call_replacement, syn_client)
    assert syn_client.credentials is None, "Synapse client is not logged out"
    return syn_client

@dag(**dag_config)
def save_minimal_jsonld_to_s3() -> None:
    """Execute a query on Snowflake and report the results to a Synapse table."""

    def execute_push_to_synapse(push_to_synapse: bool, dataset: Entity, dataset_id: str, s3_url: str, **context) -> None:
        """
        Handle the push to Synapse of the croissant file link. This is done by using
        an authenticated Synapse client to first query the table to determine if an
        update is needed. If the link already exists with the expected S3 URL, then
        skip the update. If the link does not exist or the S3 URL is different, then
        update the link with the new S3 URL using the authenticated Synapse client.

        Arguments:
            push_to_synapse: A boolean to indicate if the results should be pushed to
                Synapse. When set to `False`, the results will be printed to the logs.
            dataset: The dataset to push to Synapse.
            dataset_id: The ID of the dataset.
            s3_url: The S3 URL to use for the value of the cell in the table.
            syn_client: The unauthenticated Synapse client to use to query the table.
            context: The context of the DAG run.

        Returns:
            None
        """
        try:
            if not push_to_synapse:
                otel_logger.info(
                    f"Croissant file link for [dataset: {dataset.name}, id: {dataset_id}]: {s3_url}")
                return

            otel_logger.info(
                f"Uploading croissant file link to Synapse table {SYNAPSE_TABLE_FOR_CROISSANT_LINKS}"
            )

            # Warning: Using an authenticated Synapse Client during this section of code
            syn_hook = SynapseHook(
                context["params"]["synapse_conn_id"])
            authenticated_syn_client: Synapse = syn_hook.client
            authenticated_syn_client._rest_call = MethodType(
                _rest_call_replacement, authenticated_syn_client)
            existing_row_df = query(query=f"SELECT * FROM {SYNAPSE_TABLE_FOR_CROISSANT_LINKS} WHERE dataset = '{dataset_id}'", synapse_client=authenticated_syn_client)

            if not existing_row_df.empty and existing_row_df["minimal_croissant_file_s3_object"].values[0] == s3_url:
                otel_logger.info(
                    f"Croissant file link already exists in Synapse table {SYNAPSE_TABLE_FOR_CROISSANT_LINKS}. Skipping.")
                return

            df = DataFrame(
                data={
                    "dataset": [dataset_id],
                    "minimal_croissant_file_s3_object": [s3_url]
                }
            )
            if existing_row_df.empty:
                # If the row does not exist, create a new row
                print("Creating new row in Synapse table")
                Table(id=SYNAPSE_TABLE_FOR_CROISSANT_LINKS).store_rows(values=df)
                
            else:
                # Update the existing row with the new value
                print("Updating existing row in Synapse table")
                existing_row_df["minimal_croissant_file_s3_object"] = [s3_url]
                Table(id=SYNAPSE_TABLE_FOR_CROISSANT_LINKS).store_rows(values=existing_row_df)

        except Exception as ex:
            otel_logger.exception(
                "Failed to push croissant file link to Synapse.")
            otel_tracer.span_processor.force_flush()
            otel_logger.handlers[0].flush()
            raise ex

    @task
    def create_and_save_jsonld(**context) -> None:
        """
        Create and save the minimal croissant JSON-LD files to S3 and links to a synapse table for all datasets in the home page of data catalog.

        Arguments:
            dataset_collection: The dataset collection to query for datasets.

        Returns:
            The list of dataset IDs for the given dataset collection.
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_client: Synapse = syn_hook.client

        table = query(f"select * from {SYNAPSE_DATA_CATALOG}", synapse_client=syn_client)
        
        push_to_s3 = context["params"]["push_results_to_s3"]
        push_to_synapse = context["params"]["push_links_to_synapse"]

        
        for index, row in table.iterrows():
            if pd.isnull(row["id"]):
                # skip rows without a Synapse ID
                continue

            dataset_id = row["id"]
            dataset_name = row["name"]
            dataset = syn_client.get(dataset_id, downloadFile=False)

            link = f"https://www.synapse.org/#!Synapse:{row['id']}"

            minimal_croissant_file = {
                "@context": "https://schema.org/",
                "@type": "Dataset",
                "name": dataset_name,
                "description": "" if pd.isnull(row["description"]) else row["description"],
                "url": link,
                "identifier": dataset_id,
                "creator": {
                    "@type": "Organization",
                    "name": "Sage Bionetworks" if pd.isnull(row["contributors"]) else row["contributors"]
                },
                "license": "" if pd.isnull(row["license"]) else row["license"]
            }

            s3_key = f"{dataset_name}_{dataset_id}.minimal_croissant.jsonld"
            execute_push_to_s3(dataset=dataset,dataset_id=row["id"],s3_key=s3_key, croissant_file=minimal_croissant_file, push_to_s3=push_to_s3, **context)
            
            s3_url = f"https://{BUCKET_NAME}.s3.us-east-1.amazonaws.com/{quote_plus(s3_key)}"
            execute_push_to_synapse(push_to_synapse=push_to_synapse, dataset=dataset, dataset_id=dataset_id, s3_url=s3_url, **context)


    create_and_save_jsonld()
    

save_minimal_jsonld_to_s3()