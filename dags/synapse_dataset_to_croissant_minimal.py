
"""
This DAG interacts with a few different services to accomplish the following:

- (Synapse, Authenticated) Query the Synapse Data Catalog to retrieve all datasets listed on the data catalog homepage
- (Synapse, Authenticated) For each dataset in the data catalog, retrieve metadata including name, description, contributors, and license
- (Synapse, Authenticated) Generate minimal Schema.org JSON-LD metadata files for each dataset following the minimal Croissant format
- (S3, Authenticated) For each dataset, upload the minimal JSON-LD file to the `synapse-croissant-metadata-minimal` public S3 bucket in the `org-sagebase-dpe-prod` AWS account
- (Synapse, Authenticated) For each dataset, query the Synapse table to check if a link to the S3 object already exists
- (Synapse, Authenticated) Store or update the S3 object URL in the Synapse table for each dataset


This DAG addresses the issue where Google has difficulty indexing Croissant JSON embedded in portal pages. 

The workflow for making datasets discoverable to Google:
1. This DAG generates minimal JSON-LD files and uploads them to a publicly accessible S3 bucket
2. This DAG stores the S3 URLs in a Synapse table
3. When a Synapse dataset webpage is opened in the portal, the Synapse Web Client queries this table
4. If a croissant file link exists for that dataset, the Synapse Web Client injects it into the HTML of the page
5. Google crawler reads the JSON-LD from the HTML and indexes the dataset for Google Datasets search

See synapse-dataset-to-croissant.py for additional note on the pushing to S3. 

DAG Parameters:
- Review the DAG Parameters under the `@dag` decorated function
"""
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
from typing import Any, Dict, List
from types import MethodType
from airflow.models import Variable
from logging import NOTSET, Logger, getLogger
from opentelemetry import trace
from opentelemetry import context as otel_context
from opentelemetry._logs import set_logger_provider
from opentelemetry.trace.propagation.tracecontext import \
    TraceContextTextMapPropagator
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
    "delete_out_of_date_from_s3": Param(True, type="boolean"),
    "delete_out_of_date_from_synapse": Param(True, type="boolean"),
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

# TODO: Remove this on the next > 4.7.0 release of the Synapse Python Client
# This is a temporary hack to include the changes from: https://github.com/Sage-Bionetworks/synapsePythonClient/pull/1188
# The hack is used here because the current SYNPY client does not have an HTTP timeout
# for requests to Synapse. As a result and due to the significant number of HTTP calls that
# occur during the DAG, the DAG can stall and never return due to the requests library.
# https://requests.readthedocs.io/en/latest/user/advanced/#timeouts

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
        
    def create_root_span(**context) -> Dict:
        """
        Create a root span that all other spans will be children of. This also will
        create a context that can be used to propagate the trace context. This is
        detailed in this document:
        <https://opentelemetry.io/docs/languages/python/propagation/#manual-context-propagation>

        The reason by context is being propogated is that due to the distributed nature
        of Airflow, the context is not automatically propagated to the tasks. This is
        a workaround to ensure that the trace context is propagated to the tasks and
        all child spans are correctly linked to the root span.

        One of the issues with this is that the root span does not seem to get sent
        to the telemetry backend. This is likely due to the fact that the root span
        is not ended before the process is finished. An upgrade to the latest version
        of Apache airflow may resolve some issues with this:
        <https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#opentelemetry-traces-for-apache-airflow-37948>

        Returns:
            The trace context that can be used to propagate the trace context.
        """
        root_span = otel_tracer.start_span(
            name="synapse_dataset_to_croissant", context=otel_context.get_current()
        )
        parent_context = trace.set_span_in_context(root_span)
        otel_context.attach(parent_context)
        carrier = {}
        TraceContextTextMapPropagator().inject(carrier)
        return carrier
    
    def extract_s3_objects_to_delete(bucket_objects: List[str], combined_dataset_name_and_id: List[Dict[str, str]]) -> List[str]:
        """
        Extract the S3 objects to delete from the bucket. This is done by comparing
        the list of objects in the bucket to the list of datasets (dataset_id and dataset_name).
        If an object is found in the bucket that does not correspond to a current dataset,
        it is marked for deletion.

        Arguments:
            bucket_objects: The list of objects in the S3 bucket.
            combined_dataset_name_and_id: The list of dictionaries containing
                dataset_id and dataset_name for all current datasets.
        """
        objects_to_delete = []

        if not bucket_objects:
            otel_logger.info("No objects found in S3.")
            return objects_to_delete

        for obj_key in bucket_objects:
            # Extract dataset name and ID from the S3 object key
            # Expected format: {dataset_name}_{dataset_id}.minimal_croissant.jsonld
            if obj_key.endswith(".minimal_croissant.jsonld"):
                # Remove the extension
                obj_without_ext = obj_key.replace(".minimal_croissant.jsonld", "")
                
                # Split on the last underscore to get name and ID
                parts = obj_without_ext.rsplit("_", 1)
                if len(parts) < 2:
                    otel_logger.info(f"Object {obj_key} does not match expected pattern. Skipping.")
                    continue
                
                dataset_name = parts[0]
                dataset_id = parts[1]

                # Check if this dataset_name and dataset_id combo exists in current datasets
                match_found = False
                for current_dataset in combined_dataset_name_and_id:
                    if (current_dataset["dataset_id"] == dataset_id and 
                        current_dataset["dataset_name"] == dataset_name):
                        match_found = True
                        break
                
                if not match_found:
                    objects_to_delete.append(obj_key)

        return objects_to_delete

    
    @task
    def delete_non_current_croissant_file_in_s3(
        root_carrier_context: Dict, combined_dataset_name_and_id: List[Dict[str, str]], **context
    ) -> None:
        """
        Delete the non-current croissant files from S3. 
        This is used to remove the old files from S3 that are no longer needed. 
        A "non-current" file is defined as a croissant JSON LD file which is no longer
        present in the data catalog.

        This can occur if the dataset has been removed from the data catalog, or
        if there is a new version of the dataset that has been added to the data catalog.

        Arguments:
            root_carrier_context: The root carrier context to use for the trace context.
            combined_dataset_name_and_id: a list of dictionaries containing dataset_id and dataset_name for all current datasets

        Returns:
            None
        """
        with otel_tracer.start_as_current_span("delete_non_current_files_from_s3", context=TraceContextTextMapPropagator().extract(root_carrier_context)) as span:
            s3_hook = S3Hook(
                aws_conn_id=context["params"]["aws_conn_id"], region_name=REGION_NAME)
            bucket_objects = s3_hook.list_keys(bucket_name=BUCKET_NAME)

            objects_to_delete = extract_s3_objects_to_delete(
                bucket_objects=bucket_objects,
                combined_dataset_name_and_id=combined_dataset_name_and_id,
            )

            if objects_to_delete:
                delete_out_of_date_from_s3 = context["params"]["delete_out_of_date_from_s3"]
                if delete_out_of_date_from_s3:
                    otel_logger.info(
                        f"Deleting the following objects from S3: {objects_to_delete}")
                    s3_hook.delete_objects(
                        bucket=BUCKET_NAME, keys=objects_to_delete)
                else:
                    otel_logger.info(
                        f"Found objects to delete from S3, but not deleting due to `delete_out_of_date_from_s3` param: {objects_to_delete}")
            else:
                otel_logger.info(
                    "No objects to delete from S3. All objects are current.")
            otel_tracer.span_processor.force_flush()
            otel_logger.handlers[0].flush()
            return None
        
    def extract_synapse_rows_to_delete(synapse_rows: DataFrame, combined_dataset_name_and_id: List[Dict[str, str]]) -> List[str]:
        """
        Extract the Synapse table rows to delete. This is done by comparing
        the list of dataset IDs in the Synapse table to the list of current datasets.
        If a dataset ID is found in the Synapse table that does not correspond to a current dataset,
        it is marked for deletion.

        Arguments:
            synapse_rows: The DataFrame containing the rows from the Synapse table.
            combined_dataset_name_and_id: A list of dictionaries containing dataset_id and dataset_name for all current datasets.
        """
        rows_to_delete = []

        if synapse_rows.empty:
            otel_logger.info("No rows found in Synapse table.")
            return rows_to_delete

        # Create a set of current dataset IDs for fast lookup
        current_dataset_ids = set(d["dataset_id"] for d in combined_dataset_name_and_id)

        for index, row in synapse_rows.iterrows():
            dataset_id_in_row = str(row["dataset"])
            if dataset_id_in_row not in current_dataset_ids:
                rows_to_delete.append(str(row["ROW_ID"]))

        return rows_to_delete
        
    @task
    def delete_non_current_croissant_file_in_synapse(
        root_carrier_context: Dict, combined_dataset_name_and_id: List[Dict[str, str]], **context
    ) -> None:
        """
        Delete the non-current croissant file links from Synapse table.
        This is used to remove the old links from Synapse table that are no longer needed.
        A "non-current" link is defined as a croissant file link which is no longer
        present in the data catalog.

        This can occur if the dataset has been removed from the data catalog and thus synapse table is out of date.

        Arguments:
            root_carrier_context: The root carrier context to use for the trace context.
            combined_dataset_name_and_id: a list of dictionaries containing dataset_id and dataset_name for all current datasets

        Returns:
            None
        """
        # Warning: Using an authenticated Synapse Client during this section of code
        with otel_tracer.start_as_current_span("delete_non_current_files_from_s3", context=TraceContextTextMapPropagator().extract(root_carrier_context)) as span:
            syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
            
            authenticated_syn_client: Synapse = syn_hook.client
            authenticated_syn_client._rest_call = MethodType(
                _rest_call_replacement, authenticated_syn_client)
            
            query_string = f"SELECT * FROM {SYNAPSE_TABLE_FOR_CROISSANT_LINKS}"
            table_dataframe= query(query=query_string, synapse_client=authenticated_syn_client)

            rows_to_delete = extract_synapse_rows_to_delete(
                synapse_rows=table_dataframe,
                combined_dataset_name_and_id=combined_dataset_name_and_id,
            )

            if rows_to_delete:
                delete_out_of_date_from_synapse = context["params"]["delete_out_of_date_from_synapse"]
                if delete_out_of_date_from_synapse:
                    otel_logger.info(
                        f"Deleting the following rows from Synapse: {rows_to_delete}")
                    # Delete rows from the Synapse table
                    table_to_delete_from = Table(id=SYNAPSE_TABLE_FOR_CROISSANT_LINKS)
                    table_to_delete_from.delete_rows(row_ids=[int(row_id) for row_id in rows_to_delete])
                else:
                    otel_logger.info(
                        f"Found rows to delete from Synapse, but not deleting due to `delete_out_of_date_from_synapse` param: {rows_to_delete}")
            else:
                otel_logger.info(
                    "No rows to delete from Synapse. All rows are current.")
            otel_tracer.span_processor.force_flush()
            otel_logger.handlers[0].flush()
            return None


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

        combined_dataset_name_and_id = []
        for index, row in table.iterrows():
            if pd.isnull(row["id"]):
                continue

            dataset_id = row["id"]
            dataset_name = row["name"]
            
            # save all the active dataset ids and names to prepare for cleanup later
            combined_dataset_name_and_id.append({
                "dataset_id": dataset_id,
                "dataset_name": dataset_name
            })
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

        return combined_dataset_name_and_id

    root_carrier_context = create_root_span()
    combined_dataset_name_and_id = create_and_save_jsonld()

    delete_non_current_croissant_file_in_s3(
        root_carrier_context=root_carrier_context,
        combined_dataset_name_and_id=combined_dataset_name_and_id,
    )

    delete_non_current_croissant_file_in_synapse(root_carrier_context=root_carrier_context, 
        combined_dataset_name_and_id=combined_dataset_name_and_id,
    )


save_minimal_jsonld_to_s3()