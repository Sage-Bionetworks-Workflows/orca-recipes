"""
This DAG interacts with a few different services to accomplish the following:

- (Synapse, Unauthenticated) Given list of dataset collections query Synapse to retrieve Anonymously accessible datasets
- (Synapse, Unauthenticated) For each dataset in the dataset collections, query Synapse Anonymously to retrieve metadata about each dataset
- (Synapse, Unauthenticated) For each Anonymously accessible dataset, query Synapse Anonymously to retrieve the files attached to the dataset
- (Snowflake, Authenticated) For each file which is not Anonymously downloadable, query Snowflake to retrieve the content_md5 (Authenticated calls to the datawarehouse)
- (S3, Authenticated) For each dataset push an object a public S3 bucket in the `org-sagebase-dpe-prod` AWS account
- (Synapse, Authenticated) For each dataset query a Synapse table which contains links to the S3 object
- (Synapse, Authenticated) Delete rows from the Synapse table which are not in a dataset collection
- (Synapse, Authenticated) For each dataset push a link for the S3 object to a Synapse table via the authenticated Synapse client
- (S3, Authenticated) Delete S3 objects from the S3 bucket which are not in a dataset collection


To add more datasets to be processed by this dag:
Simply add the dataset collections to the `dataset_collections` parameter in the DAG to
run this process for the given dataset collections.


For each dataset in the dataset collections, it queries the dataset in Synapse and
pushes the results to a Croissant file in S3. The S3 bucket is stored in the
`org-sagebase-dpe-prod` AWS account. That S3 bucket is deployed to AWS through the code
in this PR: https://github.com/Sage-Bionetworks-Workflows/eks-stack/pull/57 .

Additional note on the pushing to S3. The way that the S3 hook is set up is that it will
log in as an AWS user to accomplish the required work. In order for this DAG to run the
user that is running the DAG must have the correct permissions to access the S3 bucket
and write to it. This was set up on the `airflow-secrets-backend` user in the
`org-sagebase-dpe-prod` AWS account. The user has an inline policy to grant PutObject,
GetObject, DeleteObject, and ListBucket permissions to the S3 bucket.

In addition this DAG has been set up with the first iteration of the OpenTelemetry
integration for Apache Airflow. OTEL support is officially added in 2.10.0, but requires
that we also set up the production Airflow server to support OTEL.

DAG Parameters:
- Review the DAG Parameters under the `@dag` decorated function
"""

import json
import os
import re
import tempfile
import mimetypes
from datetime import datetime
from io import BytesIO
from logging import NOTSET, Logger, getLogger
from types import MethodType
from typing import Any, Dict, List
from urllib.parse import quote_plus

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from opentelemetry import context as otel_context
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
from opentelemetry.trace.propagation.tracecontext import \
    TraceContextTextMapPropagator
from orca.services.synapse import SynapseHook
from pandas import DataFrame
from synapseclient import Entity, Synapse, Table
from synapseclient.core.exceptions import (SynapseAuthenticationError,
                                           SynapseHTTPError)
from synapseclient.core.retry import with_retry
from synapseclient.models import File
from synapseclient.core.utils import delete_none_keys

dag_params = {
    "snowflake_developer_service_conn": Param("SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "dataset_collections": Param(["syn50913342"], type="array"),
    "push_results_to_s3": Param(True, type="boolean"),
    "push_links_to_synapse": Param(True, type="boolean"),
    "delete_out_of_date_from_s3": Param(True, type="boolean"),
    "delete_out_of_date_from_synapse": Param(True, type="boolean"),
    "dataset_collections_for_cleanup": Param(["syn50913342"], type="array"),
    "aws_conn_id": Param("AWS_SYNAPSE_CROISSANT_METADATA_S3_CONN", type="string"),
}

dag_config = {
    # Every Monday
    "schedule_interval": "0 0 * * 1",
    "start_date": datetime(2025, 2, 1),
    "catchup": False,
    "max_active_tasks": 4,
    "default_args": {
        "retries": 3,
    },
    "tags": ["snowflake"],
    "params": dag_params,
}

tracer = trace.get_tracer(__name__)

REGION_NAME = "us-east-1"
BUCKET_NAME = "synapse-croissant-metadata"
MY_SERVICE_VERSION = "1.0.0"
MY_SERVICE_NAME = "airflow-synapse-dataset-to-croissant"
# Used to set `deployment.environment` in the telemetry data.
# Since tracing and logging is getting set up outside of the DAG, we need to set
# the deployment environment here.
MY_DEPLOYMENT_ENVIRONMENT = "prod"
# MY_DEPLOYMENT_ENVIRONMENT = "local"

SYNAPSE_TABLE_FOR_CROISSANT_LINKS = "syn65903895"

# Regular expression to match `_{dataset_id}.{dataset_version}_`
DATASET_ID_VERSION_PATTERN = r"_syn\d+\.\d+_"

# Regular expression to match `_datasetCollection_{dataset_collection}_`
DATASET_COLLECTION_PATTERN = r"_datasetCollection_syn\d+_"


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


def get_file_instances(
    synapse_files: List[Dict[str, str]], syn_client: Synapse
) -> List[File]:
    """
    Get the file instances for the given list of files.

    Arguments:
        synapse_files: The list of files to get the file instances for.
        syn_client: The Synapse client to use to get the file instances.

    Returns:
        The list of file instances.
    """
    file_metadata: List[File] = []
    for synapse_file in synapse_files:
        try:
            file_instance = File(id=synapse_file.get(
                "file_id"), version_number=synapse_file.get("file_version"), download_file=False).get(synapse_client=syn_client)
            file_metadata.append(file_instance)
        except SynapseHTTPError as ex:
            if "404 Client Error" in str(ex):
                otel_logger.warning(
                    f"File {synapse_file.get('file_id')} not found in Synapse.")
            else:
                raise ex
        except SynapseAuthenticationError as ex:
            if "You are not logged in and do not have access to a requested resource." in str(ex):
                otel_logger.warning(
                    f"File {synapse_file.get('file_id')} is not anonymously accessible.")
            else:
                raise ex
    return file_metadata


def construct_distribution_section_for_files(files_attached_to_dataset: List[File], **context) -> List[Dict[str, str]]:
    """
    Construct the distribution section for the files attached to the dataset. This is
    used to extract various metadata from the files and create a FileObject for each
    file.


    When we do not have the content_md5 for a file, we need to query Snowflake to get
    the content_md5 for the file. This is done by querying the synapse_data_warehouse
    database in Snowflake.

    Arguments:
        files_attached_to_dataset: The list of files attached to the dataset.
        context: The context of the DAG run.

    Returns:
        The distribution section for the files attached to the dataset.
    """
    distribution_files = []
    files_to_find_md5_in_snowflake = {}

    ids_of_files = []
    id_and_version_pairs = []
    for file in files_attached_to_dataset:
        file: File = file
        modified_file_id = int(file.id.replace("syn", ""))
        ids_of_files.append(modified_file_id)
        id_and_version_pairs.append(modified_file_id)
        id_and_version_pairs.append(file.version_number)
        files_to_find_md5_in_snowflake[int(file.id.replace(
            "syn", ""))] = file.version_number

    file_md5_and_types = None
    if ids_of_files:
        snow_hook = SnowflakeHook(
            context["params"]["snowflake_developer_service_conn"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()

        query = f"""
        WITH version_data AS (
            SELECT
                nl.id,
                flattened.value:versionNumber::int AS versionNumber,
                flattened.value:contentMd5::string AS contentMd5,
                flattened.value:fileHandleId::int AS fileHandleId
            FROM synapse_data_warehouse.synapse.node_latest AS nl,
            LATERAL FLATTEN(input => nl.VERSION_HISTORY) AS flattened
            WHERE nl.id IN ({', '.join(['%s'] * len(ids_of_files))})
            AND nl.is_public = TRUE
        )
        SELECT
            vd.id,
            vd.versionNumber,
            vd.contentMd5,
            vd.fileHandleId,
            COALESCE(fl.content_type, 'NOT_SET') as CONTENT_TYPE,
        FROM version_data AS vd
        LEFT JOIN synapse_data_warehouse.synapse.file_latest fl
            ON fl.id = vd.fileHandleId
        WHERE (vd.id, vd.versionNumber) IN ({', '.join(['(%s, %s)'] * (len(id_and_version_pairs)//2))});
        """
        try:
            cs.execute(
                query,
                (ids_of_files + id_and_version_pairs),
            )
            file_md5_and_types = cs.fetch_pandas_all()
        finally:
            cs.close()

    for file in files_attached_to_dataset:
        file: File = file
        if not file_md5_and_types.empty and int(file.id.replace("syn", "")) in file_md5_and_types["ID"].values:
            file_md5 = file_md5_and_types.loc[file_md5_and_types["ID"] == int(
                file.id.replace("syn", "")), "CONTENTMD5"].values[0]
            
            file_content_type = file_md5_and_types.loc[file_md5_and_types["ID"] == int(
                file.id.replace("syn", "")), "CONTENT_TYPE"].values[0]
        else:
            file_md5 = "unknown_md5"

        if not file_content_type or file_content_type == "NOT_SET":
            if file.file_handle and file.file_handle.file_name:
                file_content_type = mimetypes.guess_type(
                    file.file_handle.file_name, strict=False)[0]
            else:
                file_content_type = mimetypes.guess_type(
                    file.name, strict=False)[0]
                
            if not file_content_type:
                # For binary documents without a specific or known subtype, application/octet-stream should be used.
                # Source: https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/MIME_types
                file_content_type = "application/octet-stream"

        distribution_files.append(
            {
                "@type": "FileObject",
                "@id": f"{file.id}.{file.version_number}",
                "name": f"{file.name}",
                "description": file.description if file.description else f"Data file associated with {file.name}",
                "contentUrl": f"https://www.synapse.org/Synapse:{file.id}.{file.version_number}",
                "encodingFormat": file_content_type,
                "md5": file_md5,
                "sha256": "unknown",
            }
        )
    return distribution_files


def construct_record_set_section_for_files(files_attached_to_dataset: List[File]) -> Dict[str, str]:
    """
    Construct the record set section for the files attached to the dataset. This is used
    to extract the keys from the annotations of the files and create a Field for each
    key.

    Arguments:
        files_attached_to_dataset: The list of files attached to the dataset.

    Returns:
        The record set section for the files attached to the dataset.
    """
    unique_annotation_keys = set()

    for file in files_attached_to_dataset:
        file: File = file
        file_annotations = file.annotations.keys()
        for key in file_annotations:
            unique_annotation_keys.add(key)

    metadata_fields = []
    # Sort the set of unique_annotation_keys ignoring case, but keep the case in the result
    unique_annotation_keys = sorted(unique_annotation_keys, key=str.casefold)

    for key in unique_annotation_keys:
        metadata_fields.append(
            {
                "@type": "Field",
                "@id": f"metadata/{key}",
                "name": key,
                "description": "",
                "dataType": "sc:Text",
                "source": {
                    "fileObject": {"@id": "metadata"},
                    "extract": {"column": key}
                }
            }
        )

    return {
        "@type": "RecordSet",
        "@id": "default",
        "name": "default",
        "description": "Metadata for the dataset",
        "field": metadata_fields
    }


def extract_s3_objects_to_delete(bucket_objects: List[str], dataset_collections_to_consider_for_deletion: List[str], combined_dataset_collection_and_datasets: List[Dict[str, str]]) -> List[str]:
    """
    Extract the S3 objects to delete from the S3 bucket. This is done by retrieving
    all of the objects in the S3 bucket and checking if the object matches one of the
    datasets that will be kept after the full DAG as been run. Every object in the S3
    bucket that matches the pattern `_syn####.###_` and
    `_datasetCollection_syn####_` will be considered for deletion. The DAG param
    `dataset_collections_for_cleanup` is used to filter the objects that will be
    considered for deletion as the second filter.

    The last filter enforces that each object in the S3 bucket must match a dataset
    id, dataset version, dataset collection, and dataset name in the
    `combined_dataset_collection_and_datasets` list. If the object does not match
    any of the objects in the list, then it will be marked for deletion.

    Arguments:
        bucket_objects: The list of objects in the S3 bucket.
        dataset_collections_to_consider_for_deletion: The dataset collections to
            consider for deletion. This is passed in from the
            `dataset_collections_for_cleanup` DAG parameter.
    """
    objects_to_delete = []

    if not bucket_objects:
        otel_logger.info(
            "No objects found in S3.")
        return objects_to_delete

    for bucket_object in bucket_objects:
        dataset_id_and_version = re.search(
            DATASET_ID_VERSION_PATTERN, bucket_object)
        dataset_collection = re.search(
            DATASET_COLLECTION_PATTERN, bucket_object)

        if not dataset_id_and_version or not dataset_collection:
            otel_logger.info(
                f"Object {bucket_object} does not match the pattern. Skipping.")
            continue

        dataset_collection = dataset_collection.group(0).replace(
            "_datasetCollection_", "").replace("_", "")

        if dataset_collection not in dataset_collections_to_consider_for_deletion:
            otel_logger.info(
                f"Object {bucket_object} does not match a dataset collection present in `dataset_collections_for_cleanup`. Skipping.")
            continue

        dataset_id, dataset_version = dataset_id_and_version.group(0).split(
            ".")
        dataset_id = dataset_id.replace("_", "")
        dataset_version = dataset_version.replace("_", "")
        dataset_name = bucket_object.split(dataset_id_and_version.group(0))[0]

        match_found_for_object = False

        for combined_dataset_collection_and_dataset in combined_dataset_collection_and_datasets:
            if (dataset_collection == combined_dataset_collection_and_dataset["dataset_collection"] and
                    dataset_id == combined_dataset_collection_and_dataset["dataset_id"] and
                    dataset_version == str(combined_dataset_collection_and_dataset["dataset_version"]) and
                    dataset_name == combined_dataset_collection_and_dataset["dataset_name"]):
                match_found_for_object = True
                break
        if not match_found_for_object:
            objects_to_delete.append(bucket_object)

    return objects_to_delete


def extract_synapse_rows_to_delete(synapse_rows: DataFrame, combined_dataset_collection_and_datasets: List[Dict[str, str]]) -> List[str]:
    """
    Extract the rows to delete from the Synapse table. This is done by retrieving the
    dataset ids along with their versions from the dataset collections passed in via
    the `dataset_collections_for_cleanup` DAG parameter. The rows to delete are
    determined by checking if the dataset id and version are present in the
    `combined_dataset_collection_and_datasets` list. If the dataset id and version
    are not present in the list, then the row is marked for deletion.

    Arguments:
        synapse_rows: A Pandas DataFrame with the columns `dataset`, `dataset_version`,
            `dataset_collection`, and `ROW_ID`.
        combined_dataset_collection_and_datasets: The combined list of dataset IDs,
            dataset collections, and dataset versions to use to determine which
            datasets to delete.

    Returns:
        The list of rows to delete from the Synapse table.
    """
    rows_to_delete = []

    if synapse_rows.empty:
        otel_logger.info(
            "No rows found in Synapse.")
        return rows_to_delete

    for synapse_row in synapse_rows.itertuples():
        dataset_collection = synapse_row.dataset_collection
        dataset_id = synapse_row.dataset
        dataset_version = synapse_row.dataset_version
        row_id = synapse_row.ROW_ID

        match_found_for_object = False

        for combined_dataset_collection_and_dataset in combined_dataset_collection_and_datasets:
            if (dataset_collection == combined_dataset_collection_and_dataset["dataset_collection"] and
                    dataset_id == combined_dataset_collection_and_dataset["dataset_id"] and
                    dataset_version == combined_dataset_collection_and_dataset["dataset_version"]):
                match_found_for_object = True
                break
        if not match_found_for_object:
            rows_to_delete.append(row_id)

    return rows_to_delete


def execute_push_to_s3(dataset: Entity, dataset_id: str, dataset_version: str, s3_key: str, croissant_file: Dict[str, Any], push_to_s3: bool, **context) -> None:
    """
    Handle the push to S3 of the croissant file. This is done by using the S3Hook to
    upload the file to S3. The S3 bucket is stored in the `org-sagebase-dpe-prod` AWS
    account.

    Arguments:
        dataset: The dataset to push to S3.
        dataset_id: The ID of the dataset.
        dataset_version: The version of the dataset.
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
                f"Croissant file for [dataset: {dataset.name}, id: {dataset_id}.{dataset_version}]:\n{json.dumps(croissant_file)}")
            return

        otel_logger.info(
            f"Uploading croissant file for [dataset: {dataset.name}, id: {dataset_id}.{dataset_version}]")

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


def execute_push_to_synapse(push_to_synapse: bool, dataset: Entity, dataset_id: str, dataset_version: str, dataset_collection: str, s3_url: str, **context) -> None:
    """
    Handle the push to Synapse of the croissant file link. This is done by using
    an unauthenticated Synapse client to first query the table to determine if an
    update is needed. If the link already exists with the expected S3 URL, then
    skip the update. If the link does not exist or the S3 URL is different, then
    update the link with the new S3 URL using the authenticated Synapse client.

    Arguments:
        push_to_synapse: A boolean to indicate if the results should be pushed to
            Synapse. When set to `False`, the results will be printed to the logs.
        dataset: The dataset to push to Synapse.
        dataset_id: The ID of the dataset.
        dataset_version: The version of the dataset.
        dataset_collection: The ID of the dataset collection where the dataset came from.
        s3_url: The S3 URL to use for the value of the cell in the table.
        syn_client: The unauthenticated Synapse client to use to query the table.
        context: The context of the DAG run.

    Returns:
        None
    """
    try:
        if not push_to_synapse:
            otel_logger.info(
                f"Croissant file link for [dataset: {dataset.name}, id: {dataset_id}.{dataset_version}]: {s3_url}")
            return

        otel_logger.info(
            f"Uploading croissant file link to Synapse table {SYNAPSE_TABLE_FOR_CROISSANT_LINKS}"
        )

        # TODO: When 4.8.0 of the SYNPY client is released this may be
        # replaced by the upsert functionality

        # Warning: Using an authenticated Synapse Client during this section of code
        syn_hook = SynapseHook(
            context["params"]["synapse_conn_id"])
        authenticated_syn_client: Synapse = syn_hook.client
        authenticated_syn_client._rest_call = MethodType(
            _rest_call_replacement, authenticated_syn_client)
        existing_row = authenticated_syn_client.tableQuery(
            query=f"SELECT * FROM {SYNAPSE_TABLE_FOR_CROISSANT_LINKS} WHERE dataset = '{dataset_id}' AND dataset_version = {dataset_version} AND dataset_collection = '{dataset_collection}'", resultsAs="csv")

        try:
            existing_row_df = existing_row.asDataFrame()
        finally:
            os.remove(existing_row.filepath)
        if not existing_row_df.empty and existing_row_df["croissant_file_s3_object"].values[0] == s3_url:
            otel_logger.info(
                f"Croissant file link already exists in Synapse table {SYNAPSE_TABLE_FOR_CROISSANT_LINKS}. Skipping.")
            return

        df = DataFrame(
            data={
                "dataset": [dataset_id],
                "dataset_version": [dataset_version],
                "dataset_collection": [dataset_collection],
                "croissant_file_s3_object": [s3_url]
            }
        )
        temp_dir = tempfile.mkdtemp()
        filepath = os.path.join(temp_dir, "table.csv")

        schema = authenticated_syn_client.get(
            SYNAPSE_TABLE_FOR_CROISSANT_LINKS)
        if existing_row_df.empty:
            # If the row does not exist, create a new row
            authenticated_syn_client.store(
                Table(schema=schema, values=df, filepath=filepath))
        else:
            # Update the existing row with the new value
            existing_row_df["croissant_file_s3_object"] = [s3_url]
            authenticated_syn_client.store(
                Table(schema=schema, values=existing_row_df, filepath=filepath))

        os.remove(filepath)
    except Exception as ex:
        otel_logger.exception(
            "Failed to push croissant file link to Synapse.")
        otel_tracer.span_processor.force_flush()
        otel_logger.handlers[0].flush()
        raise ex


@dag(**dag_config)
def dataset_to_croissant() -> None:
    """Execute api calls to Synapse, and queries on snowflake to convert datasets into croissant JSON-LD files.


    DAG Parameters:
    - `snowflake_developer_service_conn`: A JSON-formatted string containing the connection details required to authenticate and connect to Snowflake.
    - `synapse_conn_id`: The connection ID for the Synapse connection.
    - `dataset_collections`: The dataset collections to query for datasets.
    - `push_results_to_s3`: A boolean to indicate if the results should be pushed to S3.
                            When set to `False`, the results will be printed to the logs.
    - `delete_out_of_date_from_s3`: A boolean to indicate if the old files should be 
                            deleted from S3. When set to `False`, the old files will not 
                            be deleted, but a message will be logged to indicate that
                            the files will be deleted when set to `True`.
    - `push_links_to_synapse`: A boolean to indicate if the links should be pushed to
                            Synapse. When set to `False`, the links will not be pushed,
                            but a message will be logged to indicate that the links will
                            be pushed when set to `True`.
    - `delete_out_of_date_from_synapse`: A boolean to indicate if the old files should be
                            deleted from Synapse. When set to `False`, the old files will
                            not be deleted, but a message will be logged to indicate that
                            the files will be deleted when set to `True`.
    - `dataset_collections_for_cleanup`: The dataset collections to query for datasets
                            to delete the old files from S3. Also consider the Synapse table
                            where links back to Synapse entities are connected with the S3
                            object. When this is filled this array is used to filter the
                            S3 objects which will be considered for deletion.
    - `aws_conn_id`: The connection ID for the AWS connection. Used to authenticate with S3.
    """

    @task
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

    @task
    def get_dataset_collections(root_carrier_context: Dict, **context) -> List[Dict[str, str]]:
        """
        Split the dataset collection ids into a format that is usable by the forking
        operation of the `expand_kwargs` function in Airflow.

        Returns:
            The dataset collections in a format that can be used by the
            `expand_kwargs` function.
        """
        with otel_tracer.start_span("get_dataset_collections", context=TraceContextTextMapPropagator().extract(root_carrier_context)):
            dataset_collections = context["params"]["dataset_collections"]
            result = [{"dataset_collection": dataset_collection}
                      for dataset_collection in dataset_collections]
        otel_tracer.span_processor.force_flush()
        return result

    @task
    def query_synapse_dataset_collection_for_datasets(
        root_carrier_context: Dict, dataset_collection: str, **context
    ) -> List[Dict[str, str]]:
        """
        Query the dataset_collection to get the IDs for the dataset we are going to
        be running this process for.

        Arguments:
            dataset_collection: The dataset collection to query for datasets.

        Returns:
            The list of dataset IDs for the given dataset collection.
        """
        with otel_tracer.start_span("query_synapse_dataset_collection_for_datasets", context=TraceContextTextMapPropagator().extract(root_carrier_context)) as span:
            span.set_attribute("airflow.dataset_collection",
                               dataset_collection)

            otel_logger.info(
                f"Performing query for dataset_collection {dataset_collection}")

            syn_client = create_syn_client()

            try:
                results = []
                dataset_collection_entity = syn_client.get(
                    dataset_collection, downloadFile=False)

                if not hasattr(dataset_collection_entity, "datasetItems") or not dataset_collection_entity.get("datasetItems"):
                    otel_logger.warning(
                        f"No datasets found for dataset collection {dataset_collection}")
                    return results
                dataset_items = dataset_collection_entity.get("datasetItems")
                otel_logger.info(dataset_items)
                for dataset_item in dataset_items:
                    syn_id = dataset_item["entityId"]
                    version_number = dataset_item["versionNumber"]

                    try:
                        dataset = syn_client.get(
                            f"{syn_id}.{version_number}", downloadFile=False)
                        span.set_attribute(
                            "airflow.dataset_name", dataset.name)
                    except Exception as ex:
                        otel_logger.warning(
                            f"Failed to get dataset {syn_id}.{version_number} from Synapse: {ex}")

                    results.append({"dataset_id": syn_id, "dataset_version": version_number,
                                   "dataset_collection": dataset_collection, "dataset_name": dataset.name})

            except Exception as ex:
                otel_logger.exception(
                    "Failed to query synapse for datasets.")
                otel_tracer.span_processor.force_flush()
                otel_logger.handlers[0].flush()
                raise ex

        otel_tracer.span_processor.force_flush()
        otel_logger.handlers[0].flush()
        return results

    @task
    def combine_dataset_lists(root_carrier_context: Dict, dataset_ids: List[List[Dict[str, str]]]) -> List[Dict[str, str]]:
        """
        Combine the dataset IDs into a single list. This is used to force the
        tasks to join together before being expanded again.

        Arguments:
            dataset_ids: The list of dataset IDs to combine.

        Returns:
            The combined list of dataset IDs.
        """
        with otel_tracer.start_as_current_span("combine_dataset_lists", context=TraceContextTextMapPropagator().extract(root_carrier_context)):
            return_value = [
                dataset for datasets in dataset_ids for dataset in datasets]
        otel_tracer.span_processor.force_flush()
        return return_value

    @task
    def delete_non_current_croissant_file_in_s3(
        root_carrier_context: Dict, combined_dataset_collection_and_datasets: List[Dict[str, str]], **context
    ) -> None:
        """
        Delete the non-current croissant files from S3. 
        This is used to remove the old files from S3 that are no longer needed. 
        A "non-current" file is defined as a croissant JSON LD file which is no longer
        present in any Dataset collection.

        This can occur if the dataset has been removed from the dataset collection, or
        if there is a new version of the dataset that has been added to the dataset
        collection.

        Arguments:
            root_carrier_context: The root carrier context to use for the trace context.
            combined_dataset_collection_and_datasets: The combined list of dataset IDs,
                dataset collections, and dataset versions to use to determine which
                datasets to delete.

        Returns:
            None
        """
        with otel_tracer.start_as_current_span("delete_non_current_files_from_s3", context=TraceContextTextMapPropagator().extract(root_carrier_context)) as span:
            s3_hook = S3Hook(
                aws_conn_id=context["params"]["aws_conn_id"], region_name=REGION_NAME)
            bucket_objects = s3_hook.list_keys(bucket_name=BUCKET_NAME)

            objects_to_delete = extract_s3_objects_to_delete(
                bucket_objects=bucket_objects,
                dataset_collections_to_consider_for_deletion=context[
                    "params"]["dataset_collections_for_cleanup"],
                combined_dataset_collection_and_datasets=combined_dataset_collection_and_datasets,
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

    @task
    def delete_non_current_croissant_file_in_synapse(
        root_carrier_context: Dict, combined_dataset_collection_and_datasets: List[Dict[str, str]], **context
    ) -> None:
        """
        Delete the non-current croissant files from the Synapse table. 
        This is used to remove the old files from Synapse that are no longer needed. 
        A "non-current" file is defined as a croissant JSON LD file which is no longer
        present in any Dataset collection.

        This can occur if the dataset has been removed from the dataset collection, or
        if there is a new version of the dataset that has been added to the dataset
        collection.

        Arguments:
            root_carrier_context: The root carrier context to use for the trace context.
            combined_dataset_collection_and_datasets: The combined list of dataset IDs,
                dataset collections, and dataset versions to use to determine which
                datasets to delete.

        Returns:
            None
        """
        # Warning: Using an authenticated Synapse Client during this section of code
        with otel_tracer.start_as_current_span("delete_non_current_files_from_s3", context=TraceContextTextMapPropagator().extract(root_carrier_context)) as span:
            syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
            syn_client: Synapse = syn_hook.client
            syn_client._rest_call = MethodType(
                _rest_call_replacement, syn_client)
            dataset_collections_to_consider_for_deletion = [f"'{ds}'" for ds in context[
                "params"]["dataset_collections_for_cleanup"]]
            query = f"SELECT * FROM {SYNAPSE_TABLE_FOR_CROISSANT_LINKS} WHERE dataset_collection in ({','.join(dataset_collections_to_consider_for_deletion)})"
            table_results = syn_client.tableQuery(
                query=query, resultsAs="csv")
            table_dataframe = table_results.asDataFrame(
                rowIdAndVersionInIndex=False)
            os.remove(table_results.filepath)
            rows_to_delete = extract_synapse_rows_to_delete(
                synapse_rows=table_dataframe,
                combined_dataset_collection_and_datasets=combined_dataset_collection_and_datasets,
            )

            if rows_to_delete:
                delete_out_of_date_from_synapse = context["params"]["delete_out_of_date_from_synapse"]
                if delete_out_of_date_from_synapse:
                    otel_logger.info(
                        f"Deleting the following rows from Synapse: {rows_to_delete}")
                    results_from_synapse = syn_client.tableQuery(
                        query=f"SELECT ROW_ID, ROW_VERSION FROM {SYNAPSE_TABLE_FOR_CROISSANT_LINKS} WHERE ROW_ID in ({','.join(rows_to_delete)})", resultsAs="csv")
                    syn_client.delete(results_from_synapse)
                    os.remove(results_from_synapse.filepath)
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
    def query_and_push_croissant_file(root_carrier_context: Dict, dataset_id: str, dataset_version: int, dataset_collection: str, **context) -> None:
        """
        Query the Snowflake database for the dataset in order to perform the Croissant
        transformation and push the results to S3.

        Arguments:
            dataset_id: The ID of the dataset to query in Snowflake.
        """
        with otel_tracer.start_as_current_span("query_and_push_croissant_file", context=TraceContextTextMapPropagator().extract(root_carrier_context)) as span:
            push_to_s3 = context["params"]["push_results_to_s3"]
            push_to_synapse = context["params"]["push_links_to_synapse"]
            span.set_attribute("airflow.push_results_to_s3", push_to_s3)
            span.set_attribute(
                "airflow.push_links_to_synapse", push_to_synapse)
            if not dataset_id:
                otel_logger.info("No dataset found")
                otel_tracer.span_processor.force_flush()
                otel_logger.handlers[0].flush()
                return

            span.set_attribute("airflow.dataset_id", f"{dataset_id}")
            path_to_remove = None
            syn_client = create_syn_client()
            file_ids_and_versions_attached_to_dataset = []

            try:
                dataset = syn_client.get(
                    f"{dataset_id}.{dataset_version}", downloadFile=False)
                span.set_attribute("airflow.dataset_name", dataset.name)

                if not hasattr(dataset, "datasetItems") or not dataset.get("datasetItems"):
                    span.set_attribute("airflow.croissant_result", False)
                    otel_logger.warning(
                        f"No files found for dataset {dataset_id}.{dataset_version}")
                    return
                dataset_items = dataset.get("datasetItems")
                for dataset_item in dataset_items:
                    syn_id = dataset_item["entityId"]
                    version_number = dataset_item["versionNumber"]
                    file_ids_and_versions_attached_to_dataset.append(
                        {"file_id": syn_id, "file_version": version_number})

            except Exception as ex:
                otel_logger.exception(
                    f"Failed to query synapse dataset for files. {dataset_id}")
                otel_tracer.span_processor.force_flush()
                otel_logger.handlers[0].flush()
                raise ex
            finally:
                if path_to_remove:
                    os.remove(path_to_remove)

            file_ids_and_versions_attached_to_dataset.sort(
                key=lambda x: x["file_id"])

            otel_logger.info(file_ids_and_versions_attached_to_dataset)
            files_attached_to_dataset: List[File] = get_file_instances(
                synapse_files=file_ids_and_versions_attached_to_dataset, syn_client=syn_client)

            if not files_attached_to_dataset:
                otel_logger.warning(
                    f"No files found for dataset {dataset_id}.{dataset_version}")
                span.set_attribute("airflow.croissant_result", False)
                return

            span.set_attribute("airflow.croissant_result", True)

            distribution_files = [{
                "@type": "FileObject",
                "@id": "metadata",
                "contentUrl": f"https://www.synapse.org/Synapse:{dataset_id}.{dataset_version}",
                "name": "metadata",
                "description": f"Metadata associated with {dataset.name}",
                "encodingFormat": "application/csv",
                "md5": "unknown",
                "sha256": "unknown"
            }] + construct_distribution_section_for_files(files_attached_to_dataset, **context)

            record_set = construct_record_set_section_for_files(
                files_attached_to_dataset=files_attached_to_dataset)

            croissant_file = {
                "@context": {
                    "@language": "en",
                    "@vocab": "https://schema.org/",
                    "citeAs": "cr:citeAs",
                    "column": "cr:column",
                    "conformsTo": "dct:conformsTo",
                    "cr": "http://mlcommons.org/croissant/",
                    "rai": "http://mlcommons.org/croissant/RAI/",
                    "data": {
                        "@id": "cr:data",
                        "@type": "@json"
                    },
                    "dataType": {
                        "@id": "cr:dataType",
                        "@type": "@vocab"
                    },
                    "dct": "http://purl.org/dc/terms/",
                    "examples": {
                        "@id": "cr:examples",
                        "@type": "@json"
                    },
                    "extract": "cr:extract",
                    "field": "cr:field",
                    "fileProperty": "cr:fileProperty",
                    "fileObject": "cr:fileObject",
                    "fileSet": "cr:fileSet",
                    "format": "cr:format",
                    "includes": "cr:includes",
                    "isLiveDataset": "cr:isLiveDataset",
                    "jsonPath": "cr:jsonPath",
                    "key": "cr:key",
                    "md5": "cr:md5",
                    "parentField": "cr:parentField",
                    "path": "cr:path",
                    "recordSet": "cr:recordSet",
                    "references": "cr:references",
                    "regex": "cr:regex",
                    "repeated": "cr:repeated",
                    "replace": "cr:replace",
                    "sc": "https://schema.org/",
                    "separator": "cr:separator",
                    "source": "cr:source",
                    "subField": "cr:subField",
                    "transform": "cr:transform"
                },
                "@type": "Dataset",
                "@id": f"{dataset_id}.{dataset_version}",
                "name": f"{dataset.name}",
                "description": dataset.description if hasattr(dataset, "description") and dataset.description else f"Dataset for {dataset.name}",
                "url": f"https://www.synapse.org/Synapse:{dataset_id}.{dataset_version}",
                "datePublished": dataset.modifiedOn,
                "license": dataset.license[0] if hasattr(dataset, "license") else "unknown_license",
                "version": dataset_version,
                "dct:conformsTo": "http://mlcommons.org/croissant/1.0",
                "distribution": distribution_files,
                "recordSet": record_set,
                # https://github.com/nf-osi/nf-metadata-dictionary/blob/main/registered-json-schemas/PortalDataset.json
                # These fields are derived from: https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/refs/heads/main/registered-json-schemas/PortalDataset.json
                # If a more specific schema is needed, then we can add a mapping for the dataset to point to the schema and pull these in dynamically.
                "accessType": dataset.accessType[0] if hasattr(dataset, "accessType") else None,
                "alternateName": dataset.alternateName[0] if hasattr(dataset, "alternateName") else None,
                "citation": dataset.citation[0] if hasattr(dataset, "citation") else None,
                "conditionsOfAccess": dataset.conditionsOfAccess[0] if hasattr(dataset, "conditionsOfAccess") else None,
                "countryOfOrigin": dataset.countryOfOrigin if hasattr(dataset, "countryOfOrigin") else None,
                "dataType": dataset.dataType if hasattr(dataset, "dataType") else None,
                "dataUseModifiers": dataset.dataUseModifiers if hasattr(dataset, "dataUseModifiers") else None,
                "diseaseFocus": dataset.diseaseFocus if hasattr(dataset, "diseaseFocus") else None,
                "funder": dataset.funder if hasattr(dataset, "funder") else None,
                "individualCount": dataset.individualCount[0] if hasattr(dataset, "individualCount") else None,
                "keywords": dataset.keywords if hasattr(dataset, "keywords") else None,
                "manifestation": dataset.manifestation if hasattr(dataset, "manifestation") else None,
                "measurementTechnique": dataset.measurementTechnique if hasattr(dataset, "measurementTechnique") else None,
                "series": dataset.series[0] if hasattr(dataset, "series") else None,
                "species": dataset.species if hasattr(dataset, "species") else None,
                "specimenCount": dataset.specimenCount[0] if hasattr(dataset, "specimenCount") else None,
                "studyId": dataset.studyId[0] if hasattr(dataset, "studyId") else None,
                "subject": dataset.subject if hasattr(dataset, "subject") else None,
                "title": dataset.title[0] if hasattr(dataset, "title") else None,
                "visualizeDataOn": dataset.visualizeDataOn if hasattr(dataset, "visualizeDataOn") else None,
                "yearProcessed": dataset.yearProcessed[0] if hasattr(dataset, "yearProcessed") else None
            }
            delete_none_keys(croissant_file)

            # The logic to push the file to S3 and push to Synapse is occurring within
            # the same DAG task is to prevent any issues with the large amount of
            # content that would need to be passed between tasks.
            s3_key = f"{dataset.name}_{dataset_id}.{dataset_version}_datasetCollection_{dataset_collection}_croissant.jsonld"
            execute_push_to_s3(dataset=dataset, dataset_id=dataset_id, dataset_version=dataset_version,
                               s3_key=s3_key, croissant_file=croissant_file, push_to_s3=push_to_s3, **context)

            s3_url = f"https://{BUCKET_NAME}.s3.us-east-1.amazonaws.com/{quote_plus(s3_key)}"
            execute_push_to_synapse(push_to_synapse=push_to_synapse, dataset=dataset, dataset_id=dataset_id,
                                    dataset_version=dataset_version, dataset_collection=dataset_collection, s3_url=s3_url, **context)

        otel_tracer.span_processor.force_flush()
        otel_logger.handlers[0].flush()

    root_carrier_context = create_root_span()

    datasets = query_synapse_dataset_collection_for_datasets.partial(root_carrier_context=root_carrier_context).expand_kwargs(
        get_dataset_collections(root_carrier_context=root_carrier_context))

    combined_dataset_lists = combine_dataset_lists(
        root_carrier_context=root_carrier_context, dataset_ids=datasets)

    query_and_push_croissant_file.partial(root_carrier_context=root_carrier_context).expand_kwargs(
        combined_dataset_lists)

    delete_non_current_croissant_file_in_s3(root_carrier_context=root_carrier_context,
                                            combined_dataset_collection_and_datasets=combined_dataset_lists)

    delete_non_current_croissant_file_in_synapse(root_carrier_context=root_carrier_context,
                                                 combined_dataset_collection_and_datasets=combined_dataset_lists)


dataset_to_croissant()
