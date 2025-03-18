"""
This DAG queries snowflake for the given dataset collections to look for datasets.

For each dataset in the dataset collections, it queries the dataset in Snowflake and 
pushes the results to a Croissant file in S3. The S3 bucket is stored in the
`org-sagebase-dpe-prod` AWS account. That S3 bucket is deployed to AWS through the code
in this PR: https://github.com/Sage-Bionetworks-Workflows/eks-stack/pull/57 .

Additional note on the pushing to S3. The way that the S3 hook is set up is that it will
log in as an AWS user to accomplish the required work. In order for this DAG to run the
user that is running the DAG must have the correct permissions to access the S3 bucket
and write to it. This was set up on the `airflow-secrets-backend` user in the 
`org-sagebase-dpe-prod` AWS account. The user has an inline policy to grant PutObject,
GetObject, and ListBucket permissions to the S3 bucket.

Simply add the dataset collections to the `dataset_collections` parameter in the DAG to
run this process for the given dataset collections.

In addition this DAG has been set up with the first iteration of the OpenTelemetry
integration for Apache Airflow. OTEL support is officially added in 2.10.0, but requires
that we also upgrade the production Airflow server.

DAG Parameters:
- `snowflake_conn_id`: The connection ID for the Snowflake connection.
- `synapse_conn_id`: The connection ID for the Synapse connection.
- `dataset_collections`: The dataset collections to query for datasets.
- `push_results_to_s3`: A boolean to indicate if the results should be pushed to S3.
                        When set to `False`, the results will be printed to the logs.
- `aws_conn_id`: The connection ID for the AWS connection. Used to authenticate with S3.
"""

import os
from datetime import datetime
from io import BytesIO
from logging import NOTSET, Logger, getLogger
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models import Variable
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

dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_SYSADMIN_PORTAL_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "dataset_collections": Param(["syn50913342"], type="array"),
    "push_results_to_s3": Param(True, type="boolean"),
    "aws_conn_id": Param("AWS_SYNAPSE_CROISSANT_METADATA_S3_CONN", type="string"),
}

dag_config = {
    # Every Monday
    "schedule_interval": "0 0 * * 1",
    "start_date": datetime(2025, 2, 1),
    "catchup": False,
    "default_args": {
        "retries": 1,
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
    getLogger(__name__).addHandler(handler)
    return getLogger(__name__)


otel_tracer = set_up_tracing()
otel_logger = set_up_logging()


@dag(**dag_config)
def dataset_to_croissant() -> None:
    """Execute a query on Snowflake and report the results to a Synapse table."""

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
            dataset_collection_without_syn = dataset_collection.replace(
                "syn", "")
            query = f"""
            SELECT 
                REPLACE(ITEM.value:entityId::STRING, 'syn', '') AS DATASET_IDS
            FROM 
                SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST,
            LATERAL FLATTEN(input => ITEMS) ITEM
            WHERE
                node_type = 'datasetcollection'
                AND id = {dataset_collection_without_syn}
            """
            snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
            ctx = snow_hook.get_conn()
            cs = ctx.cursor()

            otel_logger.info(
                f"Performing query for dataset_collection {dataset_collection}")

            try:
                cs.execute(query)
                datasets = cs.fetch_pandas_all()
                cs.close()
                results = []
                for syn_id in datasets["DATASET_IDS"].tolist():
                    results.append({"dataset_id": syn_id.replace("syn", "")})
                results
            except Exception as ex:
                otel_logger.exception(
                    "Failed to query snowflake for datasets.")
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
        with otel_tracer.start_as_current_span("combine_dataset_lists", context=TraceContextTextMapPropagator().extract(root_carrier_context)) as span:
            return_value = [
                dataset for datasets in dataset_ids for dataset in datasets]
        otel_tracer.span_processor.force_flush()
        return return_value

    @task
    def query_snowflake_and_push_croissant_file(root_carrier_context: Dict, dataset_id: str, **context) -> None:
        """
        Query the Snowflake database for the dataset in order to perform the Croissant
        transformation and push the results to S3.

        Arguments:
            dataset_id: The ID of the dataset to query in Snowflake.
        """
        with otel_tracer.start_as_current_span("query_snowflake_and_push_croissant_file", context=TraceContextTextMapPropagator().extract(root_carrier_context)) as span:
            if not dataset_id:
                otel_logger.info("No dataset found")
                otel_tracer.span_processor.force_flush()
                otel_logger.handlers[0].flush()
                return

            span.set_attribute("airflow.dataset_id", f"syn{dataset_id}")

            query = f"""
            WITH entity_metadata AS (
                SELECT
                    items,
                    name,
                    ANNOTATIONS,
                    CREATED_ON,
                    VERSION_NUMBER,
                    CONCAT('syn', id) as DATASET_SYNAPSE_ID
                FROM synapse_data_warehouse.synapse.node_latest
                WHERE id = {dataset_id}
            ),
            ids_of_files_belonging_to_dataset AS (
                SELECT REPLACE(ITEM.value:entityId::STRING, 'syn', '') AS entity_ids
                FROM entity_metadata,
                LATERAL FLATTEN(input => entity_metadata.ITEMS) ITEM
            ),
            files_belonging_to_dataset AS (
                SELECT
                    nl.id,
                    nl.VERSION_HISTORY,
                    nl.ANNOTATIONS,
                    DATASET_SYNAPSE_ID
                FROM synapse_data_warehouse.synapse.node_latest as nl, entity_metadata
                WHERE nl.is_public = True
                    AND nl.id IN (SELECT * FROM ids_of_files_belonging_to_dataset)
            ),
            distribution_pointing_to_files AS (
                SELECT
                    CONCAT('syn', id) as SYNAPSE_ID,
                    DATASET_SYNAPSE_ID,
                    OBJECT_CONSTRUCT(
                        '@type', 'FileObject',
                        '@id', CONCAT('syn', id),
                        'name', CONCAT('syn', id),
                        'description', CONCAT(
                            'Data file associated with ', CONCAT('syn', id)),
                        'contentUrl', CONCAT(
                            'https://www.synapse.org/Synapse:', CONCAT('syn', id)),
                        'encodingFormat', 'application/json',
                        'md5', COALESCE(VERSION_HISTORY[0]:contentMd5, 'unknown_md5'),
                        'sha256', 'unknown'
                    ) AS file_object
                FROM files_belonging_to_dataset
            ),
            distinct_annotation_keys AS (
                SELECT DATASET_SYNAPSE_ID, ARRAY_AGG(annotation_key) as annotation_keys FROM (
                    SELECT
                        DATASET_SYNAPSE_ID,
                        key as annotation_key
                    FROM files_belonging_to_dataset,
                    LATERAL FLATTEN(input => ANNOTATIONS:annotations) AS annotation
                    GROUP BY DATASET_SYNAPSE_ID, key
                ) GROUP BY DATASET_SYNAPSE_ID
            ),
            fields_pointing_to_metadata AS (
                SELECT
                    DATASET_SYNAPSE_ID,
                    OBJECT_CONSTRUCT(
                        '@type', 'RecordSet',
                        '@id', 'default',
                        'name', 'default',
                        'description', 'Metadata for the dataset',
                        'field', ARRAY_AGG(
                            OBJECT_CONSTRUCT(
                                '@type', 'cr:Field',
                                '@id', CONCAT('metadata', '/', value),
                                'name', value,
                                -- 'description', '',
                                'dataType', 'sc:Text',
                                'source', OBJECT_CONSTRUCT(
                                    'fileObject', OBJECT_CONSTRUCT(
                                        '@id', 'metadata'),
                                    'extract', OBJECT_CONSTRUCT('column', value)
                                )
                            )
                        )
                    ) AS record_set
                FROM distinct_annotation_keys,
                LATERAL FLATTEN(input => distinct_annotation_keys.annotation_keys)
                GROUP BY DATASET_SYNAPSE_ID
            ),
            croissant_metadata AS (
                SELECT
                    entity_metadata.DATASET_SYNAPSE_ID,
                    entity_metadata.name,
                    OBJECT_CONSTRUCT(
                        '@context', OBJECT_CONSTRUCT(
                            '@language', 'en',
                            '@vocab', 'https://schema.org/',
                            'citeAs', 'cr:citeAs',
                            'column', 'cr:column',
                            'conformsTo', 'dct:conformsTo',
                            'cr', 'http://mlcommons.org/croissant/',
                            'rai', 'http://mlcommons.org/croissant/RAI/',
                            'data', OBJECT_CONSTRUCT(
                                '@id', 'cr:data', '@type', '@json'),
                            'dataType', OBJECT_CONSTRUCT(
                                '@id', 'cr:dataType', '@type', '@vocab'),
                            'dct', 'http://purl.org/dc/terms/',
                            'examples', OBJECT_CONSTRUCT(
                                '@id', 'cr:examples', '@type', '@json'),
                            'extract', 'cr:extract',
                            'field', 'cr:field',
                            'fileProperty', 'cr:fileProperty',
                            'fileObject', 'cr:fileObject',
                            'fileSet', 'cr:fileSet',
                            'format', 'cr:format',
                            'includes', 'cr:includes',
                            'isLiveDataset', 'cr:isLiveDataset',
                            'jsonPath', 'cr:jsonPath',
                            'key', 'cr:key',
                            'md5', 'cr:md5',
                            'parentField', 'cr:parentField',
                            'path', 'cr:path',
                            'recordSet', 'cr:recordSet',
                            'references', 'cr:references',
                            'regex', 'cr:regex',
                            'repeated', 'cr:repeated',
                            'replace', 'cr:replace',
                            'sc', 'https://schema.org/',
                            'separator', 'cr:separator',
                            'source', 'cr:source',
                            'subField', 'cr:subField',
                            'transform', 'cr:transform'
                        ),
                        '@type', 'Dataset',
                        'name', COALESCE(entity_metadata.name, 'Synapse Dataset'),
                        'description', COALESCE(ANNOTATIONS:annotations:description:value[0], 'A dataset containing annotations for genomic data from Synapse.'),
                        'url', CONCAT('https://www.synapse.org/Synapse:',
                                    entity_metadata.DATASET_SYNAPSE_ID),
                        'citation', COALESCE(ANNOTATIONS:annotations:citation:value[0], 'unknown_citation'),
                        'datePublished', COALESCE(CREATED_ON, 'unknown_date'),
                        'license', COALESCE(ANNOTATIONS:annotations:license:value[0], 'unknown_license'),
                        'version', COALESCE(VERSION_NUMBER, 'unknown_version'),
                        'distribution', ARRAY_PREPEND(ARRAY_AGG(distribution_pointing_to_files.file_object),
                                            OBJECT_CONSTRUCT(
                                                '@type', 'FileObject',
                                                '@id', 'metadata',
                                                'name', 'metadata',
                                                'description', CONCAT(
                                                    'Metadata associated with ', entity_metadata.DATASET_SYNAPSE_ID),
                                                'contentUrl', CONCAT(
                                                    'https://www.synapse.org/Synapse:', entity_metadata.DATASET_SYNAPSE_ID),
                                                'encodingFormat', 'application/csv',
                                                'md5', '',
                                                'sha256', 'unknown'
                                            )
                                        ),
                        'recordSet', fields_pointing_to_metadata.record_set
                    ) AS metadata
                FROM
                    entity_metadata, fields_pointing_to_metadata
                JOIN
                    distribution_pointing_to_files
                ON
                    distribution_pointing_to_files.DATASET_SYNAPSE_ID = fields_pointing_to_metadata.DATASET_SYNAPSE_ID
                GROUP BY
                    entity_metadata.DATASET_SYNAPSE_ID,
                    entity_metadata.name,
                    fields_pointing_to_metadata.DATASET_SYNAPSE_ID,
                    fields_pointing_to_metadata.record_set,
                    entity_metadata.name,
                    entity_metadata.ANNOTATIONS,
                    entity_metadata.CREATED_ON,
                    entity_metadata.VERSION_NUMBER
            )

            SELECT
                metadata::STRING AS croissant_metadata,
                DATASET_SYNAPSE_ID,
                name
            FROM
                croissant_metadata;
            """
            snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
            ctx = snow_hook.get_conn()
            cs = ctx.cursor()

            otel_logger.info(
                f"Performing query for dataset {dataset_id}")

            try:
                push_to_s3 = context["params"]["push_results_to_s3"]
                span.set_attribute("airflow.push_results_to_s3", push_to_s3)

                cs.execute(query)
                croissant_json_ld_for_dataset = cs.fetch_pandas_all()
                cs.close()

                if croissant_json_ld_for_dataset.empty:
                    span.set_attribute("airflow.croissant_result", False)
                    otel_logger.info(
                        f"No content found for {dataset_id}")
                    otel_tracer.span_processor.force_flush()
                    otel_logger.handlers[0].flush()
                    return

                synapse_id = croissant_json_ld_for_dataset["DATASET_SYNAPSE_ID"][0]
                name = croissant_json_ld_for_dataset["NAME"][0]

                span.set_attribute("airflow.dataset_name", name)
                span.set_attribute("airflow.croissant_result", True)

                if not push_to_s3:
                    otel_logger.info(
                        f"Croissant file for [dataset: {name}, id: {synapse_id}]:\n{croissant_json_ld_for_dataset['CROISSANT_METADATA'][0]}")
                else:
                    otel_logger.info(
                        f"Uploading croissant file for [dataset: {name}, id: {synapse_id}]")

                    croissant_metadata_bytes = croissant_json_ld_for_dataset["CROISSANT_METADATA"][0].encode(
                        'utf-8')
                    metadata_file = BytesIO(croissant_metadata_bytes)
                    s3_hook = S3Hook(
                        aws_conn_id=context["params"]["aws_conn_id"], region_name=REGION_NAME, extra_args={
                            "ContentType": "application/ld+json"
                        }
                    )

                    s3_hook.load_file_obj(file_obj=metadata_file,
                                          key=f"{name}_{synapse_id}_croissant.jsonld",
                                          bucket_name=BUCKET_NAME,
                                          replace=True,
                                          )

            except Exception as ex:
                otel_logger.exception(
                    "Failed to query snowflake and push croissant file to S3.")
                otel_tracer.span_processor.force_flush()
                otel_logger.handlers[0].flush()
                raise ex

        otel_tracer.span_processor.force_flush()
        otel_logger.handlers[0].flush()

    root_carrier_context = create_root_span()

    datasets = query_synapse_dataset_collection_for_datasets.partial(root_carrier_context=root_carrier_context).expand_kwargs(
        get_dataset_collections(root_carrier_context=root_carrier_context))

    query_snowflake_and_push_croissant_file.partial(root_carrier_context=root_carrier_context).expand_kwargs(
        combine_dataset_lists(root_carrier_context=root_carrier_context, dataset_ids=datasets))


dataset_to_croissant()
