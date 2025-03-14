"""
This DAG queries snowflake for the given dataset collections to look for datasets.

For each dataset in the dataset collections, it queries the dataset in Snowflake and 
pushes the results to a Croissant file in GitHub.

Simply add the dataset collections to the `dataset_collections` parameter in the DAG to
run this process for the given dataset collections.

DAG Parameters:
- `snowflake_conn_id`: The connection ID for the Snowflake connection.
- `synapse_conn_id`: The connection ID for the Synapse connection.
- `dataset_collections`: The dataset collections to query for datasets.
- `push_results_to_s3`: A boolean to indicate if the results should be pushed to S3.
                        When set to `False`, the results will be printed to the logs.
"""

from datetime import datetime
from io import BytesIO
from typing import List, Dict

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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

REGION_NAME = "us-east-1"
BUCKET_NAME = "synapse-croissant-metadata"


@dag(**dag_config)
def dataset_to_croissant() -> None:
    """Execute a query on Snowflake and report the results to a Synapse table."""

    @task
    def get_dataset_collections(**context) -> List[Dict[str, str]]:
        """
        Split the dataset collection ids into a format that is usable by the forking
        operation of the `expand_kwargs` function in Airflow.

        Returns:
            The dataset collections in a format that can be used by the
            `expand_kwargs` function.
        """
        dataset_collections = context["params"]["dataset_collections"]
        return [{"dataset_collection": dataset_collection} for dataset_collection in dataset_collections]

    @task
    def query_synapse_dataset_collection_for_datasets(
        dataset_collection: str, **context
    ) -> List[Dict[str, str]]:
        """
        Query the dataset_collection to get the IDs for the dataset we are going to
        be running this process for.

        Arguments:
            dataset_collection: The dataset collection to query for datasets.

        Returns:
            The list of dataset IDs for the given dataset collection.
        """

        dataset_collection_without_syn = dataset_collection.replace("syn", "")
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

        print(f"Performing query for dataset_collection {dataset_collection}")

        try:
            cs.execute(query)
            datasets = cs.fetch_pandas_all()
            cs.close()
            results = []
            for syn_id in datasets["DATASET_IDS"].tolist():
                results.append({"dataset_id": syn_id.replace("syn", "")})
            return results
        except Exception:
            print(
                f"Failed to query snowflake for datasets.")
            raise

    @task
    def combine_dataset_lists(dataset_ids: List[List[Dict[str, str]]]) -> List[Dict[str, str]]:
        """
        Combine the dataset IDs into a single list. This is used to force the
        tasks to join together before being expanded again.

        Arguments:
            dataset_ids: The list of dataset IDs to combine.

        Returns:
            The combined list of dataset IDs.
        """
        return_value = [
            dataset for datasets in dataset_ids for dataset in datasets]
        return return_value

    @task
    def query_snowflake_and_push_croissant_file(dataset_id: str, **context) -> None:
        """
        Query the Snowflake database for the dataset in order to perform the Croissant
        transformation and push the results to S3.

        Arguments:
            dataset_id: The ID of the dataset to query in Snowflake.
        """
        if not dataset_id:
            print("No dataset found")
            return

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
            WHERE id = {dataset_id} -- This is the ID of a Dataset
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
            WHERE nl.id IN (SELECT * FROM ids_of_files_belonging_to_dataset)
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

        print(f"Performing query for dataset {dataset_id}")

        try:
            cs.execute(query)
            croissant_json_ld_for_dataset = cs.fetch_pandas_all()
            cs.close()

            synapse_id = croissant_json_ld_for_dataset["DATASET_SYNAPSE_ID"][0]
            name = croissant_json_ld_for_dataset["NAME"][0]
            push_to_s3 = context["params"]["push_results_to_s3"]

            if not push_to_s3:
                print(
                    f"Croissant file for [dataset: {name}, id: {synapse_id}]:\n{croissant_json_ld_for_dataset['CROISSANT_METADATA'][0]}")
            else:
                print(
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
                                      )

        except Exception as e:
            print(
                "Failed to query snowflake and push croissant file to S3.")
            raise

    datasets = query_synapse_dataset_collection_for_datasets.expand_kwargs(
        get_dataset_collections())

    query_snowflake_and_push_croissant_file.expand_kwargs(
        combine_dataset_lists(datasets))


dataset_to_croissant()
