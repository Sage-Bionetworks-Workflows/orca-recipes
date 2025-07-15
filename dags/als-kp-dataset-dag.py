"""ALS Knowledge Portal Dataset Collection DAG

This DAG automates the process of creating and maintaining a Synapse Dataset Collection
for the ALS Knowledge Portal. The DAG follows these steps:

1. Fetch data from the C-Path API using an authentication token stored in Airflow Variables
2. Transform the raw data using a JSONata mapping expression and validate against a JSON Schema
3. Find duplicates in the new C-Path data and send a message to AMP-ALS slack channel if duplicates are found. 
4. Create or update Synapse Datasets for each item in the transformed data
5. Create or update a Dataset Collection containing all the datasets
6. Create or update annotations for each dataset in the collection

The DAG runs monthly and uses Airflow Variables for configuration.
"""

from datetime import datetime
import requests
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
from jsonata import jsonata
from jsonschema import validate, ValidationError

from synapseclient.models import Dataset, DatasetCollection, Column, ColumnType
from orca.services.synapse import SynapseHook

from airflow.decorators import task, dag
from airflow.models import Variable, Param
from slack_sdk import WebClient
import time


dag_params = {
    "project_id": Param("syn68155335", type="string"),
    "mapping_url": Param(
        "https://raw.githubusercontent.com/amp-als/data-model/410a429540a81a63846921ee77d6cf8e33ab6407/mapping/cpath.jsonata",
        type="string",
    ),
    "schema_url": Param(
        "https://raw.githubusercontent.com/amp-als/data-model/410a429540a81a63846921ee77d6cf8e33ab6407/json-schemas/Dataset.json",
        type="string",
    ),
    "cpath_api_url": Param(
        "https://fair.dap.c-path.org/api/collections/als-kp/datasets", type="string"
    ),
    "collection_name": Param("test-collection", type="string"),
    "collection_description": Param("test dataset collection", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
}

dag_config = {
    "schedule_interval": "0 0 1 * *",  # Run on the first day of the month at midnight
    "start_date": datetime(2025, 5, 1),
    "catchup": False,
    "default_args": {
        "retries": 2,
    },
    "tags": ["als-kp"],
    "params": dag_params,
}


def load_mapping_from_url(url: str) -> str:
    """Load the JSONata mapping expression from a URL.

    Arguments:
        url (str): The URL to fetch the JSONata mapping expression from.

    Returns:
        str: The JSONata mapping expression as a string.

    Raises:
        requests.exceptions.RequestException: If the request fails or returns a non-200 status code.
        requests.exceptions.Timeout: If the request times out.
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def load_schema_from_url(url: str) -> Dict[str, Any]:
    """Load the JSON Schema from a URL.

    Arguments:
        url (str): The URL to fetch the JSON Schema from.

    Returns:
        Dict[str, Any]: The parsed JSON Schema as a dictionary.

    Raises:
        requests.exceptions.RequestException: If the request fails or returns a non-200 status code.
        requests.exceptions.Timeout: If the request times out.
        json.JSONDecodeError: If the response is not valid JSON.
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def validate_item(
    item: Dict[str, Any], schema: Dict[str, Any]
) -> Tuple[bool, Optional[str]]:
    """Validate an item against a JSON Schema.

    Arguments:
        item (Dict[str, Any]): The item to validate.
        schema (Dict[str, Any]): The JSON Schema to validate against.

    Returns:
        Tuple[bool, Optional[str]]: A tuple containing:
            - bool: True if the item is valid, False otherwise.
            - Optional[str]: Error message if validation fails, None if validation succeeds.
    """
    try:
        validate(instance=item, schema=schema)
        return True, None
    except ValidationError as e:
        return False, str(e)


def transform_with_jsonata(
    source_items: List[Dict[str, Any]],
    mapping_expr: str,
    schema: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Transform a list of items using a JSONata expression and validate against schema.

    Arguments:
        source_items (List[Dict[str, Any]]): List of source items to transform.
        mapping_expr (str): The JSONata mapping expression to apply.
        schema (Optional[Dict[str, Any]], optional): JSON Schema to validate transformed items against.
            If None, no validation is performed. Defaults to None.

    Returns:
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: A tuple containing:
            - List[Dict[str, Any]]: List of successfully transformed and validated items.
            - List[Dict[str, Any]]: List of validation errors, each containing:
                - item_index (int): Index of the item that failed validation
                - error (str): Validation error message
                - transformed_item (Dict[str, Any]): The transformed item that failed validation

    Raises:
        jsonata.JsonataError: If the JSONata expression is invalid.
    """
    expr = jsonata.Jsonata(mapping_expr)
    transformed_items: List[Dict[str, Any]] = []
    validation_errors: List[Dict[str, Any]] = []

    for i, item in enumerate(source_items):
        result = expr.evaluate(item)
        if schema:
            is_valid, error = validate_item(result, schema)
            if not is_valid:
                validation_errors.append(
                    {"item_index": i, "error": error, "transformed_item": result}
                )
                continue
        transformed_items.append(result)

    return transformed_items, validation_errors


@dag(**dag_config)
def als_kp_dataset_dag():
    @task
    def fetch_cpath_data(**context) -> Dict[str, Any]:
        """Fetch data from C-Path API using auth token from Airflow Variables.

        Arguments:
            **context: Airflow task context containing DAG parameters

        Returns:
            Dict[str, Any]: JSON response from the C-Path API containing dataset items

        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {Variable.get('CPATH_API_TOKEN')}",
        }

        response = requests.get(context["params"]["cpath_api_url"], headers=headers)
        response.raise_for_status()
        return response.json()

    @task
    def transform_data(data: Dict[str, Any], **context) -> List[Dict[str, Any]]:
        """Transform the data using JSONata mapping and validate against schema.

        This task:
        1. Loads the JSONata mapping expression from the specified URL
        2. Loads the JSON Schema from the specified URL
        3. Applies the mapping to each item in the input data
        4. Validates each transformed item against the schema
        5. Returns only the valid transformed items

        Arguments:
            data: Raw data from the C-Path API
            **context: Airflow task context containing DAG parameters

        Returns:
            List[Dict[str, Any]]: List of transformed and validated items

        Raises:
            ValueError: If any validation errors are found
            requests.exceptions.RequestException: If loading mapping or schema fails
        """
        mapping_expr = load_mapping_from_url(context["params"]["mapping_url"])
        schema = load_schema_from_url(context["params"]["schema_url"])

        transformed_items, validation_errors = transform_with_jsonata(
            data["items"], mapping_expr, schema
        )

        if validation_errors:
            raise ValueError(f"Found {len(validation_errors)} validation errors.")

        return transformed_items

    @task
    def find_duplicated_datasets(
        transformed_items: List[Dict[str, Any]], **context
    ) -> List[Dict[str, Any]]:
        """Flag duplicated new C-PATH datasets.

        This task:
        1. Retrieves the current C-PATH datasets.
        2. Identifies and flags new datasets that are duplicates based on their 'title'.

        Args:
            transformed_items (List[Dict[str, Any]]):
                A list of transformed and validated items from the previous task.

        Raises:
            ValueError: If any 'sameAs' or 'title' values are missing or empty after transformation.

        Returns:
            List[Dict[str, Any]]: A list of duplicate dataset items if there are any.
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client

        # Get current datasets
        dataset_collection = DatasetCollection(
            name=context["params"]["collection_name"],
            parent_id=context["params"]["project_id"],
        ).get()
        current_data = dataset_collection.query(
            query=f"SELECT * from {dataset_collection.id} where publisher='Critical Path Institute'",
            synapse_client=synapse_client,
        )
        current_datasets = list(current_data["sameAs"])

        # Flag duplicates from new data
        # Datasets are considered as "duplicates" if they have the same titles
        duplicates = []
        seen = {}

        for item in transformed_items:
            cpath_key = item.get("sameAs")
            title = item.get("title")

            if not cpath_key:
                raise ValueError(f"Missing or empty 'sameAs' in item: {item}")
            if not title:
                raise ValueError(f"Missing or empty 'title' in item: {item}")

            if cpath_key not in current_datasets:
                if title not in seen:
                    seen[title] = item
                else:
                    # found duplicates, add both the first one and this one
                    duplicates.append(item)
                    duplicates.append(seen[title])
        print("Found duplicated datasets:", duplicates)
        return duplicates

    @task
    def generate_slack_message(duplicates: List[Dict[str, Any]], **context) -> str:
        """Generate the message to be posted to the slack channel."""
        if not duplicates:
            return ""
        message = "There are datasets that need to be reviewed: \n\n"
        for index, item in enumerate(duplicates):
            printed_message = f"{index+1}. C-path identifier: {item['sameAs']}, title: {item['title']}, creator: {item['creator']}, keywords: {item['keywords']}, subject: {item['subject']}, collection: {item['collection']}, publisher: {item['publisher']}, species: {item['species']} \n\n"
            message += printed_message
        return message

    @task
    def post_slack_messages(message: str) -> bool:
        """Post the duplicated datasets to the slack channel."""
        if not message:
            return
        client = WebClient(token=Variable.get("SLACK_DPE_TEAM_BOT_TOKEN"))
        client.chat_postMessage(channel="test-amp-als-dataset-message", text=message)

    @task
    def create_datasets(
        transformed_items: List[Dict[str, Any]],
        duplicates: List[Dict[str, Any]],
        **context,
    ) -> str:
        """Create Synapse datasets and add them to a dataset collection.

        This task:
        1. Retrieves the existing dataset collection.
        2. Iterates over each transformed item:
        - Creates a new Dataset in Synapse using the item's title and description.
        - Adds the Dataset to the collection if it is not flagged as a duplicate.

        Arguments:
            transformed_items (List[Dict[str, Any]]):
                The list of transformed and validated dataset items to process.
            duplicates (List[Dict[str, Any]]):
                The list of items identified as duplicates, used to skip adding them.

        Returns:
            str: The ID of the updated dataset collection.
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client

        dataset_collection = DatasetCollection(
            name=context["params"]["collection_name"],
            parent_id=context["params"]["project_id"],
        ).get()

        for item in transformed_items:
            if item not in duplicates:
                dataset_description = (
                    item["description"][:1000]
                    if len(item["description"]) > 1000
                    else item["description"]
                )
                dataset = Dataset(
                    name=item["title"],
                    description=dataset_description,
                    parent_id=context["params"]["project_id"],
                )
                dataset.annotations = {"source": "Critical Path Institute"}
                dataset.store(synapse_client=synapse_client)
                time.sleep(2)
                dataset_collection.add_item(dataset)
        return dataset_collection.id

    @task
    def update_annotations(
        duplicates: List[Dict[str, Any]],
        dataset_collection_id: str,
        transformed_items: List[Dict[str, Any]],
        **context,
    ) -> None:
        """Update dataset annotations

        This task:
        1. Queries the current state of the dataset collection
        2. Prepares new annotation data from the transformed items
        3. Updates the collection with the new annotations

        Arguments:
            duplicates: The list of items identified as duplicates, used to skip adding them.
            dataset_collection_id: The ID of the dataset collection to update
            transformed_items: List of transformed items containing new annotations
            **context: Airflow task context containing DAG parameters
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client

        dataset_collection = DatasetCollection(id=dataset_collection_id).get(
            synapse_client=synapse_client
        )

        current_data = dataset_collection.query(
            query=f"SELECT * from {dataset_collection.id} where source='Critical Path Institute'",
            synapse_client=synapse_client,
        )
        dataset_ids = list(current_data["id"])
        fields = [
            "title",
            "creator",
            "keywords",
            "subject",
            "collection",
            "publisher",
            "species",
            "sameAs",
            "source",
            "url",
        ]
        annotations = {key: [] for key in fields}

        for item in transformed_items:
            if item not in duplicates:
                for key in fields:
                    value = item[key]
                    if isinstance(value, list):
                        value = ", ".join(value)
                    annotations[key].append(value)

        if len(dataset_ids) != len(annotations["title"]):
            raise ValueError(
                f" There are {len(dataset_ids)} stored, but there are {len(annotations[title])} datasets to update"
            )

        annotation_data = pd.DataFrame({"id": dataset_ids, **annotations})

        # Update the rows
        dataset_collection.update_rows(
            values=annotation_data,
            primary_keys=["id"],
            dry_run=False,
            wait_for_eventually_consistent_view=True,
            synapse_client=synapse_client,
        )

    # Define task dependencies
    data = fetch_cpath_data()
    transformed_items = transform_data(data)
    duplicates = find_duplicated_datasets(transformed_items)
    message = generate_slack_message(duplicates)

    collection_id = create_datasets(transformed_items, duplicates)
    updated = update_annotations(duplicates, collection_id, transformed_items)

    transformed_items >> duplicates >> message >> post_slack_messages(message)
    duplicates >> collection_id >> updated


als_kp_dataset_dag()
