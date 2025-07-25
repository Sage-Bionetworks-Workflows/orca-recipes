"""ALS Knowledge Portal Dataset Collection DAG

This DAG automates the process of creating and maintaining a Synapse Dataset Collection
for the ALS Knowledge Portal. The DAG follows these steps:

1. Fetch data from the C-Path API using an authentication token stored in Airflow Variables
2. Transform the raw data using a JSONata mapping expression and validate against a JSON Schema
3. Find duplicates in the new C-Path data and send a message to AMP-ALS slack channel if duplicates are found. 
4. Data managers in AMP-ALS update the JSON file. The pipeline then reads this file to retrieve datasets that should be ignored following human validation.
5. Create or update Synapse Datasets for each item in the transformed data, excluding duplicates and datasets marked to be ignored after manual review.
6. Update the Dataset Collection to include all valid datasets, excluding duplicates and manually ignored datasets.
7. Create or update annotations for each dataset in the collection

The DAG runs monthly and uses Airflow Variables for configuration.
"""

from datetime import datetime
import requests
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import json
from jsonata import jsonata
from jsonschema import validate, ValidationError

from synapseclient.models import Dataset, DatasetCollection, File
from orca.services.synapse import SynapseHook

from airflow.decorators import task, dag
from airflow.models import Variable, Param
from slack_sdk import WebClient


dag_params = {
    "project_id": Param("syn64892175", type="string"),
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
    "ignore_cpath_datasets": Param("syn68737367", type="string"),
    "collection_id": Param("syn66496326", type="string"),
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

        Arguments:
            transformed_items (List[Dict[str, Any]]):
                A list of transformed and validated items from the previous task.
            **context: Airflow task context containing DAG parameters

        Raises:
            ValueError: If any 'sameAs' or 'title' values are missing or empty after transformation.

        Returns:
            List[Dict[str, Any]]: A list of duplicate dataset items if there are any.
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client

        # Get current datasets
        collection_id = context["params"]["collection_id"]
        query_str = (
            f"SELECT * FROM {collection_id} WHERE publisher='Critical Path Institute'"
        )

        current_data = synapse_client.tableQuery(query_str).asDataFrame()

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
                    # found duplicates, add both the first one if it does not already exist and this one
                    if seen[title] not in duplicates:
                        duplicates.append(seen[title])
                    duplicates.append(item)
        print("Found duplicated datasets:", duplicates)
        return duplicates

    @task
    def generate_slack_message(duplicates: List[Dict[str, Any]]) -> str:
        """
        Generate a Slack message summarizing duplicated datasets.

        This task formats a message that includes information about datasets flagged as duplicates,
        which will be posted to a Slack channel for review.

        Arguments:
            duplicates (List[Dict[str, Any]]): A list of datasets identified as duplicates.

        Returns:
            str: A formatted Slack message string describing the duplicate datasets.
        """
        if not duplicates:
            return ""
        message = "There are datasets that need to be reviewed: \n\n"
        for index, item in enumerate(duplicates):
            printed_message = f"{index+1}. C-path identifier: {item['sameAs']}, title: {item['title']}, creator: {item['creator']}, keywords: {item['keywords']}, subject: {item['subject']}, collection: {item['collection']}, publisher: {item['publisher']}, species: {item['species']}, url: {item['url']}, description: {item['description']} \n\n"
            message += printed_message
        return message

    @task
    def post_slack_messages(message: str) -> None:
        """
        Post a message to the designated Slack channel.

        This task sends a formatted message (e.g., about duplicated datasets)
        to a Slack channel using a pre-configured webhook or Slack API.

        Args:
            message (str): The message string to be posted.

        Returns:
            None
        """
        if not message:
            return
        client = WebClient(token=Variable.get("SLACK_DPE_TEAM_BOT_TOKEN"))
        client.chat_postMessage(channel="amp-als", text=message)

    @task
    def find_ignored_datasets(**context) -> List[str]:
        """Datasets that need to be ignored after human review and validation

        This task:
        1. Retrieve json file: ignore_cpath_datasets.json
        2. Read the file and get a list of C-path datasets that need to be ignored based on the C-Path identifier.

        Arguments:
            **context: Airflow task context containing DAG parameters
        Returns:
            List[str]: A list of C-Path identifiers to be ignored
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client

        # Find datasets that need to be ignored
        ignore_cpath_datasets_json = context["params"]["ignore_cpath_datasets"]
        file = File(id=ignore_cpath_datasets_json, download_file=True).get()
        with open(file.path, "r") as f:
            contents = f.read()
            content_json = json.loads(contents)
            datasets_to_ignore = content_json.get("ignore_cpath_identifier", [])
        print(
            "dataset identifiers to be ignored after human review: "
            + str(datasets_to_ignore)
        )
        return datasets_to_ignore

    @task
    def create_datasets(
        transformed_items: List[Dict[str, Any]],
        duplicates: List[Dict[str, Any]],
        ignored_datasets: List[str],
        **context,
    ) -> str:
        """Create Synapse datasets and add them to a dataset collection.

        This task:
        1. Retrieves the existing dataset collection.
        2. Iterates over each transformed item, and if a dataset is not flagged as a duplicate:
            - Creates a dataset in Synapse using the item's title and description.
            - Adds annotation "source" = "Critical Path Institute" to the dataset
            - Adds the dataset to the collection.
            - Updates the dataset collection

        Arguments:
            transformed_items (List[Dict[str, Any]]):
                The list of transformed and validated dataset items to process.
            duplicates (List[Dict[str, Any]]):
                The list of items identified as duplicates, used to skip adding them.
            ignored_datasets: A list of C-Path identifiers that need to be ignored after reviewed by a human
            **context: Airflow task context containing DAG parameters
        Returns:
            str: The ID of the updated dataset collection.
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client

        dataset_collection = DatasetCollection(
            id=context["params"]["collection_id"]
        ).get()

        for item in transformed_items:
            if item in duplicates or item["sameAs"] in ignored_datasets:
                continue
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
            dataset.store()
            dataset_collection.add_item(dataset)
        dataset_collection.store()
        return dataset_collection.id

    @task
    def update_annotations(
        duplicates: List[Dict[str, Any]],
        dataset_collection_id: str,
        transformed_items: List[Dict[str, Any]],
        ignored_datasets: List[str],
        **context,
    ) -> None:
        """Update dataset annotations if the dataset is not flagged as a duplicate

        This task:
        1. Queries the current state of the dataset collection to get all the C-Path data
        2. Prepares new annotation data from the transformed items
        3. Updates the collection with the new annotations

        Arguments:
            duplicates: The list of items identified as duplicates, used to skip adding them.
            dataset_collection_id: The ID of the dataset collection to update
            transformed_items: List of transformed items containing new annotations
            ignored_datasets: A list of C-Path identifiers that need to be ignored after reviewed by a human
            **context: Airflow task context containing DAG parameters
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client
        dataset_collection = DatasetCollection(id=dataset_collection_id).get()

        current_data = dataset_collection.query(
            query=f"SELECT * from {dataset_collection.id} where source='Critical Path Institute'"
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
            if item in duplicates or item["sameAs"] in ignored_datasets:
                continue
            for key in fields:
                value = item[key]
                if isinstance(value, list):
                    value = ", ".join(value)
                annotations[key].append(value)

        if len(dataset_ids) != len(annotations["title"]):
            raise ValueError(
                f"There are {len(dataset_ids)} stored in dataset collection, but there are {len(annotations['title'])} datasets to update."
            )

        annotation_data = pd.DataFrame({"id": dataset_ids, **annotations})

        # Update the rows
        dataset_collection.update_rows(
            values=annotation_data,
            primary_keys=["id"],
            dry_run=False,
            wait_for_eventually_consistent_view=True,
        )

    # Define task dependencies
    data = fetch_cpath_data()
    transformed_items = transform_data(data)
    duplicates = find_duplicated_datasets(transformed_items)
    message = generate_slack_message(duplicates)
    ignored_datasets = find_ignored_datasets()
    collection_id = create_datasets(transformed_items, duplicates, ignored_datasets)
    updated = update_annotations(
        duplicates, collection_id, transformed_items, ignored_datasets
    )

    transformed_items >> duplicates >> message >> post_slack_messages(message)
    duplicates >> ignored_datasets >> collection_id >> updated


als_kp_dataset_dag()
