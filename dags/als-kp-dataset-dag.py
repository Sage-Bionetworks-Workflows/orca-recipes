"""ALS Knowledge Portal Dataset Collection DAG

This DAG automates the process of creating and maintaining a Synapse Dataset Collection
for the ALS Knowledge Portal. The DAG follows these steps:

1. Fetch data from the C-Path API using an authentication token stored in Airflow Variables
2. Transform the raw data using a JSONata mapping expression and validate against a JSON Schema
3. Create or update Synapse Datasets for each item in the transformed data
4. Create or update a Dataset Collection containing all the datasets
5. Create or update annotations for each dataset in the collection
6. Create a new snapshot of the collection if any changes were detected

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


dag_params = {
    "project_id": Param("syn64892175", type="string"),
    "mapping_url": Param(
        "https://raw.githubusercontent.com/amp-als/data-model/dd0e476c3659c9b98977d567d0ddce01d5057639/mapping/cpath.jsonata",
        type="string",
    ),
    "schema_url": Param(
        "https://raw.githubusercontent.com/amp-als/data-model/dd0e476c3659c9b98977d567d0ddce01d5057639/json-schemas/Dataset.json",
        type="string",
    ),
    "cpath_api_url": Param(
        "https://fair.dap.c-path.org/api/collections/als-kp/datasets", type="string"
    ),
    "collection_name": Param("Dataset collection (Production)", type="string"),
    "collection_description": Param(
        "A collection of datasets curated for the ALS Knowledge Portal", type="string"
    ),
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
    def create_datasets(transformed_items: List[Dict[str, Any]], **context) -> str:
        """Create Synapse datasets and collection.

        This task:
        1. Creates a new Dataset Collection with predefined columns
        2. For each transformed item:
           - Creates a new Dataset with the item's title and description
           - Adds the Dataset to the collection
        3. Stores the collection in Synapse

        Arguments:
            transformed_items: List of transformed and validated items
            **context: Airflow task context containing DAG parameters

        Returns:
            str: The Synapse ID of the created and stored dataset collection
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client

        columns = [
            Column(name="id", column_type=ColumnType.ENTITYID),
            Column(name="title", column_type=ColumnType.STRING, maximum_size=200),
            Column(name="creator", column_type=ColumnType.STRING, maximum_size=100),
            Column(name="keywords", column_type=ColumnType.STRING, maximum_size=250),
            Column(name="subject", column_type=ColumnType.STRING, maximum_size=100),
            Column(name="collection", column_type=ColumnType.STRING, maximum_size=100),
            Column(name="publisher", column_type=ColumnType.STRING, maximum_size=100),
            Column(name="species", column_type=ColumnType.STRING, maximum_size=100),
            Column(name="sameAs", column_type=ColumnType.STRING, maximum_size=100),
        ]

        dataset_collection = DatasetCollection(
            name=context["params"]["collection_name"],
            description=context["params"]["collection_description"],
            parent_id=context["params"]["project_id"],
            include_default_columns=True,
            columns=columns,
        )

        for item in transformed_items:
            dataset_description = (
                item["description"][:1000]
                if len(item["description"]) > 1000
                else item["description"]
            )
            dataset = Dataset(
                name=item["title"],
                description=dataset_description,
                parent_id=context["params"]["project_id"],
            ).store(synapse_client=synapse_client)
            dataset_collection.add_item(dataset)

        dataset_collection = dataset_collection.store(synapse_client=synapse_client)
        return dataset_collection.id

    @task
    def update_annotations(
        dataset_collection_id: str,
        transformed_items: List[Dict[str, Any]],
        **context,
    ) -> None:
        """Update dataset annotations and create snapshot if changes are detected.

        This task:
        1. Queries the current state of the dataset collection
        2. Prepares new annotation data from the transformed items
        3. Updates the collection with the new annotations
        4. Queries the updated state
        5. Compares the before and after states
        6. If changes are detected:
           - Creates a summary of which columns were modified
           - Creates a new snapshot with a descriptive comment
           - The comment includes timestamp, changed columns, and dataset count

        Arguments:
            dataset_collection_id: The ID of the dataset collection to update
            transformed_items: List of transformed items containing new annotations
            **context: Airflow task context containing DAG parameters
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client

        dataset_collection = DatasetCollection(id=dataset_collection_id).get(
            synapse_client=synapse_client
        )

        dataset_ids = [item.id for item in dataset_collection.items]

        # Get current data before update
        current_data = dataset_collection.query(
            query=f"SELECT * from {dataset_collection.id}",
            synapse_client=synapse_client,
        )
        # Prepare and apply new data
        annotation_data = pd.DataFrame(
            {
                "id": dataset_ids,
                **{
                    key: [
                        (
                            ", ".join(item[key])
                            if isinstance(item[key], list)
                            else item[key]
                        )
                        for item in transformed_items
                    ]
                    for key in [
                        "title",
                        "creator",
                        "keywords",
                        "subject",
                        "collection",
                        "publisher",
                        "species",
                        "sameAs",
                    ]
                },
            }
        )

        # Update the rows
        dataset_collection.update_rows(
            values=annotation_data,
            primary_keys=["id"],
            dry_run=False,
            wait_for_eventually_consistent_view=True,
            synapse_client=synapse_client,
        )

        # Get data after update
        updated_data = dataset_collection.query(
            query=f"SELECT * from {dataset_collection.id}",
            synapse_client=synapse_client,
        )

        # Compare data before and after update
        if not current_data.equals(updated_data):
            # Generate change summary
            changed_columns = []
            for col in current_data.columns:
                if not current_data[col].equals(updated_data[col]):
                    changed_columns.append(col)

            # Create snapshot comment with timestamp and change summary
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            snapshot_comment = (
                f"Snapshot created at {timestamp}. "
                f"Updated columns: {', '.join(changed_columns)}. "
                f"Total datasets: {len(dataset_ids)}"
            )

            print("Changes detected in dataset collection, creating new snapshot...")
            print(f"Snapshot comment: {snapshot_comment}")
            dataset_collection.snapshot(
                comment=snapshot_comment, synapse_client=synapse_client
            )
        else:
            print("No changes detected in dataset collection, skipping snapshot.")

    # Define task dependencies
    data = fetch_cpath_data()
    transformed_items = transform_data(data)
    dataset_collection_id = create_datasets(transformed_items)
    update_annotations(dataset_collection_id, transformed_items)


als_kp_dataset_dag()
