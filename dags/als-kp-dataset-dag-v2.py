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
        "https://raw.githubusercontent.com/amp-als/data-model/refs/heads/main/mapping/cpath.jsonata",
        type="string",
    ),
    "schema_url": Param(
        "https://raw.githubusercontent.com/amp-als/data-model/refs/heads/main/json-schemas/Dataset.json",
        type="string",
    ),
    "cpath_api_url": Param(
        "https://fair.dap.c-path.org/api/collections/als-kp/datasets", type="string"
    ),
    "ignore_cpath_datasets": Param("syn68737367", type="string"),
    "collection_id": Param("syn69962707", type="string"),
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



def parse_dataset_code(dataset_code: str) -> Tuple[str, str, str]:
    """Parse dataset code to extract ALS number, prefix, and date.

    Args:
        dataset_code: e.g., "src_als1003_2025_04_17", "fv1_als1001_2025_02_26"

    Returns:
        Tuple of (als_number, prefix, date_str)

    Raises:
        ValueError: If dataset code format is invalid
    """
    import re

    # Pattern to match: prefix_als{number}_{date}
    pattern = r'^(src_|fm[12]_|fv[123]_)(als\d+)_(\d{4}_\d{2}_\d{2})$'
    match = re.match(pattern, dataset_code)

    if not match:
        raise ValueError(f"Invalid dataset code format: {dataset_code}")

    prefix = match.group(1).rstrip('_')  # Remove trailing underscore
    als_number = match.group(2)
    date_str = match.group(3)

    return als_number, prefix, date_str


def extract_dataset_code_from_url(url: str) -> str:
    """Extract dataset code from C-Path URL.

    Args:
        url: e.g., "https://fair.dap.c-path.org/#/data/datasets/src_als1003_2025_04_17"

    Returns:
        Dataset code e.g., "src_als1003_2025_04_17"
    """
    if not url:
        return ""
    return url.split("/")[-1]


def get_version_priority(prefix: str) -> int:
    """Get priority score for dataset prefix (higher = better).

    Priority: fv3 > fv2 > fv1 > fm2 > fm1 > src
    """
    priority_map = {
        'fv3': 6,
        'fv2': 5,
        'fv1': 4,
        'fm2': 3,
        'fm1': 2,
        'src': 1
    }
    return priority_map.get(prefix, 0)


def select_latest_versions(datasets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Select the latest and greatest version for each ALS dataset group.

    Args:
        datasets: List of dataset items with 'url' field containing dataset codes

    Returns:
        List of selected datasets (one per ALS number)
    """
    from collections import defaultdict
    from datetime import datetime

    # Group datasets by ALS number
    als_groups = defaultdict(list)

    for dataset in datasets:
        url = dataset.get("url", "")
        dataset_code = extract_dataset_code_from_url(url)

        if not dataset_code:
            print(f"Skipping dataset with missing URL: {dataset.get('title', 'Unknown')}")
            continue

        try:
            als_number, prefix, date_str = parse_dataset_code(dataset_code)
            als_groups[als_number].append({
                'dataset': dataset,
                'prefix': prefix,
                'date_str': date_str,
                'priority': get_version_priority(prefix),
                'dataset_code': dataset_code
            })
        except ValueError as e:
            print(f"Skipping dataset with invalid code: {dataset_code}, error: {e}")
            continue

    selected_datasets = []

    # For each ALS group, select the best version
    for als_number, versions in als_groups.items():
        # Sort by priority (desc), then by date (desc)
        best_version = max(versions, key=lambda x: (
            x['priority'],
            datetime.strptime(x['date_str'], '%Y_%m_%d')
        ))

        selected_datasets.append(best_version['dataset'])
        print(f"Selected {best_version['dataset_code']} for {als_number} "
              f"(priority: {best_version['priority']}, date: {best_version['date_str']})")

    return selected_datasets

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
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Apply latest and greatest version selection for ALS datasets.

        This task:
        1. Retrieves the current C-PATH datasets from Synapse.
        2. Filters out datasets that already exist in the collection.
        3. Groups remaining datasets by ALS number and selects the latest version.

        Arguments:
            transformed_items (List[Dict[str, Any]]):
                A list of transformed and validated items from the previous task.
            **context: Airflow task context containing DAG parameters

        Returns:
            Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
                - Selected datasets to ingest
                - Empty list (no duplicates for manual review)
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        synapse_client = syn_hook.client

        # Get current datasets (using sameAs field which has cpath:ID format)
        collection_id = context["params"]["collection_id"]
        query_str = (
            f"SELECT * FROM {collection_id} WHERE source='Critical Path Institute'"
        )

        current_data = synapse_client.tableQuery(query_str).asDataFrame()
        current_datasets = set(current_data["sameAs"].tolist())

        # Filter out items that already exist (check by sameAs field)
        new_items = []
        for item in transformed_items:
            cpath_key = item.get("sameAs")  # This is "cpath:1725" format
            title = item.get("title")
            url = item.get("url")

            if not cpath_key:
                raise ValueError(f"Missing or empty 'sameAs' in item: {item}")
            if not title:
                raise ValueError(f"Missing or empty 'title' in item: {item}")
            if not url:
                raise ValueError(f"Missing or empty 'url' in item: {item}")

            if cpath_key not in current_datasets:
                new_items.append(item)

        print(f"Found {len(new_items)} new items to process from {len(transformed_items)} total items")

        # Apply latest and greatest selection using URL field for ALS codes
        selected_items = select_latest_versions(new_items)

        print(f"Selected {len(selected_items)} datasets from {len(new_items)} new items")

        # Return selected items and empty duplicates list (no manual review needed)
        return selected_items, []
    
    @task
    def get_existing_als_datasets(syn_hook, collection_id: str) -> Dict[str, Dict[str, Any]]:
        """Get all existing ALS datasets from the collection, grouped by ALS number.
        Returns:
            Dict mapping als_number -> dataset_info with fields:
            - synapse_id, dataset_code, prefix, priority, date_str, sameAs
        """
        synapse_client = syn_hook.client
        query_str = f"SELECT * FROM {collection_id} WHERE publisher='Critical Path Institute'"
        current_data = synapse_client.tableQuery(query_str).asDataFrame()

        existing_datasets = {}

        for _, row in current_data.iterrows():
            url = row.get('url', '')
            dataset_code = extract_dataset_code_from_url(url)

            if not dataset_code:
                continue

            try:
                als_number, prefix, date_str = parse_dataset_code(dataset_code)
                existing_datasets[als_number] = {
                    'synapse_id': row['id'],
                    'dataset_code': dataset_code,
                    'prefix': prefix,
                    'priority': get_version_priority(prefix),
                    'date_str': date_str,
                    'sameAs': row['sameAs'],
                    'url': row['url']
                }
            except ValueError:
                continue

        return existing_datasets

    @task
    def identify_dataset_actions(
        selected_items: List[Dict[str, Any]], **context
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Identify which datasets need new versions vs new creation."""

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        collection_id = context["params"]["collection_id"]

        # Get existing datasets
        existing_datasets = get_existing_als_datasets(syn_hook, collection_id)

        datasets_to_create = []
        datasets_to_update = []

        for item in selected_items:
            url = item.get("url", "")
            dataset_code = extract_dataset_code_from_url(url)

            if not dataset_code:
                continue

            try:
                als_number, new_prefix, new_date_str = parse_dataset_code(dataset_code)
                new_priority = get_version_priority(new_prefix)
                new_date = datetime.strptime(new_date_str, '%Y_%m_%d')

                if als_number in existing_datasets:
                    existing = existing_datasets[als_number]
                    existing_date = datetime.strptime(existing['date_str'], '%Y_%m_%d')

                    # Check if we should update (version upgrade OR same version with potential annotation changes)
                    should_update = (
                        new_priority > existing['priority'] or
                        (new_priority == existing['priority'] and new_date >= existing_date)
                    )

                    if should_update:
                        upgrade_type = 'version' if new_priority > existing['priority'] else 'annotation'
                        datasets_to_update.append({
                            'new_data': item,
                            'existing_synapse_id': existing['synapse_id'],
                            'upgrade_type': upgrade_type,
                            'dataset_code': dataset_code
                        })
                        print(f"Will update {existing['dataset_code']} -> {dataset_code} ({upgrade_type})")
                    else:
                        print(f"Skipping {dataset_code} (no improvement over {existing['dataset_code']})")
                else:
                    # Completely new ALS number
                    datasets_to_create.append(item)
                    print(f"Will create new dataset: {dataset_code}")

            except ValueError as e:
                print(f"Skipping invalid dataset code {dataset_code}: {e}")
                continue

        return datasets_to_create, datasets_to_update

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
    def update_existing_datasets(
        datasets_to_update: List[Dict[str, Any]],
        **context
    ) -> List[str]:
        """Create new versions of existing datasets with updated annotations."""

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        updated_dataset_ids = []

        for update_info in datasets_to_update:
            item = update_info['new_data']
            existing_id = update_info['existing_synapse_id']
            upgrade_type = update_info['upgrade_type']

            # Get existing dataset
            existing_dataset = Dataset(id=existing_id).get()

            # Update with new information
            existing_dataset.name = item["title"]
            existing_dataset.description = (
                item["description"][:1000]
                if len(item["description"]) > 1000
                else item["description"]
            )

            # Update annotations with new data
            existing_dataset.annotations.update({
                "source": "Critical Path Institute",
                "creator": ", ".join(item["creator"]) if isinstance(item["creator"], list) else item["creator"],
                "keywords": ", ".join(item["keywords"]) if isinstance(item["keywords"], list) else item["keywords"],
                "subject": ", ".join(item["subject"]) if isinstance(item["subject"], list) else item["subject"],
                "collection": ", ".join(item["collection"]) if isinstance(item["collection"], list) else item["collection"],
                "publisher": item["publisher"],
                "species": ", ".join(item["species"]) if isinstance(item["species"], list) else item["species"],
                "sameAs": item["sameAs"],
                "url": item["url"],
                "contributor": item["contributor"] if isinstance(item["contributor"], list) else [item["collection"]]
            })

            # Store as new version
            existing_dataset.store()
            updated_dataset_ids.append(existing_dataset.id)

            print(f"{upgrade_type.title()} update for {item['title']}: new version {existing_dataset.version_number}")

        return updated_dataset_ids
    
    @task
    def create_new_datasets(
            datasets_to_create: List[Dict[str, Any]],
            ignored_datasets: List[str],
            **context,
        ) -> str:
            """Create brand new datasets for ALS numbers that don't exist yet."""

            syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
            dataset_collection = DatasetCollection(id=context["params"]["collection_id"]).get()

            created_dataset_ids = []

            for item in datasets_to_create:
                # Skip if in ignored list
                if item["sameAs"] in ignored_datasets:
                    continue

                dataset_description = (
                    item["description"][:1000]
                    if len(item["description"]) > 1000
                    else item["description"]
                )
                # Set initial annotations
                dataset_annotations = {
                    "source": ["Critical Path Institute"],
                    "creator": item["creator"] if isinstance(item["creator"], list) else [item["creator"]],
                    "keywords": item["keywords"] if isinstance(item["keywords"], list) else [item["keywords"]],
                    "subject": item["subject"] if isinstance(item["subject"], list) else [item["subject"]],
                    "collection": item["collection"] if isinstance(item["collection"], list) else [item["collection"]],
                    "publisher": [item["publisher"]],
                    "species": item["species"] if isinstance(item["species"], list) else [item["species"]],
                    "sameAs": [item["sameAs"]],
                    "url": [item["url"]],
                    "title": item["title"],
                    "contributor": item["contributor"] if isinstance(item["contributor"], list) else [item["collection"]]
                }
                # Create new dataset
                dataset = Dataset(
                    parent_id=context["params"]["project_id"],
                    name=item["title"],
                    annotations=dataset_annotations,
                    description=dataset_description).store()
                dataset_collection.add_item(dataset)
                created_dataset_ids.append(dataset.id)
                #Storing annotations using synapseclient rather than models method cause it would not store with models method for some reason. 
                #dataset_id=dataset.id
                #dataset = syn.get(dataset_id, downloadFile=False)
                #dataset.annotations=dataset_annotations
                
                # After store():
                #syn.store(dataset, forceVersion=False)
                #print(dataset.annotations)



                print(f"Created new dataset: {item['title']} (ID: {dataset.id})")

            dataset_collection.store()
            return dataset_collection.id
    @task
    def refresh_collection_annotations(
        collection_id: str,
        updated_dataset_ids: List[str],
        **context,
    ) -> None:
        """Refresh collection table annotations for updated datasets.

        This ensures the collection table reflects the latest annotations
        from newly versioned datasets.
        """

        if not updated_dataset_ids:
            print("No updated datasets to refresh annotations for")
            return

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        dataset_collection = DatasetCollection(id=collection_id).get()

        # Get current collection data
        current_data = dataset_collection.query(
            query=f"SELECT * from {collection_id} where source='Critical Path Institute'"
        )

        # Filter to only the datasets that were updated
        rows_to_update = current_data[current_data['id'].isin(updated_dataset_ids)]

        if rows_to_update.empty:
            print("No matching rows found in collection for updated datasets")
            return

        # For each updated dataset, get its latest annotations
        updated_rows = []

        for _, row in rows_to_update.iterrows():
            dataset_id = row['id']

            # Get the latest version of the dataset
            dataset = Dataset(id=dataset_id).get()

            # Prepare updated row data
            updated_row = {
                "id": dataset_id,
                "title": dataset.name,
                "creator": dataset.annotations.get("creator", ""),
                "keywords": dataset.annotations.get("keywords", ""),
                "subject": dataset.annotations.get("subject", ""),
                "collection": dataset.annotations.get("collection", ""),
                "publisher": dataset.annotations.get("publisher", ""),
                "species": dataset.annotations.get("species", ""),
                "sameAs": dataset.annotations.get("sameAs", ""),
                "source": dataset.annotations.get("source", "Critical Path Institute"),
                "url": dataset.annotations.get("url", ""),
                "contributor": dataset.annotations.get("contributor")
            }

            updated_rows.append(updated_row)
            print(f"Refreshed annotations for {dataset.name} (version {dataset.version_number})")

        if updated_rows:
            import pandas as pd
            update_df = pd.DataFrame(updated_rows)

            # Update the collection table
            dataset_collection.update_rows(
                values=update_df,
                primary_keys=["id"],
                dry_run=False,
                wait_for_eventually_consistent_view=True,
            )

            print(f"Updated collection annotations for {len(updated_rows)} datasets")
    # Define task dependencies
    data = fetch_cpath_data()
    transformed_items = transform_data(data)
    selected_items, duplicates = find_duplicated_datasets(transformed_items)
    datasets_to_create, datasets_to_update = identify_dataset_actions(selected_items)
    ignored_datasets = find_ignored_datasets()

    # Handle updates first (versioning existing datasets)
    updated_ids = update_existing_datasets(datasets_to_update)

    # Create completely new datasets
    collection_id = create_new_datasets(datasets_to_create, ignored_datasets)

    # Refresh collection table with latest annotations
    refresh_collection_annotations(collection_id, updated_ids)

    transformed_items >> selected_items >> datasets_to_create
    datasets_to_create >> updated_ids >> collection_id >> refresh_collection_annotations.override(task_id="refresh_annotations")


als_kp_dataset_dag()
