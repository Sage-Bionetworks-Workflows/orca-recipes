import json
import os
import requests

import pandas as pd
from jsonata import jsonata
from jsonschema import validate, ValidationError

from synapseclient import Synapse
from synapseclient.models import Dataset, DatasetCollection, Column, ColumnType, Table


def load_mapping_from_url(url):
    """Load the JSONata mapping expression from a URL"""
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def load_schema_from_url(url):
    """Load the JSON Schema from a URL"""
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def validate_item(item, schema):
    """Validate an item against a JSON Schema"""
    try:
        validate(instance=item, schema=schema)
        return True, None
    except ValidationError as e:
        return False, str(e)


def transform_with_jsonata(source_items, mapping_expr, schema=None):
    """Transform a list of items using a JSONata expression and validate against schema"""
    # Compile the JSONata expression once
    expr = jsonata.Jsonata(mapping_expr)

    # Apply the transformation to each item
    transformed_items = []
    validation_errors = []

    for i, item in enumerate(source_items):
        # Apply the JSONata transformation
        result = expr.evaluate(item)

        # Validate against schema if provided
        if schema:
            is_valid, error = validate_item(result, schema)
            if not is_valid:
                validation_errors.append(
                    {"item_index": i, "error": error, "transformed_item": result}
                )
                continue  # Skip invalid items

        # Add the transformed item to our results
        transformed_items.append(result)

    return transformed_items, validation_errors


def fetch_cpath_data():
    """Fetch data from C-Path API using auth token from environment variable"""
    url = "https://fair.dap.c-path.org/api/collections/als-kp/datasets"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.environ.get('CPATH_AUTH_TOKEN')}",  # to be replaced by an Airflow variable
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise exception for HTTP errors

    return response.json()


def main():
    # URLs for JSONata mapping and JSON Schema
    MAPPING_URL = "https://raw.githubusercontent.com/amp-als/data-model/refs/heads/add-cpath-mapping/mapping/cpath.jsonata"
    SCHEMA_URL = "https://raw.githubusercontent.com/amp-als/data-model/refs/heads/add-cpath-mapping/json-schemas/Dataset.json"

    # Fetch data from C-Path API
    data = fetch_cpath_data()

    # Load mapping and schema from URLs
    mapping_expr = load_mapping_from_url(MAPPING_URL)
    schema = load_schema_from_url(SCHEMA_URL)

    # Transform the data
    transformed_items, validation_errors = transform_with_jsonata(
        data["items"], mapping_expr, schema
    )

    if validation_errors:
        print(f"Found {len(validation_errors)} validation errors.")

    print(len(transformed_items[0]["description"]))
    print(len(transformed_items[1]["description"]))

    # Print the output
    print(json.dumps(transformed_items, indent=2))
    print(f"Successfully transformed {len(transformed_items)} items.")

    syn = Synapse()
    syn.login()

    PROJECT_ID = "syn41746002"

    # Define columns based on the jsonata mapping
    columns = [
        Column(name="id", column_type=ColumnType.ENTITYID),
        Column(name="Title", column_type=ColumnType.STRING, maximum_size=200),
        Column(name="Creator", column_type=ColumnType.STRING, maximum_size=100),
        Column(name="Keywords", column_type=ColumnType.STRING, maximum_size=250),
        Column(name="Subject", column_type=ColumnType.STRING, maximum_size=100),
        Column(name="Collection", column_type=ColumnType.STRING, maximum_size=100),
        Column(name="Publisher", column_type=ColumnType.STRING, maximum_size=100),
        Column(name="Species", column_type=ColumnType.STRING, maximum_size=100),
        Column(name="SameAs", column_type=ColumnType.STRING, maximum_size=100),
    ]

    dataset_collection = DatasetCollection(
        name="ALS-KP Dataset Collection",
        description="A collection of ALS-KP datasets",
        parent_id=PROJECT_ID,
        include_default_columns=True,
        columns=columns,
    )

    for item in transformed_items:
        # Truncate the description to 1000 characters if needed
        if len(item["description"]) > 1000:
            dataset_description = item["description"][:1000]
        else:
            dataset_description = item["description"]

        dataset = Dataset(
            name=item["title"],
            description=dataset_description,
            parent_id=PROJECT_ID,
        ).store()
        dataset_collection.add_item(dataset)

    # Store the dataset collection first
    dataset_collection = dataset_collection.store()

    # Prepare data for annotation
    dataset_ids = [item.id for item in dataset_collection.items]

    # Create a DataFrame with all the annotation data
    annotation_data = pd.DataFrame(
        {
            "id": dataset_ids,
            "Title": [item["title"] for item in transformed_items],
            "Creator": [", ".join(item["creator"]) for item in transformed_items],
            "Keywords": [", ".join(item["keywords"]) for item in transformed_items],
            "Subject": [", ".join(item["subject"]) for item in transformed_items],
            "Collection": [", ".join(item["collection"]) for item in transformed_items],
            "Publisher": [item["publisher"] for item in transformed_items],
            "Species": ["".join(item["species"]) for item in transformed_items],
            "SameAs": [item["sameAs"] for item in transformed_items],
        }
    )

    dataset_collection.store()
    dataset_collection.update_rows(
        values=annotation_data, primary_keys=["id"], dry_run=False
    )


if __name__ == "__main__":
    main()
