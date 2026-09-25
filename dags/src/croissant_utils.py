"""
Croissant metadata generation helpers.

These functions are pure transformations: given data already fetched from
Synapse (dataset entity, file instances) and Snowflake (a DataFrame of file
md5/content-type), they build the Croissant JSON-LD document. All I/O — Synapse
queries, Snowflake queries, S3/Synapse pushes, Airflow orchestration — lives in
the calling DAG. Call `build_croissant_metadata` with all inputs to get a
complete Croissant file.
"""

import mimetypes
from typing import Any, Dict, List, Union

from pandas import DataFrame
from synapseclient import Entity
from synapseclient.core.utils import delete_none_keys
from synapseclient.models import File

# Static mapping of Data Use Ontology (DUO) term codes to their human-readable
# labels. Sourced from the EBI Ontology Lookup Service:
# https://www.ebi.ac.uk/ols4/ontologies/duo . Obsolete and purely structural
# terms are intentionally omitted. Update this map if a new DUO term appears in
# the `dataUseModifiers` field of a Synapse dataset; unmapped codes fall back to
# the raw code as the name.
DUO_CODE_TO_NAME = {
    "DUO:0000004": "no restriction",
    "DUO:0000006": "health or medical or biomedical research",
    "DUO:0000007": "disease specific research",
    "DUO:0000011": "population origins or ancestry research only",
    "DUO:0000012": "research specific restrictions",
    "DUO:0000015": "no general methods research",
    "DUO:0000016": "genetic studies only",
    "DUO:0000018": "not for profit, non commercial use only",
    "DUO:0000019": "publication required",
    "DUO:0000020": "collaboration required",
    "DUO:0000021": "ethics approval required",
    "DUO:0000022": "geographical restriction",
    "DUO:0000024": "publication moratorium",
    "DUO:0000025": "time limit on use",
    "DUO:0000026": "user specific restriction",
    "DUO:0000027": "project specific restriction",
    "DUO:0000028": "institution specific restriction",
    "DUO:0000029": "return to database or resource",
    "DUO:0000031": "method development",
    "DUO:0000042": "general research use",
    "DUO:0000043": "clinical care use",
    "DUO:0000044": "population origins or ancestry research prohibited",
    "DUO:0000045": "not for profit organisation use only",
    "DUO:0000046": "non-commercial use only",
}

# The `@context` block shared by every full Croissant file produced here.
CROISSANT_CONTEXT = {
    "@language": "en",
    "@vocab": "https://schema.org/",
    "sc": "https://schema.org/",
    "cr": "http://mlcommons.org/croissant/",
    "rai": "http://mlcommons.org/croissant/RAI/",
    "dct": "http://purl.org/dc/terms/",
    "duo": "https://purl.obolibrary.org/obo/DUO_",
    "annotation": "cr:annotation",
    "arrayShape": "cr:arrayShape",
    "citeAs": "cr:citeAs",
    "column": "cr:column",
    "conformsTo": "dct:conformsTo",
    "containedIn": "cr:containedIn",
    "data": {"@id": "cr:data", "@type": "@json"},
    "dataType": {"@id": "cr:dataType", "@type": "@vocab"},
    "equivalentProperty": "cr:equivalentProperty",
    "examples": {"@id": "cr:examples", "@type": "@json"},
    "excludes": "cr:excludes",
    "extract": "cr:extract",
    "field": "cr:field",
    "fileProperty": "cr:fileProperty",
    "fileObject": "cr:fileObject",
    "fileSet": "cr:fileSet",
    "format": "cr:format",
    "includes": "cr:includes",
    "isArray": "cr:isArray",
    "isLiveDataset": "cr:isLiveDataset",
    "jsonPath": "cr:jsonPath",
    "key": "cr:key",
    "md5": "cr:md5",
    "parentField": "cr:parentField",
    "path": "cr:path",
    "recordSet": "cr:recordSet",
    "references": "cr:references",
    "regex": "cr:regex",
    "readLines": "cr:readLines",
    "repeated": "cr:repeated",
    "replace": "cr:replace",
    "samplingRate": "cr:samplingRate",
    "sdVersion": "cr:sdVersion",
    "separator": "cr:separator",
    "source": "cr:source",
    "subField": "cr:subField",
    "transform": "cr:transform",
    "unArchive": "cr:unArchive",
    "value": "cr:value",
}


def construct_distribution_section_for_files(
    files_attached_to_dataset: List[File], file_md5_and_types: DataFrame
) -> List[Dict[str, str]]:
    """
    Construct the distribution section for the files attached to the dataset. A
    `FileObject` is created for each file, using the md5 checksum and content
    type looked up from `file_md5_and_types`.

    The caller is responsible for querying Snowflake for the md5/content-type
    DataFrame; this function only reads from it.

    Arguments:
        files_attached_to_dataset: The list of files attached to the dataset.
        file_md5_and_types: A DataFrame with columns `ID`, `CONTENTMD5`, and
            `CONTENT_TYPE` for the files. May be empty when no md5 data was
            available in Snowflake.

    Returns:
        The distribution section for the files attached to the dataset.
    """
    distribution_files = []

    for file in files_attached_to_dataset:
        file: File = file
        file_content_type = None
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

        file_object = {
            "@type": "cr:FileObject",
            "@id": f"{file.id}.{file.version_number}",
            "name": f"{file.name}",
            "description": file.description if file.description else f"Data file associated with {file.name}",
            "contentUrl": f"https://www.synapse.org/Synapse:{file.id}.{file.version_number}",
            "encodingFormat": file_content_type,
        }
        # Only include the md5 checksum when it is actually known. Emitting a
        # placeholder value would trip checksum validation in consuming tools.
        # sha256 is not available from Synapse, so it is omitted entirely.
        if isinstance(file_md5, str) and file_md5 and file_md5 != "unknown_md5":
            file_object["md5"] = file_md5
        distribution_files.append(file_object)
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
                "@type": "cr:Field",
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
        "@type": "cr:RecordSet",
        "@id": "default",
        "name": "default",
        "description": "Metadata for the dataset",
        "field": metadata_fields
    }


def construct_bibtex_citation(dataset: Entity, dataset_id: str, dataset_version: str) -> str:
    """
    Construct a BibTeX-formatted citation string for the dataset. The Croissant
    specification expects the `citeAs` property to be a BibTeX-formatted string, so
    this is used to populate that property.

    The entry is built from the metadata that is reliably available on the Synapse
    dataset entity. Fields that are not present are simply omitted from the entry.

    Arguments:
        dataset: The dataset entity from Synapse.
        dataset_id: The ID of the dataset.
        dataset_version: The version of the dataset.

    Returns:
        A BibTeX-formatted citation string, e.g.

        @misc{syn12345.1,
          title = {My Dataset},
          year = {2025},
          publisher = {Synapse - Sage Bionetworks},
          howpublished = {\\url{https://www.synapse.org/Synapse:syn12345.1}},
          url = {https://www.synapse.org/Synapse:syn12345.1}
        }
    """
    url = f"https://www.synapse.org/Synapse:{dataset_id}.{dataset_version}"

    title = (
        dataset.title[0]
        if hasattr(dataset, "title") and dataset.title
        else dataset.name
    )

    # `createdOn` is an ISO-8601 timestamp, e.g. "2025-02-01T12:00:00.000Z".
    # The first four characters are the publication year.
    year = str(dataset.createdOn)[:4] if getattr(
        dataset, "createdOn", None) else None

    # `creator` is not part of the PortalDataset schema, but include it as the
    # BibTeX author when the entity happens to expose it.
    author = None
    if getattr(dataset, "creator", None):
        author = dataset.creator[0] if isinstance(
            dataset.creator, list) else dataset.creator

    # Ordered so the most important fields appear first in the entry. Only
    # fields with a value are emitted.
    fields = {
        "title": title,
        "author": author,
        "year": year,
        "publisher": "Synapse - Sage Bionetworks",
        "howpublished": f"\\url{{{url}}}",
        "url": url,
    }

    body = ",\n".join(
        f"  {key} = {{{value}}}" for key, value in fields.items() if value
    )
    return f"@misc{{{dataset_id}.{dataset_version},\n{body}\n}}"


def construct_usage_info(data_use_modifiers: Union[str, List[str]]) -> List[Dict[str, str]]:
    """
    Convert the Data Use Ontology (DUO) codes from the Synapse `dataUseModifiers`
    field into a list of schema.org `DefinedTerm` objects for the Croissant
    `usageInfo` property.

    Each DUO CURIE (e.g. `DUO:0000042`) is expanded into:
    - `name`: the human-readable label from `DUO_CODE_TO_NAME`, falling back to the
        raw code when it is not in the map.
    - `termCode`: the DUO code with the separator normalized to `_` (e.g.
        `DUO_0000042`).
    - `url`: the compact CURIE with a lower-cased prefix (e.g. `duo:0000042`).

    Arguments:
        data_use_modifiers: The value of the dataset's `dataUseModifiers` annotation.
            Expected to be a list of DUO CURIEs, but a single string is also handled.

    Returns:
        A list of `DefinedTerm` dictionaries, or `None` when there are no modifiers
        (so that `delete_none_keys` drops the property).
    """
    if not data_use_modifiers:
        return None

    if isinstance(data_use_modifiers, str):
        data_use_modifiers = [data_use_modifiers]

    usage_info = []
    for modifier in data_use_modifiers:
        # Accept either `DUO:0000042` or `DUO_0000042` and normalize to the
        # colon form used as the key in `DUO_CODE_TO_NAME`.
        curie = modifier.replace("_", ":")
        prefix, _, local_id = curie.partition(":")
        usage_info.append(
            {
                "@type": "DefinedTerm",
                "name": DUO_CODE_TO_NAME.get(curie, modifier),
                "termCode": f"{prefix}_{local_id}",
                "url": f"{prefix.lower()}:{local_id}",
            }
        )
    return usage_info


def construct_creators(dataset: Entity) -> List[Dict[str, str]]:
    """
    Convert the Synapse `creator` annotation into a list of schema.org `Person`
    objects for the Croissant `creator` property. `creator` is a required Croissant
    property that expects values of type `Organization` or `Person`.

    The PortalDataset schema defines `creator` as an ordered list of the main
    researchers involved in producing the data, so each entry is represented as a
    `sc:Person`.

    Arguments:
        dataset: The dataset entity from Synapse.

    Returns:
        A list of `Person` dictionaries, or `None` when no creator is present (so
        that `delete_none_keys` drops the property).
    """
    creators = getattr(dataset, "creator", None)
    if not creators:
        return None

    if isinstance(creators, str):
        creators = [creators]

    return [
        {"@type": "sc:Person", "name": creator}
        for creator in creators
        if creator
    ] or None


def build_croissant_metadata(
    dataset: Entity,
    dataset_id: str,
    dataset_version: int,
    files_attached_to_dataset: List[File],
    file_md5_and_types: DataFrame,
) -> Dict[str, Any]:
    """
    Build a complete Croissant JSON-LD document for a single dataset from
    already-fetched inputs. No I/O is performed here — the caller supplies the
    dataset entity, its file instances, and a DataFrame of file md5/content-type
    values (typically queried from Snowflake).

    Arguments:
        dataset: The dataset entity from Synapse.
        dataset_id: The ID of the dataset.
        dataset_version: The version of the dataset.
        files_attached_to_dataset: The file instances attached to the dataset.
        file_md5_and_types: A DataFrame with columns `ID`, `CONTENTMD5`, and
            `CONTENT_TYPE` for the files. May be empty.

    Returns:
        The full Croissant JSON-LD document as a dictionary, with `None`-valued
        properties removed.
    """
    # The distribution section describes the files attached to the dataset. The
    # first `metadata` file describes the metadata associated with the dataset
    # and where to find it on the Synapse server (the Dataset view).
    distribution_files = [{
        "@type": "cr:FileObject",
        "@id": "metadata",
        "contentUrl": f"https://www.synapse.org/Synapse:{dataset_id}.{dataset_version}",
        "name": "metadata",
        "description": f"Metadata associated with {dataset.name}",
        "encodingFormat": "text/csv",
    }] + construct_distribution_section_for_files(files_attached_to_dataset, file_md5_and_types)

    record_set = construct_record_set_section_for_files(
        files_attached_to_dataset=files_attached_to_dataset)

    croissant_file = {
        "@context": CROISSANT_CONTEXT,
        "@type": "sc:Dataset",
        "@id": f"{dataset_id}.{dataset_version}",
        "name": f"{dataset.name}",
        "description": dataset.description if hasattr(dataset, "description") and dataset.description else f"Dataset for {dataset.name}",
        "url": f"https://www.synapse.org/Synapse:{dataset_id}.{dataset_version}",
        "datePublished": dataset.createdOn,
        "dateModified": dataset.modifiedOn,
        "creator": construct_creators(dataset),
        "license": (
            dataset.license[0]
            if hasattr(dataset, "license") and dataset.license
            else {
                "@type": "sc:CreativeWork",
                "name": "License not specified",
                "url": f"https://www.synapse.org/Synapse:{dataset_id}.{dataset_version}"
            }
        ),
        "version": dataset_version,
        "dct:conformsTo": "http://mlcommons.org/croissant/1.1",
        # https://github.com/nf-osi/nf-metadata-dictionary/blob/main/registered-json-schemas/PortalDataset.json
        # These fields are derived from: https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/refs/heads/main/registered-json-schemas/PortalDataset.json
        # If a more specific schema is needed, then we can add a mapping for the dataset to point to the schema and pull these in dynamically.
        "alternateName": dataset.alternateName[0] if hasattr(dataset, "alternateName") else None,
        "citeAs": construct_bibtex_citation(dataset, dataset_id, dataset_version),
        "countryOfOrigin": dataset.countryOfOrigin if hasattr(dataset, "countryOfOrigin") else None,
        "diseaseFocus": dataset.diseaseFocus if hasattr(dataset, "diseaseFocus") else None,
        "keywords": dataset.keywords if hasattr(dataset, "keywords") else None,
        "measurementTechnique": dataset.measurementTechnique if hasattr(dataset, "measurementTechnique") else None,
        "subject": dataset.subject if hasattr(dataset, "subject") else None,
        "title": dataset.title[0] if hasattr(dataset, "title") else None,
        "usageInfo": construct_usage_info(dataset.dataUseModifiers if hasattr(dataset, "dataUseModifiers") else None),
        "distribution": distribution_files,
        "recordSet": record_set,
    }
    delete_none_keys(croissant_file)
    return croissant_file


if __name__ == "__main__":
    # Self-check for the DUO CURIE normalization, which accepts both `:` and `_`
    # separators and falls back to the raw code for unmapped terms.
    assert construct_usage_info(None) is None
    assert construct_usage_info("DUO:0000042") == [
        {"@type": "DefinedTerm", "name": "general research use",
         "termCode": "DUO_0000042", "url": "duo:0000042"}
    ]
    assert construct_usage_info(["DUO_0000042", "DUO:9999999"]) == [
        {"@type": "DefinedTerm", "name": "general research use",
         "termCode": "DUO_0000042", "url": "duo:0000042"},
        {"@type": "DefinedTerm", "name": "DUO:9999999",
         "termCode": "DUO_9999999", "url": "duo:9999999"},
    ]
    print("croissant_utils self-check passed")
