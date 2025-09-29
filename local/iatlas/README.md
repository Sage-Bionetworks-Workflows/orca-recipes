# iAtlas Recipes
A home for recipes used for launching Nextflow Tower workflows for iAtlas Data Processing

## Setup

These instructions assume that you already have Python (>3.10.0), a Python version manager (`pyenv`), and `pipenv` installed.

### Python Environment

Set up your Python environment for using these scripts by ruinning from the `orca-recipes` home directory.
```
pipenv install --dev
pipenv shell
```

### Environment Variables

In order for the scripts leveraging `py-orca` to connect to Nextflow Tower, you will need to configure the following environment variables:
    - `NEXTFLOWTOWER_CONNECTION_URI`
    - `SYNAPSE_CONNECTION_URI`
    - `AWS_ACCESS_KEY_ID`*
    - `AWS_SECRET_ACCESS_KEY`*
    - `AWS_SESSION_TOKEN`*

* Optionally, you can use `aws sso login` in place of the above AWS environment variable credentials if you have pre-configured AWS profiles already and include `AWS_PROFILE` in your `.env` file
You can copy `local/iatlas/.env.example` into a local `local/iatlas/.env` file, replace the placeholders with your credentials, and run `source local/iatlas/.env` in your terminal.

## Immune Subtype Classifier

Steps to run the Immune Subtype Classifier workflow:
1. Upload all sample files to a folder on Synapse. Make sure that the only `.tsv` files in the folder are the ones that you want to be processed.
2. Prepare the master data sheet by executing `local/iatlas/immune_subtype_classifier/prepare_data_sheet.py` with three arguments:
    - `parent`: Synapse ID of the folder where your data files are
    - `export_name`: Name that you want the master data file to be exported to
    - `upload_location`: Synapse ID of the folder that you want to upload the master data file to
```
python local/iatlas/immune_subtype_classifier/prepare_data_sheet.py <parent> <export_name> <upload_location>
```
3. Create and store your CWL `.json` configuration file in the same location in Synapse as the data file produced by the previous step.
Example `.json` file:
``` immune_subtype_classifier_input.json
{
    "input_file": {
        "path": <export_name>,
        "class": "File"
    },
    "input_gene_column": "gene"
}
```
4. Create and store your `nf-synstage` input `.csv` file in an S3 bucket accessible to the Nextflow Tower workspace (`s3://iatlas-project-tower-bucket` or `s3://iatlas-project-tower-scratch`).
Example `.csv` file:
``` input.csv
data_file,input_file
<synapse_id_for_master_data_sheet>,<synapse_id_for_json_input_file>
```
5. Stage the master data sheet and your CWL `.json` configuration file to S3 buckets by executing `local/iatlas/immune_subtype_classifier/nf_stage.py` with three arguments:
    - `run_name`: What you want the workflow on Nextflow Tower to be named
    - `input`: S3 URI for your `nf-synstage`-friendly input `.csv` file 
    - `outdir`: S3 URI for where you want the output of `nf-synstage` to be stored
```
python local/iatlas/immune_subtype_classifier/nf_stage.py <run_name> <input> <outdir>
```
6. Execute the Immune Subtypes Classifier workflow on Nextflow Tower by executing `local/iatlas/immune_subtype_classifier/nf_launch.py` with three arguments:
    - `run_name`: What you want the workflow on Nextflow Tower to be named
    - `s3_file`: S3 URI the output from `nf-synstage`
    - `cwl_file`: File path to your CWL workflow file
```
python local/iatlas/immune_subtype_classifier/nf_launch.py <run_name> <s3_file> <cwl_file>
```

## LENS (Work in Progress)

Steps to run LENS workflow:
1. Create necessary input files and upoload them to Synapse.
    - The metaflow DAG requires 2 pre-formatted input files
        - an input CSV file containing the LENS manifest information along with the Synapse URI's needed for `nf-synstage`
        - A dataset YAML file containing a dataset ID and the Synapse ID for the above described input CSV file
2. Set up your environment.
    - Use the `.env.example` file to create your own `.env` file with all of the necessary environment variables defined.
    - Source your `.env` file 
    ```
    source .env
    ```
3. Run the `lens.py` script with appropriate inputs
```
python3 local/iatlas/lens.py run --dataset_id <yaml-dataset-synapse-id> --s3_prefix s3://<your-s3-bucket>/<your-s3-subdirectory>
```

## cbioportal_export

### Table of Contents

- [cbioportal_export](#cbioportal_export)
  - [Overview](#overview)
    - [maf_to_cbioportal.py](#maf_to_cbioportalpy)
    - [clinical_to_cbioportal.py](#clinical_to_cbioportalpy)
  - [Setup](#setup-1)
  - [How to Run](#how-to-run)
  - [Outputs](#outputs)
    - [maf_to_cbioportal.py](#maf_to_cbioportalpy-1)
    - [clinical_to_cbioportal.py](#clinical_to_cbioportalpy-1)
  - [General Workflow](#general-workflow)

### Overview

#### maf.py
This script will run the iatlas mutations data through genome nexus so it can be ingested by cbioportal team for visualization.

The script does the following:

1. Reads in and merges all the individual mafs from a given folder
2. Splits the maf into smaller chunks for genome nexus annotation
3. [Annotates via genome nexus](https://github.com/genome-nexus/genome-nexus-annotation-pipeline)
4. Concatenates the results
5. [Creates the required meta_* data](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/generate-meta-files)


#### clinical.py
This script will process/transform the iatlas clinical data to be cbioportal format friendly so it can be ingested by cbioportal team for visualization.

The script does the following:

1. Preprocesses the data and adds [required mappings like ONCOTREE or LENS_ID](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/oncotree-code-converter)
2. [Adds clinical headers](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/add-clinical-header)
3. [Creates the required meta_* data](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/generate-meta-files)
4. [Creates the required caselists](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/generate-case-lists)
5. [Validates the files for cbioportal](https://github.com/cBioPortal/cbioportal-core/blob/main/scripts/importer/validateData.py)


### Setup
Prior to testing/developing/running this locally, you will need to setup the Docker image in order to run this.
Optional: You can also build your environment via python env and install the `uv.lock` file

1. Create and activate your venv

```
python3 -m venv <your_env_name>
source <your_env_name>/bin/activate
```

2. Export dependencies from uv.lock

```
pip install uv
uv export > requirements.txt
```

3. Install into your venv

```
pip install -r requirements.txt
```

But it is highly recommended you use the docker image

1. Build the dockerfile

```
cd /orca-recipes/local/iatlas/cbioportal_export
docker build -f Dockerfile -t <some_docker_name> .
```

2. Run the Dockerfile

```
docker run --rm -it -e SYNAPSE_AUTH_TOKEN=$YOUR_SYNAPSE_TOKEN <some_docker_image_name>
```

3. Follow the **How to Run** section below

### How to Run

Getting help
```
python3 clinical.py --help
```

```
python3 maf.py --help
```

```
python3 load.py --help
```

### Outputs

This pipeline generates the following key datasets that eventually get uploaded to synapse and ingested by cbioportal.
All datasets will be saved to:
`<datahub_tools_path>/add-clinical-header/<dataset_name>/` unless otherwise stated

#### maf.py

- `data_mutations_annotated.txt` – Annotated MAF file from genome nexus
    - Generated by: `concatenate_mafs()`

- `data_mutations_error_report.txt` – Error report from genome nexus
    - Generated by: `genome_nexus`

- `meta_mutations.txt` – Metadata file for mutations data
    - Generated by: `datahub-study-curation-tools`' `generate-meta-files` code


#### clinical.py

- `data_clinical_patient.txt` – Clinical patient data file
    - Generated by: `add_clinical_header()`

- `data_clinical_sample.txt` – Clinical sample data file
    - Generated by: `add_clinical_header()`

- `meta_clinical_patient.txt` – Metadata file for clinical patient data file
    - Generated by: `datahub-study-curation-tools`' `generate-meta-files` code

- `meta_clinical_sample.txt` – Metadata file for clinical sample data file
    - Generated by: `datahub-study-curation-tools`' `generate-meta-files` code

- `meta_study.txt` – Metadata file for the entire study
    - Generated by: `datahub-study-curation-tools`' `generate-meta-files` code

- `cases_<cancer_type>.txt` – case list files for each cancer type available in the clinical data
    - `<datahub_tools_path>/add-clinical-header/<dataset_name>/case-lists/`
    - Generated by: `datahub-study-curation-tools`' `generate-case-lists` code


#### validate.py

- `iatlas_validation_log.txt` - Validator results from our own iatlas validation results for all of the files
    - Generated by: updated by each validation function

- `cbioportal_validator_output.txt` – Validator results from cbioportal for all of the files not just clinical
    - Generated by: `cbioportal`' validator code

#### load.py

- `cases_all.txt` – case list file for all the clinical samples in the study
    - `<datahub_tools_path>/add-clinical-header/<dataset_name>/case-lists/`
    - Generated by: `datahub-study-curation-tools`' `generate-case-lists` code

- `cases_sequenced.txt` – case list file containing the sequenced samples (mutation)
in the study
    - `<datahub_tools_path>/add-clinical-header/<dataset_name>/case-lists/`
    - Generated by: `datahub-study-curation-tools`' `generate-case-lists` code


Any additional files are the intermediate processing files and can be ignored.


### General Workflow

1. Run processing on the maf datasets via `maf.py`
2. Run processing on the clinical datasets via `clinical.py`
3. Run `load.py` to create case lists
4. Run the general validation + cbioportal validator on your outputted files via `validate.py`
5. Check your `cbioportal_validator_output.txt`
6. Resolve any `ERROR`s
7. Repeat steps 4-6 until all `ERROR`s are gone
8. Run `load.py` now with the `upload` flag to upload to synapse

**Example:**
Sample workflow

Run clinical processing
```
python3 clinical.py 
    --input_df_synid syn66314245 \
    --cli_to_cbio_mapping_synid syn66276162 
    --cli_to_oncotree_mapping_synid syn66313842 \
    --datahub_tools_path /<some_path>/datahub-study-curation-tools \
    --lens_id_mapping_synid syn68826836
    --neoantigen-data-synid syn21841882
```

Run maf processing
```
python3 maf.py 
    --dataset Riaz
    --input_folder_synid syn68785881 
    --datahub_tools_path /<some_path>/datahub-study-curation-tools 
    --n_workers 3 
```

Create the case lists
```
python3 load.py 
    --dataset Riaz  
    --output_folder_synid syn64136279 
    --datahub_tools_path /<some_path>/datahub-study-curation-tools  
    --create_case_lists
```

Run the general iatlas validation + cbioportal validator on all files
```
python3 validate.py 
    --datahub_tools_path /<some_path>/datahub-study-curation-tools 
    --neoantigen_data_synid syn69918168 
    --cbioportal_path /<some_path>/cbioportal/ 
    --dataset Riaz  
```

Save into synapse with version comment `v1`

```
python3 load.py
    --dataset Riaz  
    --output_folder_synid syn64136279
    --datahub_tools_path /<some_path>/datahub-study-curation-tools  
    --version_comment "v1"
    --upload
```

### Running tests

Tests are written via `pytest`.

In your docker environment or local environment, install `pytest` via

```
pip install pytest
```

Then run all tests via
```
python3 -m pytest tests
```