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
