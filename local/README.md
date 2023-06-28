# local development scripts
A home for experimental scripts used for launching Nextflow Tower workflows

## Setup

These instructions assume that you already have Python, a Python version manager (`pyenv`), and `pipenv` installed.

### Python Environment

Set up your Python environment for using these scripts by ruinning
```
pipenv install --dev
pipenv shell
```

### Environment Variables

In order for the scripts leveraging `py-orca` to connect to Nextflow Tower, you will need to configure the following environment variables:
    - `NEXTFLOWTOWER_CONNECTION_URI`
    - `SYNAPSE_CONNECTION_URI`
    - `AWS_ACCESS_KEY_ID`
    - `AWS_SECRET_ACCESS_KEY`
    - `AWS_SESSION_TOKEN`
    * Optionally, you can use `aws sso login` in place of the above AWS environment variable credentials if you have pre-configured AWS profiles already and include `AWS_PROFILE` in your `.env` file
You can copy `local/.env.example` into a local `local/.env` file, replace the placeholders with your credentials, and run `source local/.env` in your terminal.

## LENS

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
python3 local/lens.py run --dataset_id <yaml-dataset-synapse-id> --s3_prefix s3://<your-s3-bucket>/<your-s3-subdirectory>
```
