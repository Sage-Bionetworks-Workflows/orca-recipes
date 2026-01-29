# Genie Ingestion scripts

## Overview

Ingestion scripts for loading in the GENIE database in Snowflake. Note that
you can have multiple records per patient-sample pairs per table for the Main Genie and BPC ingestions as we ingest multiple releases per table.

- Main genie will be the most automated in ingesting new releases. There will be one schema where all of the releases (public and consortium) are stacked into one table called `CLINICAL_SAMPLE`

    So a `CLINICAL_SAMPLE` table for main genie would look like:

    | SAMPLE_ID | PATIENT_ID | RELEASE | RELEASE_TYPE | MAJOR_VERSION | MINOR_VERSION | OTHER_COLUMNS | 
    |-----------|------------|---------|--------------|---------------|---------------|---------------|
    | GENIE-SAGE-1-1 | GENIE-SAGE-1 | 17_2_CONSORTIUM | CONSORTIUM | 17 | 2 | ... |
    | GENIE-SAGE-1-2 | GENIE-SAGE-1 | 17_2_CONSORTIUM | CONSORTIUM | 17 | 2 | ... |
    | GENIE-SAGE-1-1 | GENIE-SAGE-1 | 17_0_PUBLIC | PUBLIC | 17 | 0 | ... |
    | GENIE-SAGE-1-2 | GENIE-SAGE-1 | 17_0_PUBLIC | PUBLIC | 17 | 0 | ... |
    | GENIE-SAGE-1-1 | GENIE-SAGE-1 | 17_3_CONSORTIUM | CONSORTIUM | 17 | 3 | ... |

- Genie BPC will need a config update to ingest releases. This is due to the structure of the BPC projects
    A `CANCER_PANEL_TEST` table for BPC will look like the above for main genie but with an addition `COHORT` column:

    | SAMPLE_ID | PATIENT_ID | COHORT | RELEASE | RELEASE_TYPE | MAJOR_VERSION | MINOR_VERSION | OTHER_COLUMNS | 
    |-----------|------------|---------|---------|--------------|---------------|---------------|---------------|
    | GENIE-SAGE-1-1 | GENIE-SAGE-1 | BRCA | 2_2_CONSORTIUM | CONSORTIUM | 2 | 2 | ... |
    | GENIE-SAGE-1-2 | GENIE-SAGE-1 | BRCA | 2_2_CONSORTIUM | CONSORTIUM | 2 | 2 | ... |
    | GENIE-SAGE-1-1 | GENIE-SAGE-1 | BRCA | 2_0_PUBLIC | PUBLIC | 2 | 0 | ... |
    | GENIE-SAGE-1-2 | GENIE-SAGE-1 | BRCA | 2_0_PUBLIC | PUBLIC | 2 | 0 | ... |
    | GENIE-SAGE-1-1 | GENIE-SAGE-1 | BRCA | 2_1_CONSORTIUM | CONSORTIUM | 2 | 1 | ... |
    

- Genie SP will only ingest the clinical files as the rest as not as relevant. There will be one schema-table per SP project because each project's clinical table fields are too vastly differently for them to be stacked into the same table. As a result, downstream our query to put all of the tables together will be more complicated.

    | SAMPLE_ID | PATIENT_ID | RELEASE | OTHER_COLUMNS | 
    |-----------|------------|---------|---------|
    | GENIE-SAGE-1-1 | GENIE-SAGE-1 | SP_PROJECT_NAME | ... |
    | GENIE-SAGE-1-2 | GENIE-SAGE-1 | SP_PROJECT_NAME | ... |
    | GENIE-SAGE-1-1 | GENIE-SAGE-1 | SP_PROJECT_NAME | ... |
    | GENIE-SAGE-1-2 | GENIE-SAGE-1 | SP_PROJECT_NAME | ... |
    | GENIE-SAGE-1-1 | GENIE-SAGE-1 | SP_PROJECT_NAME | ... |

## Getting Started 

Please note that everything here is for how to setup your environment and run the code locally.

### Setting up your system

- You will need to be a developer on the genie projects and have access to all of the expected access in order to run anything in this module. See [GENIE - Getting Started](https://sagebionetworks.jira.com/wiki/spaces/DPE/pages/2552037385/Genie) for more information.

- See the [snowflake_utils running locally docs](/src/snowflake_utils/README.md#connecting-to-snowflake-locally) in your code for how to setup your local system's snowflake credentials

- Create a python environment using a env manager of your choice (e.g: `python -m venv <virtual environment name>`) and install the requirements into it

    ```bash
    pip install -r requirements-airflow.txt
    pip install -r requirements-dev.txt
    ```

### How to Use

1. Modify the [genie_bpc_releases yaml](src/genie/genie_bpc_releases.yaml) when a new BPC release has been QC'ed and approved for release for a given cohort. Format should be:

    ```yaml
    - cohort: [sponsored project abbreviation]
    patient_id_key: [name of the patient id column]
    sample_id_key: [name of the sample id column]
    table_name: [name to call the snowflake table]
    file_synid: [synapse_id to the sponsored project clinical file]
    ```

1. Modify the [genie_sp_releases yaml](src/genie/genie_sp_releases.yaml) when a new SP release has been QC'ed and approved for release. Format should be:

    ```yaml
    - cohort: [cohort_name]
    version: public_02_2
    clinical_synid: [synapse_id to the folder with the clinical release files]
    cbioportal_synid: [synapse_id to the folder with the cbioportal release files]
    ```

1. Main GENIE releases are automatically ingested. Add/exclude entire releases in main genie by adding to `RELEASES_TO_SKIP` in [main_genie_ingestion.py](/src/genie/main_genie_ingestion.py)

1. Create a PR to push the changes.


### Example Usage

Run ingestion to Snowflake to the genie dev database. for Genie Sponsored Projects (SP). This will also overwrite any pre-existing data in the tables in the GENIE_DEV database and create new tables for new data when applicable.

```bash
python3 src/genie/genie_sp_ingesion.py --database GENIE_DEV --overwrite
```

Run ingestion to Snowflake for Genie Biopharma Collaborative (BPC) Project to the genie dev database. This will not overwrite any pre-existing data in the tables in the GENIE_DEV database and will instead skip writing to the table if it already exists. This will still create new tables for new data when applicable.

```bash
python3 src/genie/genie_bpc_ingestion.py --database GENIE_DEV 
```

Run ingestion to Snowflake for Main Genie Project to the genie dev database.

```bash
python3 src/genie/main_genie_ingestion.py --database GENIE_DEV --overwrite-partition
```