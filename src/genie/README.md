# Genie ELT scripts

Ingestion scripts for loading in the GENIE database in Snowflake

## How to Use

You will need to be a developer on the genie projects and have access to all of the expected accesses in order to run anything in this module. See [GENIE - Getting Started](https://sagebionetworks.jira.com/wiki/spaces/DPE/pages/2552037385/Genie) for more information.

- Modify the [genie_bpc_releases yaml](src/genie/genie_bpc_releases.yaml) when a new BPC release has been QC'ed and approved for release for a given cohort. Create a PR to push the changes.
- Modify the [genie_sp_releases yaml](src/genie/genie_sp_releases.yaml) when a new SP release has been QC'ed and approved for release. Create a PR to push the changes.

Main GENIE releases are automatically ingested.

## Example Usage

Run ingestion to Snowflake to the genie dev database. for Genie Sponsored Projects (SP). This will also overwrite any pre-existing data in the tables in the GENIE_DEV database and create new tablesn for new data when applicable.

```bash
python3 src/genie/genie_sp_elt.py --database GENIE_DEV --overwrite
```

Run ingestion to Snowflake for Genie Biopharma Collaborative (BPC) Project to the genie dev database. This will not overwrite any pre-existing data in the tables in the GENIE_DEV database and will instead skip. This will still create new tables for new data when applicable.

```bash
python3 src/genie/genie_bpc_elt.py --database GENIE_DEV
```

Run ingestion to Snowflake for Main Genie Project to the genie dev database.

```bash
python3 src/genie/main_genie_elt.py --database GENIE_DEV --overwrite
```