# Grant Ingestion Pipeline

This script scrapes grant information from a list of URLs and outputs a consolidated CSV file.

## Scraping non-federal grants

### Dependencies

You will need the following:

* **Access to the data model Synapse CSV**
* **A valid Synapse account**, with `synapseclient` configured in your environment
* **A developer account with Anthropic**
* **Anthropic API key** stored in your environment as:

  ```bash
  ANTHROPIC_API_KEY
  ```

### Setting Up the Environment

1. Ensure you are using **Python 3**.
2. Install the required Python packages:

   ```bash
   pip install anthropic requests pandas synapseclient
   ```

3. Confirm your Synapse credentials are configured (e.g., via `~/.synapseConfig`).

### How to Run

1. Gather a list of URLs to scrape.

   * Provide them in a text file named `urls.txt`, **one URL per line**,
     **or**
   * Update the local `urls` variable directly in the script.

   A pre-existing table of URLs is available here:
   [https://www.synapse.org/Synapse:syn72003087](https://www.synapse.org/Synapse:syn72003087)

2. Run the script:

   ```bash
   python3 scrape_grants.py
   ```

3. After completion, check your working directory for the output file:

   ```text
   all_grants.csv
   ```

4. Save the `all_grants.csv` to https://www.synapse.org/Synapse:syn72004505

## Running the rest of the ingestion pipeline

1. Run the script:

    ```bash
    python3 retrieve_grants.py
    ```

2. This will create the compiled federal + non-federal grant results and upload to a Snowflake table. If you have access to Snowflake and the `DATA_ANALYTICS` role granted to you, you can query the table like so:

    ```sql
    -- show the entire table with all columns
    select *
    from sage.grants.grants_pipeline_test;

    -- select only relevant columns
    select "title", "funding_amount", "grant_duration", "contact_info", "end_date"
    from sage.grants.grants_pipeline_test
    ```