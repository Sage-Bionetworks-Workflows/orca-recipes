import anthropic
import requests
import os
import pandas as pd
import synapseclient

# Configuration
DATA_MODEL_SYNID = "syn72010973"  # Synapse ID to data model file
URLS_FILE = "urls.txt"  # Path to file with URLs (one per line)
OUTPUT_FILE = "extracted_grants.csv"
ANTHROPIC_API_KEY= os.getenv("ANTHROPIC_API_KEY")

def load_data_model(syn : synapseclient.Synapse, data_model_synid : str) -> pd.DataFrame:
    """Load the data model from a CSV file on synapse

    Args:
        syn (synapseclient.Synapse): synapse client connection
        data_model_synid (str): synapse_id of the data model file

    Returns:
        pd.DataFrame: data model as pandas table
    """
    try:
      df = pd.read_csv(syn.get(data_model_synid).path, sep = ",")
      # Assuming the CSV has columns like: field_name, description, required
      return df
    except Exception as e:
        print(f"Error loading data model: {e}")
        return None
      

def format_data_model_for_prompt(data_model_df):
    """Format the data model into a detailed prompt section"""
    prompt_section = "FIELD SPECIFICATIONS:\n\n"
    
    for idx, row in data_model_df.iterrows():
      field_name = row.get('Column_Name', 'Unknown')
      description = row.get('Column_Definition', 'No description')
      
      prompt_section += f"{idx + 1}. **{field_name}**\n"
      prompt_section += f"   - Description: {description}\n"
      prompt_section += "\n"
    
    return prompt_section

def fetch_webpage_content(url):
    """Fetch the HTML content of a webpage"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        return response.text
    except Exception as e:
        return f"Error fetching {url}: {str(e)}"

def extract_grants_with_claude(url, data_model_df):
    """Use Claude to extract grant information from webpage content"""
    
    # Fetch the webpage content
    print(f"Fetching content from {url}...")
    webpage_content = fetch_webpage_content(url)
    
    if webpage_content.startswith("Error fetching"):
        print(webpage_content)
        return None
    
    # Initialize Anthropic client
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    # Get field names for CSV headers
    field_names = data_model_df['Column_Name'].tolist()
    
    # Format the complete data model for the prompt
    data_model_prompt = format_data_model_for_prompt(data_model_df)
    
    # Create the prompt
    prompt = f"""Here is the HTML content from {url}:

<webpage_content>
{webpage_content}
</webpage_content>

Please analyze this webpage and extract all grant opportunities listed.

<data_model>
{data_model_prompt}
</data_model>

EXTRACTION INSTRUCTIONS:
1. Extract information for each grant based on the field specifications above
2. Pay careful attention to the description and data type for each field
3. For example, if a field description mentions "email", extract email addresses for that field
4. Return the results in CSV format with these exact column headers (in order): {', '.join(field_names)}
5. If information is not available for a field, use "N/A"
6. Include all grants found on the page
7. Do not add any additional text before or after the CSV
8. Ensure proper CSV escaping for fields containing commas or quotes
9. Follow any specific format requirements mentioned in the field descriptions

Please be thorough and extract all relevant information according to the data model specifications.
"""
    
    # Call Claude API
    print("Sending to Claude for analysis...")
    try:
        message = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=4000,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract the response
        response_text = message.content[0].text
        
        # Clean up response (remove markdown code blocks if present)
        if response_text.startswith("```csv"):
            response_text = response_text.replace("```csv", "").replace("```", "").strip()
        elif response_text.startswith("```"):
            response_text = response_text.replace("```", "").strip()
            
        return response_text
        
    except Exception as e:
        print(f"❌ Error calling Claude API: {e}")
        return None

def process_multiple_urls(urls, data_model_df, output_file="combined_grants.csv"):
    """Process multiple URLs and combine results"""
    
    all_results = []
    
    for idx, url in enumerate(urls, 1):
        print(f"\n{'='*60}")
        print(f"Processing URL {idx}/{len(urls)}: {url}")
        print(f"{'='*60}")
        
        csv_result = extract_grants_with_claude(url, data_model_df)
        
        if csv_result:
            # Parse CSV result
            try:
                from io import StringIO
                df = pd.read_csv(StringIO(csv_result))
                df['source_url'] = url  # Add source URL column
                df['extraction_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                all_results.append(df)
                print(f"✅ Successfully extracted {len(df)} grants from {url}")
            except Exception as e:
                print(f"⚠️ Error parsing CSV from {url}: {e}")
                # Save raw output for debugging
                debug_file = f"debug_output_{idx}.txt"
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(csv_result)
                print(f"Raw output saved to {debug_file}")
        else:
            print(f"❌ Failed to extract grants from {url}")
    
    # Combine all results
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        combined_df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\n{'='*60}")
        print(f"✨ Successfully saved {len(combined_df)} total grants to {output_file}")
        print(f"{'='*60}")
        return combined_df
    else:
        print("❌ No results to save")
        return None

def main():
    """Main execution function"""
    
    # Configuration
    DATA_MODEL_CSV = "data_model.csv"  # Path to your data model CSV
    URLS_FILE = "urls.txt"  # Path to file with URLs (one per line)
    OUTPUT_FILE = "extracted_grants.csv"
    
    # Load data model from CSV
    print("="*60)
    print("Loading data model...")
    print("="*60)
    syn = synapseclient.login()
    data_model_df = load_data_model(syn=syn, data_model_synid = DATA_MODEL_SYNID)
    
    # Display the data model
    print("\nData Model Preview:")
    print(data_model_df.to_string(index=False))
    print()
    
    # Load URLs
    print("="*60)
    print("Loading URLs...")
    print("="*60)
    
    try:
        with open(URLS_FILE, 'r') as f:
            urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        print(f"✅ Loaded {len(urls)} URLs from {URLS_FILE}")
    except FileNotFoundError:
        print(f"⚠️ {URLS_FILE} not found. Using default URLs...")
        urls = [
            "https://www.aacr.org/professionals/research-funding/current-funding-opportunities/",
        ]
    
    print(f"\nURLs to process:")
    for idx, url in enumerate(urls, 1):
        print(f"  {idx}. {url}")
    
    # Process all URLs
    print("\n" + "="*60)
    print("Starting extraction process...")
    print("="*60)
    results_df = process_multiple_urls(urls, data_model_df, OUTPUT_FILE)
    
    # Display summary
    if results_df is not None:
        print("\n" + "="*60)
        print("EXTRACTION SUMMARY")
        print("="*60)
        print(f"   Total grants extracted: {len(results_df)}")
        print(f"   URLs processed: {len(urls)}")
        print(f"   Output file: {OUTPUT_FILE}")
        print(f"   Fields extracted: {len(data_model_df)}")
        
        # Show first few rows
        print("\nPreview of extracted data:")
        print(results_df.head().to_string())
        
        # Show data quality summary
        print("\nData Quality Summary:")
        for col in data_model_df['Column_Name']:
            if col in results_df.columns:
                filled = results_df[col].notna().sum()
                total = len(results_df)
                pct = (filled / total * 100) if total > 0 else 0
                print(f"   {col}: {filled}/{total} ({pct:.1f}%) filled")

if __name__ == "__main__":
    main()
    