import os
import requests
import pandas as pd
from sqlalchemy import create_engine
import logging
print("--- SCRIPT STARTED ---") # ADD THIS LINE
# --- Configuration ---
# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# The CORRECT API endpoint from your screenshot (everything before the '?')
API_URL = "https://services1.arcgis.com/ioennV6PpG5Xodq0/ArcGIS/rest/services/OpenData_A5/FeatureServer/1/query"

# Parameters for the API request based on your screenshot
API_PARAMS = {
    'where': '1=1',          # This means "get all records"
    'outFields': '*',        # Get all available fields
    'outSR': '4326',         # This was in your URL, so we include it
    'f': 'json'              # Specify the format as JSON
}

# --- Database Configuration ---
# Make sure your password here is correct
DB_USER = 'postgres'
DB_PASSWORD = 'BunnyBunn' # CHANGE THIS if you haven't already
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'fairfax_real_estate'
SCHEMA_NAME = 'raw'
TABLE_NAME = 'real_estate_sales'

def fetch_data_from_api(url, params):
    """Fetches data from the ArcGIS API using a POST request."""
    logging.info(f"Fetching data from {url} using POST request...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.post(url, data=params, headers=headers, timeout=300)
        response.raise_for_status()
        data = response.json()
        if 'error' in data:
            logging.error(f"API returned an error: {data['error']}")
            return None
        if 'features' not in data:
            logging.error("API response did not contain 'features' key.")
            return None
        records = [feature['attributes'] for feature in data['features']]
        logging.info(f"Successfully fetched {len(records)} records.")
        return records
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None
    
def create_dataframe(records):
    """Converts a list of records into a pandas DataFrame."""
    if not records:
        logging.warning("No records to process. Exiting.")
        return None
    logging.info("Converting records to pandas DataFrame...")
    df = pd.DataFrame(records)
    
    # Convert Unix timestamps (milliseconds) to datetime objects for relevant columns
    for col in ['SALEDATE', 'DEEDDATE']:
        if col in df.columns:
            # pd.to_datetime handles the conversion, 'ms' specifies the unit
            df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
    
    logging.info(f"DataFrame created with {df.shape[0]} rows and {df.shape[1]} columns.")
    return df

def load_data_to_postgres(df, db_conn_str, schema, table):
    """Loads a DataFrame into a PostgreSQL table."""
    if df is None:
        logging.error("DataFrame is None. Cannot load to database.")
        return

    logging.info(f"Preparing to load data into {schema}.{table}...")
    try:
        # Create a SQLAlchemy engine to connect to the database
        engine = create_engine(db_conn_str)
        
        # Use pandas' to_sql method to load the data
        # 'replace' will drop the table if it exists and create a new one.
        # This is useful for re-running the script to get fresh data.
        df.to_sql(table, engine, schema=schema, if_exists='replace', index=False)
        
        logging.info(f"Successfully loaded data into {schema}.{table}.")
    except Exception as e:
        logging.error(f"Failed to load data to PostgreSQL: {e}")


if __name__ == "__main__":
    logging.info("--- Starting Data Ingestion Pipeline ---")

    # 1. Fetch data from the API
    property_records = fetch_data_from_api(API_URL, API_PARAMS)

    if property_records:
        # 2. Convert data to a pandas DataFrame
        property_df = create_dataframe(property_records)

        # 3. Load the DataFrame into PostgreSQL
        if property_df is not None:
            # Construct the database connection string
            connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            load_data_to_postgres(property_df, connection_string, SCHEMA_NAME, TABLE_NAME)
    
    logging.info("--- Data Ingestion Pipeline Finished ---")
    print("--- SCRIPT FINISHED ---") # ADD THIS LINE