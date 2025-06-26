import os
import requests
import pandas as pd
from sqlalchemy import create_engine
import logging
print("--- SCRIPT STARTED ---")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
API_URL = "https://services1.arcgis.com/ioennV6PpG5Xodq0/ArcGIS/rest/services/OpenData_A5/FeatureServer/1/query"
API_PARAMS = {
'where': '1=1',
'outFields': '*',
'outSR': '4326',
'f': 'json'
}
DB_USER = 'postgres'
DB_PASSWORD = 'BunnyBunn'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'fairfax_real_estate'
SCHEMA_NAME = 'raw'
TABLE_NAME = 'real_estate_sales'
def fetch_data_from_api(url, params):
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
if not records:
logging.warning("No records to process. Exiting.")
return None
logging.info("Converting records to pandas DataFrame...")
df = pd.DataFrame(records)
for col in ['SALEDATE', 'DEEDDATE']:
if col in df.columns:
df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
logging.info(f"DataFrame created with {df.shape[0]} rows and {df.shape[1]} columns.")
return df
def load_data_to_postgres(df, db_conn_str, schema, table):
if df is None:
logging.error("DataFrame is None. Cannot load to database.")
return
logging.info(f"Preparing to load data into {schema}.{table}...")
try:
engine = create_engine(db_conn_str)
df.to_sql(table, engine, schema=schema, if_exists='replace', index=False)
logging.info(f"Successfully loaded data into {schema}.{table}.")
except Exception as e:
logging.error(f"Failed to load data to PostgreSQL: {e}")
if __name__ == "__main__":
logging.info("--- Starting Data Ingestion Pipeline ---")
property_records = fetch_data_from_api(API_URL, API_PARAMS)
if property_records:
property_df = create_dataframe(property_records)
if property_df is not None:
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
load_data_to_postgres(property_df, connection_string, SCHEMA_NAME, TABLE_NAME)
logging.info("--- Data Ingestion Pipeline Finished ---")
print("--- SCRIPT FINISHED ---")
