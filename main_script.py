# Import the function from your module
from snowflake_deployer import deploy_csvs_to_snowflake

# --- 1. Define your deployment parameters ---
SOURCE_DATA_DIR = 'data/new_reports'
TARGET_DATABASE = 'REPORTING_DB'
TARGET_SCHEMA = 'FINANCE'
SNOWFLAKE_CONNECTION = 'my_prod_connection' # Assumes a connection named this in your .toml file

# --- 2. Call the function to run the deployment ---
print(f"Starting deployment for {SOURCE_DATA_DIR}...")

# The function will handle all the logic and print its progress.
successful_run = deploy_csvs_to_snowflake(
    csv_directory=SOURCE_DATA_DIR,
    sf_database=TARGET_DATABASE,
    sf_schema=TARGET_SCHEMA,
    sf_connection_name=SNOWFLAKE_CONNECTION
)

# --- 3. Check the result ---
if successful_run:
    print("ETL process completed successfully.")
else:
    print("ETL process encountered an error.")
