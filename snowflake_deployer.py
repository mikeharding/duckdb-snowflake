import os
import glob
import duckdb
from snowflake.snowpark import Session

def deploy_csvs_to_snowflake(
    csv_directory: str,
    sf_database: str,
    sf_schema: str,
    sf_connection_name: str = "cursor-pat"
) -> bool:
    """
    Loads all CSV files from a directory into an in-memory DuckDB, then deploys
    each as a table to a target Snowflake database and schema using Arrow tables
    for efficient transfer.

    Args:
        csv_directory (str): The path to the directory containing CSV files.
        sf_database (str): The target Snowflake database name.
        sf_schema (str): The target Snowflake schema name.
        sf_connection_name (str): The name of the connection defined in your
                                  ~/.snowflake/connections.toml file.

    Returns:
        bool: True if the deployment was successful, False otherwise.
    """
    print("🚀 Starting the data deployment process...")
    
    session = None
    con = None
    
    try:
        # --- Step 1: Connect to Snowflake ---
        session = Session.builder.config("connection_name", sf_connection_name).create()
        print("✅ Connection to Snowflake successful!")
        session.use_database(sf_database)
        session.use_schema(sf_schema)
        print(f"   -> Using Snowflake Context: DB: '{sf_database}', Schema: '{sf_schema}'")

        # --- Step 2: Load CSV files into an in-memory DuckDB database ---
        con = duckdb.connect(database=':memory:', read_only=False)
        print("🦆 Successfully connected to in-memory DuckDB.")

        csv_files = glob.glob(os.path.join(csv_directory, '*.csv'))
        if not csv_files:
            print(f"⚠️ No CSV files found in the '{csv_directory}' directory. Exiting.")
            return False

        print(f"Found {len(csv_files)} CSV files to process.")
        for csv_file in csv_files:
            table_name = os.path.basename(csv_file).replace('.csv', '').upper()
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}')"
            con.execute(query)
            print(f"   -> Successfully loaded '{csv_file}' into DuckDB table '{table_name}'.")

        # --- Step 3: Get list of tables and deploy to Snowflake using Arrow ---
        duckdb_tables_result = con.execute("SHOW TABLES").fetchall()
        duckdb_tables = [table[0] for table in duckdb_tables_result]
        print(f"\nFound tables in DuckDB to deploy: {duckdb_tables}")

        for table_name in duckdb_tables:
            print(f"   -> Processing table '{table_name}' for deployment...")
            arrow_table = con.table(table_name).to_arrow_table()
            snowpark_df = session.create_dataframe(arrow_table)
            snowpark_df.write.mode("overwrite").save_as_table(table_name)
            print(f"      ✅ Successfully deployed table '{table_name}' to Snowflake.")

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return False
        
    finally:
        # --- Step 4: Clean up connections ---
        if session:
            session.close()
            print("\n❄️  Snowflake connection closed.")
        if con:
            con.close()
            print("🦆 DuckDB connection closed.")
    
    print("\n🎉 Process finished successfully.")
    return True

# This block allows the script to be run directly for testing purposes.
# It will NOT run when the module is imported into another script.
if __name__ == '__main__':
    print("--- Running module in test mode ---")
    
    # --- Configuration for Direct Execution ---
    TEST_CSV_DIR = 'data'
    TEST_SF_DATABASE = 'UTILITY_ANALYTICS'
    TEST_SF_SCHEMA = 'CALL_CENTER'
    
    # Check if the test directory exists
    if not os.path.isdir(TEST_CSV_DIR):
        print(f"🔥 Error: Test directory '{TEST_CSV_DIR}' not found.")
        print("Please create it and add your CSV files to run the test.")
    else:
        # Call the main function
        success = deploy_csvs_to_snowflake(
            csv_directory=TEST_CSV_DIR,
            sf_database=TEST_SF_DATABASE,
            sf_schema=TEST_SF_SCHEMA
        )

        if success:
            print("\n--- Test run completed successfully! ---")
        else:
            print("\n--- Test run failed. Please check the errors above. ---")
