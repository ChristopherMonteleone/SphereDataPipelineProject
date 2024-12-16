import pandas as pd
import psycopg2
from psycopg2 import sql
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_data(csv_path, redshift_host, redshift_port, redshift_db, redshift_user, redshift_password):
    """
    Loads data from a CSV file into an Amazon Redshift table.

    Args:
        csv_path (str): Path to the CSV file.
        redshift_host (str): Redshift host address.
        redshift_port (int): Redshift port number.
        redshift_db (str): Redshift database name.
        redshift_user (str): Redshift database username.
        redshift_password (str): Redshift database password.

    Raises:
        Exception: If an error occurs during the loading process.
    """

    try:
        logging.info(f"Loading data from {csv_path} into Redshift...")

        # Create a Redshift Connection object and connect to the database.
        logging.info(f"Connecting to redshift using: \nHOST: {redshift_host} \nPORT: {redshift_port} \nDATABASE: {redshift_db} \nUSER: {redshift_user} \nPassword: {redshift_password}")
        conn = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            database=redshift_db,
            user=redshift_user,
            password=redshift_password
        )
        conn.autocommit = True  # Set autocommit to True
        cursor = conn.cursor()
        logging.info("Connected to Redshift successfully")

        # Read the data from the CSV file using Pandas
        df = pd.read_csv(csv_path)
        logging.info("Data loaded from CSV using pandas")

        # Data Cleaning and Data Validation Logic
        df = clean_data(df)
        logging.info("Data cleaning and validation successful.")

        # Convert timestamp to string to ensure that it can be parsed correctly by redshift
        df['transaction_date'] = df['transaction_date'].astype(str)

        # Create the table if it doesn't exist in redshift
        create_table_sql = open(os.path.join("sql", "create_table.sql"), "r").read()
        cursor.execute(create_table_sql)
        logging.info("Created Redshift table if not already existing")

         # Prepare the INSERT statement dynamically using psycopg2's SQL library
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = sql.SQL("INSERT INTO sphere_revenue ({}) VALUES ({})").format(
             sql.SQL(columns), sql.SQL(placeholders)
        )
       

        # Execute the INSERT statement for each row
        for row in df.itertuples(index=False):
            try:
                cursor.execute(insert_sql, list(row))
            except psycopg2.Error as e:
                 logging.error(f"Failed to execute query on row: {row}. Error: {e}")


        # Close the database cursor and connection
        cursor.close()
        conn.close()
        logging.info("Redshift data load process complete")


    except Exception as e:
        logging.error(f"Error during data loading: {e}")
        raise


def clean_data(df):
    """
    Clean and validate the data.
    Args:
      df(pd.DataFrame): The data to be cleaned

    Returns:
      df(pd.DataFrame): The cleaned data.
    """
    # Data cleaning steps
    logging.info("Beginning data cleaning process...")

    #Handle missing data. Fill missing values in 'payment_method' with 'unknown'
    df['payment_method'].fillna('unknown', inplace=True)

    #Convert necessary columns to correct type
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')


    #Data validation steps
    logging.info("Beginning data validation process...")
    # Check that price and quantity are never negative
    invalid_prices = df[df['price'] < 0]
    if not invalid_prices.empty:
        logging.warning(f"Found invalid prices: \n{invalid_prices}")
        df = df[df['price'] >= 0]  # Remove rows where price is negative

    invalid_quantities = df[df['quantity'] < 0]
    if not invalid_quantities.empty:
         logging.warning(f"Found invalid quantities: \n{invalid_quantities}")
         df = df[df['quantity'] >= 0] # Remove rows where quantity is negative

    #Ensure revenue is consistent with price * quantity.
    df['calculated_revenue'] = df['price'] * df['quantity']
    revenue_mismatch = df[abs(df['revenue'] - df['calculated_revenue']) > 0.01]  # Allow for small floating-point differences

    if not revenue_mismatch.empty:
        logging.warning(f"Found revenue mismatches: \n{revenue_mismatch}")
        df['revenue'] = df['calculated_revenue'] # Correct revenues that don't match
    df.drop('calculated_revenue', axis=1, inplace=True)

    logging.info("Data validation process complete.")
    return df

if __name__ == '__main__':
    csv_file = os.path.join("data", "revenue_data.csv")

    # Get credentials from the .env file
    REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
    REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
    REDSHIFT_DB = os.getenv("REDSHIFT_DB")
    REDSHIFT_USER = os.getenv("REDSHIFT_USER")
    REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

    load_data(csv_file, REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD)
    logging.info("ETL process completed")