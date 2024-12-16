import psycopg2
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_validation_queries(redshift_host, redshift_port, redshift_db, redshift_user, redshift_password):
    """
    Runs the SQL validation queries against the Amazon Redshift database.

    Args:
        redshift_host (str): Redshift host address.
        redshift_port (int): Redshift port number.
        redshift_db (str): Redshift database name.
        redshift_user (str): Redshift database username.
        redshift_password (str): Redshift database password.

    """
    try:
        logging.info("Starting validation queries against Redshift...")
        # Connect to Redshift
        conn = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            database=redshift_db,
            user=redshift_user,
            password=redshift_password
        )
        cursor = conn.cursor()
        logging.info("Successfully connected to Redshift for validation.")


        # Read queries from the validation_queries.sql file
        with open(os.path.join("sql", "validation_queries.sql"), "r") as f:
            validation_queries = f.read().split(';')

         # Execute queries one by one, ignoring the last empty string
        for query in validation_queries[:-1]:
            try:
               cursor.execute(query)
               logging.info(f"Successfully executed query:\n{query}\n")
               results = cursor.fetchall()
               if results:
                  logging.info("Results:\n")
                  for row in results:
                    logging.info(row)
               else:
                  logging.info("No Results")
            except psycopg2.Error as e:
                logging.error(f"Error executing query:\n{query}\nError: {e}")
        logging.info("Finished validation queries")
        # Close database cursor and connection
        cursor.close()
        conn.close()
        logging.info("Closed connection to Redshift")

    except Exception as e:
        logging.error(f"Error while running validation queries: {e}")
        raise

if __name__ == '__main__':
    # Retrieve environment variables from the .env file
    REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
    REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
    REDSHIFT_DB = os.getenv("REDSHIFT_DB")
    REDSHIFT_USER = os.getenv("REDSHIFT_USER")
    REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

    run_validation_queries(REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD)
    logging.info("Validation queries completed")