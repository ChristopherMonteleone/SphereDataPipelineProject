import pandas as pd
from faker import Faker
import random
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_revenue_data(num_records=1000):
    """
    Generates realistic revenue data using Faker and returns it as a Pandas DataFrame.
    """
    fake = Faker()
    data = []
    venues = ["Sphere Las Vegas", "MSG Network"]
    event_names = ["U2 Concert", "Knicks Game", "Rangers Game", "Movie Night", "Comedy Show"]
    ticket_types = ["General Admission", "VIP", "Premium", "Child", "Student"]
    payment_methods = ["Credit Card", "Debit Card", "Cash", "Gift Card"]

    logging.info("Starting revenue data generation...")

    for _ in range(num_records):
        transaction_id = fake.uuid4()
        transaction_date = fake.date_time_between(start_date='-1y', end_date='now')
        venue = random.choice(venues)
        event_name = random.choice(event_names)
        ticket_type = random.choice(ticket_types)
        quantity = random.randint(1, 5)
        price = round(random.uniform(20, 200), 2)  # Price with 2 decimal places
        revenue = round(quantity * price, 2)
        customer_id = fake.uuid4()
        payment_method = random.choice(payment_methods)


        data.append({
            'transaction_id': transaction_id,
            'transaction_date': transaction_date,
            'venue': venue,
            'event_name': event_name,
            'ticket_type': ticket_type,
            'quantity': quantity,
            'price': price,
            'revenue': revenue,
            'customer_id': customer_id,
            'payment_method': payment_method
        })

    logging.info("Finished revenue data generation.")
    return pd.DataFrame(data)


def save_revenue_data(df, filename="revenue_data.csv"):
    """Saves the Pandas DataFrame to a CSV file."""
    try:
        logging.info(f"Saving revenue data to {filename}...")
        df.to_csv(os.path.join("data", filename), index=False)
        logging.info(f"Revenue data saved successfully to {filename}.")
    except Exception as e:
        logging.error(f"Error saving revenue data: {e}")
        raise


if __name__ == '__main__':
    num_records_to_generate = int(os.getenv("NUM_RECORDS_TO_GENERATE", 10000))
    df = generate_revenue_data(num_records_to_generate)
    save_revenue_data(df)
    logging.info("Data generation completed.")