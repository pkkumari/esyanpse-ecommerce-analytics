"""
This script generates a backfill of realistic, historical e-commerce event data
and uploads it directly to a specified BigQuery table.

Key features of the simulation include:
- Generation of multiple historical days of data.
- Stateful user sessions with complex journeys (view, add to cart, purchase, return).
- Realistic seasonality patterns, including daily peaks (evenings) and weekend surges.
- Handles out-of-stock scenarios from the product catalog.
"""
import os
import csv
import json
import random
import datetime
import uuid
from google.cloud import bigquery
import google.auth

# --- Configuration ---
# Google Cloud settings
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = "ecommerce"
TABLE_ID = "live_events"

# Data generation settings
PRODUCTS_CSV_PATH = 'products.csv'
NUM_DAYS_HISTORY = 90
AVG_SESSIONS_PER_DAY = 250

# --- Initialization ---
product_catalog = []
# Explicitly get credentials set by 'gcloud auth application-default login'
credentials, project = google.auth.default()
bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)


# --- Helper Functions ---

def load_product_catalog(file_path):
    """Loads the product catalog from a CSV file into a global list."""
    global product_catalog
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            product_catalog = list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        exit()

def generate_user_session(session_timestamp):
    """
    Generates a list of events for a single complex user session.

    Args:
        session_timestamp (datetime): The starting timestamp for the session.

    Returns:
        list: A list of event dictionaries representing the user's journey.
    """
    events = []
    session_id = f"session-{uuid.uuid4()}"
    user_id = f"user-{random.randint(1, 250)}"
    source = random.choice(['google', 'facebook', 'email', 'direct'])
    cart = []
    purchased_items = []

    # A user views a random number of products (1 to 10) in a session.
    for i in range(random.randint(1, 10)):
        product = random.choice(product_catalog)
        # Stagger event timestamps slightly to simulate real-time behavior.
        event_ts = session_timestamp + datetime.timedelta(seconds=i * random.randint(10, 60))
        events.append(_create_event(session_id, user_id, source, 'product_view', product, event_ts))

        # User has a 40% chance of adding a viewed product to their cart.
        if random.random() < 0.40:
            if product.get('in_stock') == 'True':
                cart.append(product)
                events.append(_create_event(session_id, user_id, source, 'add_to_cart', product, event_ts))

    # If the cart is not empty, the user has a 30% chance to proceed to checkout.
    if cart and random.random() < 0.30:
        # The user may buy all, some, or just one of the items in their cart.
        items_to_buy_count = random.randint(1, len(cart))
        items_to_buy = random.sample(cart, k=items_to_buy_count)

        for item in items_to_buy:
            event_ts = session_timestamp + datetime.timedelta(minutes=random.randint(5, 15))
            purchased_items.append(item)
            events.append(_create_event(session_id, user_id, source, 'purchase', item, event_ts))

    # A user has an 8% chance of returning each item they purchased.
    for item in purchased_items:
        if random.random() < 0.08:
            # Returns happen a few days after the purchase.
            event_ts = session_timestamp + datetime.timedelta(days=random.randint(1, 5))
            events.append(_create_event(session_id, user_id, source, 'return_item', item, event_ts))

    return events

def _create_event(session_id, user_id, source, event_type, product, timestamp):
    """Helper function to construct a single event payload."""
    on_sale = random.random() < 0.25
    sale_price = round(float(product['regular_price']) * random.uniform(0.7, 0.9), 2) if on_sale else float(product['regular_price'])
    quantity = random.randint(1, 3) if event_type in ['purchase', 'add_to_cart'] else (-1 if event_type == 'return_item' else 1)

    event = {
        "event_id": f"evt-{uuid.uuid4()}",
        "event_timestamp": timestamp.isoformat(),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product['product_id'],
        "on_sale": on_sale,
        "sale_price": sale_price if event_type in ['add_to_cart', 'purchase', 'return_item'] else None,
        "quantity": quantity,
        "rating": random.randint(1, 5) if event_type == 'submit_review' else None,
        "source": source
    }
    # Clean the event payload by removing any keys with a None value.
    return {k: v for k, v in event.items() if v is not None}

def upload_to_bigquery(events_list):
    """Uploads a list of event dictionaries to the specified BigQuery table."""
    print(f"Uploading {len(events_list)} events to BigQuery...")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False  # Use the table's predefined schema.
    )

    table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
    load_job = bigquery_client.load_table_from_json(
        events_list, table_ref, job_config=job_config
    )

    load_job.result()  # Wait for the job to complete.
    print(f"Successfully loaded {load_job.output_rows} rows to {DATASET_ID}.{TABLE_ID}")


# --- Main Execution ---

if __name__ == "__main__":
    print("Generating historical event data with strong patterns...")
    load_product_catalog(PRODUCTS_CSV_PATH)

    all_historical_events = []
    today = datetime.datetime.now(datetime.timezone.utc)

    # Define weights for different hours to simulate peak traffic times.
    hours = list(range(24))
    # Weights are higher for afternoon and peak evening hours.
    weights = [1]*8 + [2]*4 + [4]*4 + [8]*6 + [2]*2

    # Loop backwards from today to generate historical data.
    for day in range(NUM_DAYS_HISTORY, 0, -1):
        current_day = today - datetime.timedelta(days=day)

        # Simulate a weekend surge with ~50% more traffic on Fridays and Saturdays.
        sessions_multiplier = 1.5 if current_day.weekday() >= 4 else 1.0
        num_sessions = random.randint(int(AVG_SESSIONS_PER_DAY * 0.7 * sessions_multiplier), int(AVG_SESSIONS_PER_DAY * 1.3 * sessions_multiplier))

        for _ in range(num_sessions):
            # Choose a session hour based on the defined weights.
            session_hour = random.choices(hours, weights=weights, k=1)[0]
            session_start_time = current_day.replace(
                hour=session_hour, minute=random.randint(0, 59), second=random.randint(0, 59)
            )
            all_historical_events.extend(generate_user_session(session_start_time))

    if all_historical_events:
        upload_to_bigquery(all_historical_events)
    else:
        print("No historical events were generated.")