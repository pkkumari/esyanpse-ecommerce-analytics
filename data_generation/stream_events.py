"""
This script generates a continuous, realistic stream of e-commerce events and
publishes them to a Google Cloud Pub/Sub topic.

Key features of the simulation include:
- Stateful user sessions with complex journeys (view, add to cart, purchase, return).
- Simulation of repeat users who purchase items from previous sessions.
- Realistic seasonality patterns, including daily peaks and off-peak hours.
- Handles out-of-stock scenarios from the product catalog.
"""
import os
import csv
import json
import random
import time
import datetime
import uuid
from google.cloud import pubsub_v1
from collections import deque

# --- Configuration ---
# Google Cloud settings for publishing events.
PRODUCTS_CSV_PATH = 'products.csv'
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = "ecommerce-events"

# --- Simulation Behavior ---
# These probabilities control the behavior of the simulated users.
PROB_ADD_TO_CART = 0.40
PROB_CHECKOUT = 0.30
PROB_RETURN_ITEM = 0.08
PROB_REPEAT_USER = 0.25 # 25% of sessions are from repeat users

# --- Script Initialization ---
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
product_catalog = []
# Use a deque to store the last 100 users who abandoned their carts.
# This serves as a memory for simulating repeat user purchases.
recent_user_activity = deque(maxlen=100)


# --- Helper Functions ---

def load_product_catalog(file_path):
    """Loads the product catalog from a CSV file into a global list."""
    global product_catalog
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            product_catalog = list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"Error: '{file_path}' not found."); exit()

def get_current_sleep_time():
    """Varies the script's sleep time to simulate seasonality (peak/off-peak hours)."""
    now = datetime.datetime.now()
    # During peak hours (e.g., 6 PM to 10 PM), generate events more frequently.
    if 18 <= now.hour <= 22:
        return random.uniform(0.1, 0.5)
    # During off-peak hours, generate events less frequently.
    else:
        return random.uniform(0.8, 2.5)

def generate_user_session():
    """
    Generates a list of events representing one complete, complex user session.
    Returns:
        list: A list of event dictionaries for the session.
    """
    events = []
    session_id = f"session-{uuid.uuid4()}"
    source = random.choice(['google', 'facebook', 'email', 'direct', 'organic_search'])
    cart = []
    purchased_items = []

    # There is a chance that this session is from a returning user.
    if recent_user_activity and random.random() < PROB_REPEAT_USER:
        user_id, prev_product = random.choice(recent_user_activity)
        # Repeat users have a high chance of purchasing an item they previously abandoned.
        if random.random() < 0.50:
            events.append(_create_event(session_id, user_id, source, 'purchase', prev_product))
            return events # This is a short session focused on a repeat purchase.
    else:
        # This is a new user session.
        user_id = f"user-{random.randint(1, 500)}"

    # A user views a random number of products.
    for _ in range(random.randint(1, 10)):
        product = random.choice(product_catalog)
        events.append(_create_event(session_id, user_id, source, 'product_view', product))

        # The user has a chance to add the product to their cart.
        if random.random() < PROB_ADD_TO_CART:
            if product.get('in_stock') == 'True':
                cart.append(product)
                events.append(_create_event(session_id, user_id, source, 'add_to_cart', product))
            else:
                # Simulate the "out-of-stock" edge case.
                events.append(_create_event(session_id, user_id, source, 'add_to_cart_failure', product))

    if cart:
        # If the user abandons the cart, store their ID and a product for a potential future repeat session.
        recent_user_activity.append((user_id, random.choice(cart)))

        # The user has a chance to proceed to checkout.
        if random.random() < PROB_CHECKOUT:
            items_to_buy_count = random.randint(1, len(cart))
            items_to_buy = random.sample(cart, k=items_to_buy_count)
            for item in items_to_buy:
                purchased_items.append(item)
                events.append(_create_event(session_id, user_id, source, 'purchase', item))

    # After a purchase, the user has a chance to leave a review.
    if purchased_items and random.random() < 0.25:
        item_to_review = random.choice(purchased_items)
        events.append(_create_event(session_id, user_id, source, 'submit_review', item_to_review))

    # Each purchased item has a small chance of being returned.
    for item in purchased_items:
        if random.random() < PROB_RETURN_ITEM:
            events.append(_create_event(session_id, user_id, source, 'return_item', item))
            
    return events

def _create_event(session_id, user_id, source, event_type, product):
    """Helper function to construct a single event payload."""
    on_sale = random.random() < 0.25
    sale_price = round(float(product['regular_price']) * random.uniform(0.7, 0.9), 2) if on_sale else float(product['regular_price'])
    
    event = {
        "event_id": f"evt-{uuid.uuid4()}",
        "event_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product['product_id'],
        "on_sale": on_sale,
        "sale_price": sale_price if event_type in ['add_to_cart', 'purchase', 'return_item'] else None,
        "quantity": -1 if event_type == 'return_item' else 1,
        "rating": random.randint(1, 5) if event_type == 'submit_review' else None,
        "source": source
    }
    # Clean the event payload by removing any keys with a None value.
    return {k: v for k, v in event.items() if v is not None}

def publish_event(event_data):
    """Publishes a single event to the configured Pub/Sub topic."""
    data = json.dumps(event_data).encode("utf-8")
    publisher.publish(topic_path, data)


# --- Main Execution Block ---

if __name__ == "__main__":
    print("Starting the final realistic e-commerce event stream...")
    print("Press Ctrl+C to stop.")
    
    load_product_catalog(PRODUCTS_CSV_PATH)
    if not product_catalog:
        exit()
        
    # Run in an infinite loop to continuously generate new user sessions.
    while True:
        try:
            session_events = generate_user_session()
            if session_events:
                print(f"--- PUBLISHING NEW SESSION: {session_events[0]['session_id']} ({len(session_events)} events) ---")
                for event in session_events:
                    publish_event(event)
                    # The delay between events is varied to simulate peak/off-peak hours.
                    time.sleep(get_current_sleep_time())
        except KeyboardInterrupt:
            print("\n🛑 Stream stopped by user.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(5)