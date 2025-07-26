"""
This script generates a realistic product catalog CSV file for a simulated
e-commerce store similar to Best Buy.

Key features of the simulation include:
- A diverse set of product categories with realistic price ranges.
- A mix of product performance tiers (bestsellers, poorly-rated, average, new).
- Plausible, procedurally generated product names.
- A stock status for each product to enable out-of-stock simulations.
"""

import csv
import random
import uuid
from faker import Faker

# --- Configuration ---
NUM_PRODUCTS = 500
OUTPUT_FILE = 'products.csv'

# Defines product categories and their corresponding realistic price ranges.
CATEGORIES = {
    "Laptops": (600, 3500), "Desktop Computers": (800, 4000),
    "Monitors": (180, 2000), "PC Gaming Accessories": (40, 500),
    "Printers & Scanners": (80, 800), "Smartphones": (300, 1800),
    "Tablets": (200, 1500), "Smartwatches & Fitness Trackers": (150, 900),
    "Headphones": (30, 600), "Portable Bluetooth Speakers": (40, 450),
    "TVs": (250, 6000), "Sound Bars & Home Theater Audio": (150, 1500),
    "Streaming Media Players": (30, 200), "Digital Cameras": (400, 4500),
    "Drones": (300, 2500), "Video Game Consoles & VR": (200, 800),
    "Smart Home & Security": (30, 700), "Vacuums & Floor Care": (100, 1000)
}

# Defines different product performance archetypes to make the data more realistic.
# Format: (percentage_of_catalog, review_count_range, avg_rating_range)
PERFORMANCE_TIERS = {
    "bestseller": (0.10, (1000, 7500), (4.3, 4.9)),
    "bad_product": (0.05, (800, 4000), (2.5, 3.5)),
    "average": (0.65, (50, 800), (3.6, 4.6)),
    "new_or_niche": (0.20, (0, 50), (3.0, 4.9))
}

# Initialize the Faker library for generating realistic-sounding names.
fake = Faker()


# --- Helper Functions ---

def generate_product_name(category):
    """Generates a plausible product name based on its category."""
    brand = fake.company().split(' ')[0]
    model_suffix = random.choice(['Pro', 'Max', 'Ultra', 'SE', 'Series X', 'G'])
    model_number = f"{random.randint(100, 9000)}"

    if category in ["Laptops", "Smartphones", "Desktop Computers", "Tablets"]:
        return f"{brand} {model_suffix} {model_number}"
    elif category == "Headphones":
        return f"{brand} SoundCore {model_number}"
    elif category == "TVs":
        return f"{brand} Vision-Max {random.randint(40, 85)}-inch 4K TV"
    else:
        return f"{brand} {model_suffix} {random.choice(['System', 'Kit', 'Device'])}"

def generate_products():
    """Generates a list of all product data dictionaries."""
    products = []
    
    # Create a weighted list of performance tiers to pick from, ensuring the
    # distribution matches the percentages defined in PERFORMANCE_TIERS.
    tier_choices = []
    for tier, (percentage, _, _) in PERFORMANCE_TIERS.items():
        count = int(NUM_PRODUCTS * percentage)
        tier_choices.extend([tier] * count)
    
    # Fill any rounding errors with 'average' tier products.
    while len(tier_choices) < NUM_PRODUCTS:
        tier_choices.append("average")
    random.shuffle(tier_choices)

    for i in range(NUM_PRODUCTS):
        # Assign a performance tier to the current product.
        tier = tier_choices[i]
        _, review_range, rating_range = PERFORMANCE_TIERS[tier]
        
        category = random.choice(list(CATEGORIES.keys()))
        price_range = CATEGORIES[category]
        
        product = {
            "product_id": f"SKU-{category[:3].upper()}-{str(uuid.uuid4())[:4].upper()}",
            "product_name": generate_product_name(category),
            "category": category,
            "regular_price": round(random.uniform(*price_range), 2),
            "avg_rating": round(random.uniform(*rating_range), 1),
            "review_count": random.randint(*review_range),
            "in_stock": random.random() > 0.10  # 90% chance for a product to be in stock.
        }
        products.append(product)
        
    return products

def write_to_csv(products):
    """Writes the list of product dictionaries to a CSV file."""
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        header = products[0].keys()
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(products)
    print(f"Successfully created '{OUTPUT_FILE}' with {len(products)} products.")


# --- Main Execution ---

if __name__ == "__main__":
    all_products = generate_products()
    write_to_csv(all_products)