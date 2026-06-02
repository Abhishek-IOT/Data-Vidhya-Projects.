import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()

NUM_CUSTOMERS = 100
NUM_SELLERS = 20
NUM_PRODUCTS = 50
NUM_ORDERS = 500
NUM_PRICE_UPDATES = 250
NUM_PROMOTIONS = 150

LOCATIONS = [
    ("New York", "NY", "USA"),
    ("Los Angeles", "CA", "USA"),
    ("Chicago", "IL", "USA"),
    ("Houston", "TX", "USA"),
    ("Seattle", "WA", "USA"),
    ("Boston", "MA", "USA"),
    ("Austin", "TX", "USA")
]



PRODUCT_CATALOG = {
    "Electronics": {
        "brands": ["Apple", "Samsung", "Sony"],
        "products": [
            ("iPhone 15", 999),
            ("Galaxy S24", 899),
            ("Sony Headphones", 199),
            ("Smart TV", 799)
        ]
    },
    "Sports": {
        "brands": ["Nike", "Adidas"],
        "products": [
            ("Running Shoes", 120),
            ("Football", 30),
            ("Gym Bag", 60)
        ]
    },
    "Home Appliances": {
        "brands": ["LG", "Whirlpool"],
        "products": [
            ("Air Purifier", 250),
            ("Microwave Oven", 180),
            ("Dishwasher", 700)
        ]
    }
}

def generate_customers():
    rows = []

    for i in range(1, NUM_CUSTOMERS + 1):

        city, state, country = random.choice(LOCATIONS)

        rows.append({
            "CUSTOMER_ID": f"CUST{i:05}",
            "CUSTOMER_NAME": fake.name(),
            "EMAIL": fake.email(),
            "PHONE": fake.msisdn()[:10],
            "CITY": city,
            "STATE": state,
            "COUNTRY": country,
            "REGISTERED_DATE":
                fake.date_time_between(
                    start_date="-3y",
                    end_date="now"
                ),
            "CUSTOMER_STATUS":
                random.choice(["ACTIVE","INACTIVE"]),
            "RECORD_SOURCE": "WEBSITE"
        })

    return pd.DataFrame(rows)


def generate_sellers():

    seller_names = [
        "BestBuy Marketplace",
        "Digital Hub",
        "Tech World",
        "Elite Sports",
        "Home Essentials"
    ]

    rows = []

    for i in range(1, NUM_SELLERS + 1):

        city, state, country = random.choice(LOCATIONS)

        rows.append({
            "SELLER_ID": f"SELL{i:03}",
            "SELLER_NAME":
                random.choice(seller_names) +
                f" {i}",
            "SELLER_RATING":
                round(random.uniform(4.0,5.0),2),
            "CITY": city,
            "COUNTRY": country,
            "RECORD_SOURCE": "MARKETPLACE"
        })

    return pd.DataFrame(rows)



def generate_products():

    rows = []

    for i in range(1, NUM_PRODUCTS + 1):

        category = random.choice(
            list(PRODUCT_CATALOG.keys())
        )

        product_name, base_price = random.choice(
            PRODUCT_CATALOG[category]["products"]
        )

        brand = random.choice(
            PRODUCT_CATALOG[category]["brands"]
        )

        rows.append({
            "PRODUCT_ID": f"PROD{i:04}",
            "PRODUCT_NAME": product_name,
            "CATEGORY": category,
            "BRAND": brand,
            "BASE_PRICE": base_price,
            "PRODUCT_STATUS": "ACTIVE",
            "RECORD_SOURCE": "ERP"
        })

    return pd.DataFrame(rows)



def generate_price_updates(products):

    rows = []

    for _ in range(NUM_PRICE_UPDATES):

        product = products.sample(1).iloc[0]

        discount = random.choice(
            [0,5,10,15,20]
        )

        price = float(product["BASE_PRICE"])

        final_price = round(
            price * (1-discount/100),2
        )

        rows.append({
            "PRODUCT_ID":
                product["PRODUCT_ID"],
            "PRICE": price,
            "DISCOUNT_PERCENT":
                discount,
            "FINAL_PRICE":
                final_price,
            "PRICE_UPDATE_TIME":
                fake.date_time_between(
                    start_date="-1y",
                    end_date="now"
                ),
            "RECORD_SOURCE":"ERP"
        })

    return pd.DataFrame(rows)


def generate_promotions(products):

    promotion_names = [
        "Summer Sale",
        "Black Friday",
        "Winter Discount",
        "Holiday Offer"
    ]

    rows = []

    for i in range(1, NUM_PROMOTIONS+1):

        product = products.sample(1).iloc[0]

        start = fake.date_time_between(
            start_date="-1y",
            end_date="now"
        )

        end = start + timedelta(days=30)

        rows.append({
            "PROMOTION_ID":
                f"PROM{i:04}",
            "PRODUCT_ID":
                product["PRODUCT_ID"],
            "PROMOTION_NAME":
                random.choice(
                    promotion_names
                ),
            "DISCOUNT_PERCENT":
                random.choice(
                    [5,10,15,20]
                ),
            "START_DATE": start,
            "END_DATE": end,
            "RECORD_SOURCE":"CRM"
        })

    return pd.DataFrame(rows)



def generate_orders(customers, products, sellers):

    orders = []
    order_items = []

    for i in range(1, NUM_ORDERS+1):

        order_id = f"ORD{i:06}"

        customer = customers.sample(1).iloc[0]

        num_items = random.randint(1,5)

        order_total = 0

        for _ in range(num_items):

            product = products.sample(1).iloc[0]
            seller = sellers.sample(1).iloc[0]

            qty = random.randint(1,3)

            unit_price = float(
                product["BASE_PRICE"]
            )

            total_price = round(
                qty * unit_price,
                2
            )

            order_total += total_price

            order_items.append({
                "ORDER_ID": order_id,
                "PRODUCT_ID":
                    product["PRODUCT_ID"],
                "SELLER_ID":
                    seller["SELLER_ID"],
                "QUANTITY": qty,
                "UNIT_PRICE":
                    unit_price,
                "TOTAL_PRICE":
                    total_price,
                "RECORD_SOURCE":
                    "WEBSITE"
            })

        orders.append({
            "ORDER_ID": order_id,
            "CUSTOMER_ID":
                customer["CUSTOMER_ID"],
            "ORDER_DATE":
                fake.date_time_between(
                    start_date="-1y",
                    end_date="now"
                ),
            "ORDER_STATUS":
                random.choices(
                    [
                        "DELIVERED",
                        "SHIPPED",
                        "PLACED",
                        "CANCELLED"
                    ],
                    weights=[
                        60,
                        20,
                        15,
                        5
                    ]
                )[0],
            "TOTAL_AMOUNT":
                round(order_total,2),
            "RECORD_SOURCE":
                "WEBSITE"
        })

    return (
        pd.DataFrame(orders),
        pd.DataFrame(order_items)
    )


customers = generate_customers()
sellers = generate_sellers()
products = generate_products()

price_updates = generate_price_updates(products)
promotions = generate_promotions(products)

orders, order_items = generate_orders(
    customers,
    products,
    sellers
)

customers.to_csv(
    "customers.csv",
    index=False
)

orders.to_csv(
    "orders.csv",
    index=False
)

promotions.to_csv(
    "promotions.csv",
    index=False
)

products.to_json(
    "products.json",
    orient="records",
    indent=4
)

sellers.to_json(
    "sellers.json",
    orient="records",
    indent=4
)

price_updates.to_parquet(
    "price_updates.parquet",
    index=False
)

order_items.to_parquet(
    "order_items.parquet",
    index=False
)

print("Files generated successfully.")