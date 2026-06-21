
import random
from datetime import datetime, timedelta, timezone
import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)

NUM_CUSTOMERS = 100
NUM_SELLERS = 20
NUM_PRODUCTS = 50
NUM_ORDERS = 500

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
        "products": [("iPhone 15",999),("Galaxy S24",899),("Sony Headphones",199),("Smart TV",799)]
    },
    "Sports": {
        "brands": ["Nike","Adidas"],
        "products": [("Running Shoes",120),("Football",30),("Gym Bag",60)]
    },
    "Home Appliances": {
        "brands": ["LG","Whirlpool"],
        "products": [("Air Purifier",250),("Microwave Oven",180),("Dishwasher",700)]
    }
}

def generate_id(prefix,n):
    return f"{prefix}{n:08d}"

def generate_customers():
    rows=[]
    for i in range(1,NUM_CUSTOMERS+1):
        city,state,country=random.choice(LOCATIONS)
        rows.append({
            "CUSTOMER_ID":generate_id("CUST",i),
            "CUSTOMER_NAME":fake.name(),
            "EMAIL":fake.email(),
            "PHONE":fake.msisdn()[:10],
            "CITY":city,
            "STATE":state,
            "COUNTRY":country,
            "REGISTERED_DATE":fake.date_time_between(start_date="-3y",end_date="now"),
            "CUSTOMER_STATUS":random.choice(["ACTIVE","INACTIVE"]),
            "RECORD_SOURCE":"WEBSITE"
        })
    return pd.DataFrame(rows)

def generate_sellers():
    names=["BestBuy Marketplace","Digital Hub","Tech World","Elite Sports","Home Essentials"]
    rows=[]
    for i in range(1,NUM_SELLERS+1):
        city,state,country=random.choice(LOCATIONS)
        rows.append({
            "SELLER_ID":generate_id("SELL",i),
            "SELLER_NAME":f"{random.choice(names)} {i}",
            "SELLER_RATING":round(random.uniform(3.5,5.0),2),
            "CITY":city,
            "COUNTRY":country,
            "RECORD_SOURCE":"MARKETPLACE"
        })
    return pd.DataFrame(rows)

def generate_products():
    unique_products=[]
    pid=1
    for category,data in PRODUCT_CATALOG.items():
        for pname,price in data["products"]:
            unique_products.append({
                "PRODUCT_ID":generate_id("PROD",pid),
                "PRODUCT_NAME":pname,
                "CATEGORY":category,
                "BRAND":random.choice(data["brands"]),
                "BASE_PRICE":price,
                "PRODUCT_STATUS":"ACTIVE",
                "RECORD_SOURCE":"ERP"
            })
            pid+=1

    while len(unique_products) < NUM_PRODUCTS:
        template=random.choice(unique_products[:])
        unique_products.append({
            **template,
            "PRODUCT_ID":generate_id("PROD",pid),
            "PRODUCT_NAME":f"{template['PRODUCT_NAME']} Variant {pid}"
        })
        pid+=1

    return pd.DataFrame(unique_products)

def create_product_seller_map(products,sellers):
    mapping=[]
    seller_ids=sellers["SELLER_ID"].tolist()
    for product_id in products["PRODUCT_ID"]:
        assigned=random.sample(seller_ids, random.randint(2,5))
        for seller_id in assigned:
            mapping.append({
                "PRODUCT_ID":product_id,
                "SELLER_ID":seller_id
            })
    return pd.DataFrame(mapping)

def generate_promotions(products):
    rows=[]
    names=["Summer Sale","Black Friday","Winter Discount","Holiday Offer"]
    for i, product in enumerate(products.itertuples(), start=1):
        discount=random.choice([0,5,10,15,20])
        start=fake.date_time_between(start_date="-1y", end_date="-30d")
        rows.append({
            "PROMOTION_ID":generate_id("PROM",i),
            "PRODUCT_ID":product.PRODUCT_ID,
            "PROMOTION_NAME":random.choice(names),
            "DISCOUNT_PERCENT":discount,
            "START_DATE":start,
            "END_DATE":start + timedelta(days=30),
            "RECORD_SOURCE":"CRM"
        })
    return pd.DataFrame(rows)

def generate_price_updates(products,promotions):
    rows=[]
    promo_lookup=promotions.set_index("PRODUCT_ID")["DISCOUNT_PERCENT"].to_dict()

    for product in products.itertuples():
        discount=promo_lookup.get(product.PRODUCT_ID,0)
        final_price=round(product.BASE_PRICE*(1-discount/100),2)

        rows.append({
            "PRODUCT_ID":product.PRODUCT_ID,
            "PRICE":product.BASE_PRICE,
            "DISCOUNT_PERCENT":discount,
            "FINAL_PRICE":final_price,
            "PRICE_UPDATE_TIME":datetime.now(timezone.utc),
            "RECORD_SOURCE":"ERP"
        })

    return pd.DataFrame(rows)

def generate_orders(customers,products,sellers_map,price_updates):
    orders=[]
    order_items=[]

    price_lookup=price_updates.set_index("PRODUCT_ID").to_dict("index")

    grouped={}
    for row in sellers_map.itertuples():
        grouped.setdefault(row.PRODUCT_ID,[]).append(row.SELLER_ID)

    for i in range(1,NUM_ORDERS+1):

        order_id=generate_id("ORD",i)
        customer=customers.sample(1).iloc[0]
        order_total=0

        for _ in range(random.randint(1,5)):
            product=products.sample(1).iloc[0]

            seller_id=random.choice(grouped[product["PRODUCT_ID"]])

            price_info=price_lookup[product["PRODUCT_ID"]]

            qty=random.randint(1,3)
            unit_price=float(price_info["FINAL_PRICE"])
            total_price=round(unit_price*qty,2)

            order_total += total_price

            order_items.append({
                "ORDER_ID":order_id,
                "PRODUCT_ID":product["PRODUCT_ID"],
                "SELLER_ID":seller_id,
                "QUANTITY":qty,
                "UNIT_PRICE":unit_price,
                "TOTAL_PRICE":total_price,
                "RECORD_SOURCE":"WEBSITE"
            })

        orders.append({
            "ORDER_ID":order_id,
            "CUSTOMER_ID":customer["CUSTOMER_ID"],
            "ORDER_DATE":fake.date_time_between(start_date="-1y", end_date="now"),
            "ORDER_STATUS":random.choices(
                ["DELIVERED","SHIPPED","PLACED","CANCELLED"],
                weights=[60,20,15,5]
            )[0],
            "TOTAL_AMOUNT":round(order_total,2),
            "RECORD_SOURCE":"WEBSITE"
        })

    return pd.DataFrame(orders), pd.DataFrame(order_items)

customers=generate_customers()
sellers=generate_sellers()
products=generate_products()

product_seller_map=create_product_seller_map(products,sellers)

promotions=generate_promotions(products)
price_updates=generate_price_updates(products,promotions)

orders,order_items=generate_orders(
    customers,
    products,
    product_seller_map,
    price_updates
)

customers.to_csv("customers.csv",index=False)
orders.to_csv("orders.csv",index=False)
promotions.to_csv("promotions.csv",index=False)

products.to_json("products.json",orient="records",indent=4)
sellers.to_json("sellers.json",orient="records",indent=4)

price_updates.to_csv("price_updates.csv",index=False)
order_items.to_parquet("order_items.parquet",index=False)

print("Files generated successfully.")

# Validation checks
assert all(
    round(r["UNIT_PRICE"] * r["QUANTITY"],2) == round(r["TOTAL_PRICE"],2)
    for _,r in order_items.iterrows()
)

print("Validation passed.")
