import json
import time
import random
from datetime import datetime
# from azure.eventhub import EventHubProducerClient, EventData
from faker import Faker

fake = Faker("en_IN")   # Indian realistic names

# CONNECTION_STR = "EVENT_HUB_CONNECTION_STR"
# EVENT_HUB_NAME = "EVENT_HUB_NAME"

# producer = EventHubProducerClient.from_connection_string(
#     conn_str=CONNECTION_STR,
#     eventhub_name=EVENT_HUB_NAME
# )


STATE_CITY_MAP = {
    "UP": ["Lucknow", "Kanpur", "Varanasi", "Agra"],
    "MH": ["Mumbai", "Pune", "Nagpur"],
    "DL": ["New Delhi"],
    "KA": ["Bengaluru", "Mysuru"],
    "TN": ["Chennai", "Coimbatore"],
    "GJ": ["Ahmedabad", "Surat"],
    "WB": ["Kolkata"],
    "RJ": ["Jaipur", "Udaipur"]
}

state = random.choice(list(STATE_CITY_MAP.keys()))
city = random.choice(STATE_CITY_MAP[state])

def generate_sample_event(i):
    return {
        "event_id": f"EVT{i}",
        "event_ts": datetime.utcnow().isoformat(),

        "customer": {
            "customer_id": f"CUST{i}",
            "full_name": fake.name(),  #
            "date_of_birth": fake.date_of_birth(minimum_age=21, maximum_age=60).isoformat(),
            "gender": random.choice(["M", "F"]),
            "city": city,
            "state": state,
            "employment_type": random.choice(["SALARIED", "SELF_EMPLOYED"]),
            "annual_income": random.randint(300000, 1500000),
            "kyc_status": random.choice(["VERIFIED", "PENDING"])
        },

        "application": {
            "application_id": f"APP{i}",
            "channel": random.choice(["MOBILE", "WEB", "BRANCH"]),
            "loan_product": random.choice(["PERSONAL", "HOME", "AUTO"]),
            "requested_amount": random.randint(100000, 1000000),
            "tenure_months": random.choice([12, 24, 36]),
            "interest_rate": round(random.uniform(10.5, 15.5), 2),
            "application_status": "SUBMITTED"
        },

        "device": {
            "device_id": f"DEV{i}",
            "device_type": random.choice(["ANDROID", "IOS", "WEB"]),
            "os_version": random.choice(["13", "14", "15"]),
            "ip_address": fake.ipv4()
        }
    }

try:
    for i in range(1, 10):
        # batch = producer.create_batch()
        event = generate_sample_event(i)
        # batch.add(EventData(json.dumps(event)))
        # producer.send_batch(batch)
        print(event)
        print(f"Sent event {i}")
        time.sleep(1)

finally:
    # producer.close()
    print("Completed")