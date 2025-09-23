from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import random
from datetime import datetime, timedelta

topicname = 'greentaxi'
BROKERS = 'boot-4vl3tyer.c1.kafka-serverless.us-east-1.amazonaws.com:9098'
region = 'us-east-1'

class MSKTokenProvider(AbstractTokenProvider):
    def __init__(self):
        super().__init__()
    
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token
    
    def extend_token(self):
        return self.token()
    
    def principal(self):
        return "msk-iam-user"

tp = MSKTokenProvider()

producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retry_backoff_ms=500,
    request_timeout_ms=20000,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
)

# NYC cities (boroughs) with short names
cities = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']

# Locations with short names
manhattan_locations = ['UES', 'UWS', 'Midtown', 'Downtown', 'Chelsea', 'Harlem']
brooklyn_locations = ['Williamsburg', 'Park Slope', 'DUMBO', 'Bk Heights', 'Bushwick']
queens_locations = ['Astoria', 'LIC', 'Flushing', 'J Heights']
bronx_locations = ['Yankees', 'Fordham', 'Pelham']
staten_island_locations = ['St George', 'Tottenville', 'New Dorp']

# Payment types with short names
payment_types = ['Credit', 'Cash', 'No charge', 'Dispute', 'Unknown']

# Rate codes with short names
rate_codes = ['Standard', 'JFK', 'Newark', 'Nassau', 'Negotiated', 'Group']

# Trip types with short names
trip_types = ['Street', 'Dispatch']

def get_pickup_location(city):
    if city == 'Manhattan':
        return random.choice(manhattan_locations)
    elif city == 'Brooklyn':
        return random.choice(brooklyn_locations)
    elif city == 'Queens':
        return random.choice(queens_locations)
    elif city == 'Bronx':
        return random.choice(bronx_locations)
    else:
        return random.choice(staten_island_locations)

def get_dropoff_location(pickup_city):
    # 70% chance dropoff in same city, 30% chance in different city
    if random.random() < 0.7:
        return get_pickup_location(pickup_city)
    else:
        return get_pickup_location(random.choice([c for c in cities if c != pickup_city]))

def generate_green_taxi_data(trip_id):
    # Base timestamp - random date in the last 30 days
    base_time = datetime.now() - timedelta(days=random.randint(1, 30))
    
    # Pickup details
    pickup_city = random.choice(cities)
    pickup_location = get_pickup_location(pickup_city)
    
    # Dropoff details
    dropoff_city = pickup_city if random.random() < 0.7 else random.choice([c for c in cities if c != pickup_city])
    dropoff_location = get_dropoff_location(dropoff_city)
    
    # Trip metrics
    trip_distance = round(random.uniform(0.5, 25.0), 2)
    fare_amount = round(trip_distance * 2.5 + random.uniform(2.0, 10.0), 2)
    tip_amount = round(fare_amount * random.uniform(0.1, 0.25), 2) if random.random() < 0.8 else 0.0
    tolls_amount = round(random.uniform(0.0, 10.0), 2) if random.random() < 0.3 else 0.0
    total_amount = fare_amount + tip_amount + tolls_amount
    
    # Generate timestamps
    pickup_time = base_time
    dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 90))
    
    taxi_data = {
        "trip_id": f"trip_{trip_id:04d}",
        "vendor_id": random.randint(1, 2),
        "pickup_datetime": pickup_time.strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_datetime": dropoff_time.strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_count": random.randint(1, 6),
        "trip_distance": trip_distance,
        "pickup_city": pickup_city,
        "pickup_location": pickup_location,
        "dropoff_city": dropoff_city,
        "dropoff_location": dropoff_location,
        "fare_amount": fare_amount,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "total_amount": total_amount,
        "payment_type": random.choice(payment_types),
        "rate_code": random.choice(rate_codes),
        "trip_type": random.choice(trip_types),
        "congestion_surcharge": round(random.uniform(0.0, 2.5), 2) if random.random() < 0.4 else 0.0,
        "airport_fee": round(random.uniform(0.0, 1.25), 2) if random.random() < 0.2 else 0.0,
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return taxi_data

# Send exactly 100 records
total_records = 100
sent_count = 0

print(f"Starting to send {total_records} NYC Green Taxi records...")

for i in range(total_records):
    data = generate_green_taxi_data(i + 1)
    
    try:
        future = producer.send(topicname, value=data)
        producer.flush()
        record_metadata = future.get(timeout=10)
        
        sent_count += 1
        print(f"✅ Sent record {sent_count}/{total_records}: Trip ID {data['trip_id']} - ${data['total_amount']} - {data['pickup_city']} to {data['dropoff_city']}")
        
    except Exception as e:
        print(f"❌ Error sending message: {e}")
        break

print(f"\nFinished! Successfully sent {sent_count} out of {total_records} records.")
producer.close()