from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

producer = KafkaProducer(
    bootstrap_servers= "localhost:9092",
    api_version=(3, 5, 0),
    key_serializer =lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


topic = 'transactions'

users = ['U12345', 'U11111', 'U35342', 'U23456']
sources_valid = ["mobile", "web", "pos"]
sources_invalid = ["tablet", "unknown", "iot"]

event_id = 1
sent_events = set()

def iso_time(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

while True:
    user = random.choice(users)
    now = datetime.utcnow()

    #----Normal Events----
    event = {
        "event_id": event_id,
        "user_id": user,
        "amount": random.randint(10_000, 300_000),
        "timestamp": iso_time(now),
        "source": random.choice(sources_valid)
    }

    #----Invalid Events----
    if event_id == 3:
        event["amount"] = -50_000  # Invalid negative amount
    elif event_id == 10:
        event["timestamp"] = "INVALID"
    elif event_id == 11:
        event['source'] = random.choice(sources_invalid)
    
    #duplicate
    if event_id == 13:
        event['event_id'] = 2

    #late event
    if event_id in [7, 8, 9]:
        late_time = now - timedelta(minutes=5)
        event["timestamp"] = iso_time(late_time)
    
    producer.send(topic, key=user, value=event)

    print(f"Sent event: {event}")

    event_id += 1
    time.sleep(random.uniform(1, 2))  # 1–2 detik




