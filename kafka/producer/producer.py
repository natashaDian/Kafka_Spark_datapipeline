from kafka import KafkaProducer
import json
import time

#define producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer =lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#
#producer harus kirim :
# 1. topicnya apa
# 2. keynya apa aja
# 3. valuenya apa

topic = "events_topic"
users = ["user_1", "user_2", "user_3"] #key

i=1
index=0

while True: #streaming data
    user = users[index % len(users)]    #sama kaya rumus hashing kafka = hash%partitiopn

    data = {
        "event_id" : i,
        "user" : user,
        "message" : "event generated"
    }

    producer.send(topic, key=user, value=data)
    print(f"sent -> key={user}, value={data}")

    i += 1
    index += 1
    time.sleep(5)

