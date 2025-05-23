from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

gudangs = ['G1', 'G2', 'G3']

while True:
    data = {
        "gudang_id": random.choice(gudangs),
        "suhu": random.randint(70, 90)
    }
    producer.send('sensor-suhu-gudang', value=data)
    print(f"[SUHU] Sent: {data}")
    time.sleep(1)
