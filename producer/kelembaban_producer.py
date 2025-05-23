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
        "kelembaban": random.randint(60, 80)
    }
    producer.send('sensor-kelembaban-gudang', value=data)
    print(f"[KELEMBABAN] Sent: {data}")
    time.sleep(1)
