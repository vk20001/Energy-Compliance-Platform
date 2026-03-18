from kafka import KafkaConsumer, KafkaProducer
import ssl

TOPICS = [
    "energy.public.facilities",
    "energy.public.meter_readings", 
    "energy.public.investments"
]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers="localhost:29092",
    auto_offset_reset="earliest",
    group_id="redpanda-bridge",
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers="d6sinugbmgg6innos300.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="energy-compliance-user",
    sasl_plain_password="energypass123",
    ssl_context=ssl.create_default_context()
)

print("Starting bridge...")
for msg in consumer:
    producer.send(msg.topic, key=msg.key, value=msg.value)
    if msg.offset % 1000 == 0:
        print(f"Forwarded {msg.topic}:{msg.offset}")
        producer.flush()
