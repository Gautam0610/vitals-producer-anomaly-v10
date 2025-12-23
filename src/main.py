from dotenv import load_dotenv
import os
import time
import random
from kafka import KafkaProducer
import json

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", 'PLAIN')
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC")
INTERVAL_MS = int(os.getenv("INTERVAL_MS", "1000")) / 1000  # Default to 1 second


def create_producer():
    if KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            sasl_mechanism=KAFKA_SASL_MECHANISM,
            sasl_plain_username=KAFKA_SASL_USERNAME,
            sasl_plain_password=KAFKA_SASL_PASSWORD,
            security_protocol='SASL_SSL',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    else:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    return producer


def generate_vitals(inject_anomaly=False):
    body_temp = round(random.uniform(36.5, 37.5), 1)
    heart_rate = random.randint(60, 100)
    systolic_pressure = random.randint(110, 140)
    diastolic_pressure = random.randint(70, 90)
    breaths_per_minute = random.randint(12, 20)
    oxygen_saturation = random.randint(95, 100)
    blood_glucose = random.randint(70, 140)

    if inject_anomaly:
        heart_rate = random.randint(150, 220)  # Unrealistic heart rate
        breaths_per_minute = random.randint(30, 50)  # Unrealistic breaths

    return {
        "body_temp": body_temp,
        "heart_rate": heart_rate,
        "systolic_pressure": systolic_pressure,
        "diastolic_pressure": diastolic_pressure,
        "breaths_per_minute": breaths_per_minute,
        "oxygen_saturation": oxygen_saturation,
        "blood_glucose": blood_glucose
    }


def main():
    producer = create_producer()
    anomaly_probability = 0.1  # 10% chance of anomaly

    while True:
        inject_anomaly = random.random() < anomaly_probability
        vitals = generate_vitals(inject_anomaly)
        print(f"Sending: {vitals}")
        producer.send(OUTPUT_TOPIC, vitals)
        producer.flush()  # Ensure message is sent immediately
        time.sleep(INTERVAL_MS)

if __name__ == "__main__":
    main()