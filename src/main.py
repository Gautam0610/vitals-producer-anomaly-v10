import time
import random
from kafka import KafkaProducer
import json

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

# Anomaly probability (adjust as needed)
anomaly_probability = 0.1

# Define valid ranges for vitals
valid_ranges = {
    "heart_rate": (60, 100),
    "blood_pressure": (90, 140),  # Example systolic range
    "oxygen_saturation": (95, 100),
}

# Generate vitals with random values
def generate_heart_rate():
    return random.randint(50, 150)

def generate_blood_pressure():
    return random.randint(80, 160)

def generate_oxygen_saturation():
    return random.randint(90, 100)

# Apply anomalies to vitals
def apply_anomaly(vital, vital_type):
    if random.random() < anomaly_probability:
        if vital_type == "heart_rate":
            vital *= random.uniform(0.5, 2)  # heart rate can double or halve
        elif vital_type == "blood_pressure":
            vital *= random.uniform(0.7, 1.3)  # blood pressure can vary by 30%
        elif vital_type == "oxygen_saturation":
            vital *= random.uniform(0.8, 1.0)  # SpO2 can drop by 20%
        vital = int(vital)
        print(f"Anomaly detected in {vital_type}: {vital}")
    return vital

def is_valid_vital(vital_type, value):
    """
    Checks if a vital is within the allowed range.
    """
    if vital_type in valid_ranges:
        lower_bound, upper_bound = valid_ranges[vital_type]
        return lower_bound <= value <= upper_bound
    else:
        return True  # Unknown vital, consider it valid

# Modify the producer function to filter invalid vitals
def produce_vitals(producer):
    while True:
        heart_rate = generate_heart_rate()
        blood_pressure = generate_blood_pressure()
        oxygen_saturation = generate_oxygen_saturation()

        # Apply anomalies
        heart_rate = apply_anomaly(heart_rate, "heart_rate")
        blood_pressure = apply_anomaly(blood_pressure, "blood_pressure")
        oxygen_saturation = apply_anomaly(oxygen_saturation, "oxygen_saturation")

        # Create a dictionary for the vitals data
        vitals_data = {
            "heart_rate": heart_rate,
            "blood_pressure": blood_pressure,
            "oxygen_saturation": oxygen_saturation,
        }

        # Filter out invalid vitals before sending
        valid_vitals_data = {}
        for key, value in vitals_data.items():
            if is_valid_vital(key, value):
                valid_vitals_data[key] = value
            else:
                print(f"Vital {key} with value {value} is outside the valid range.")

        # Send the valid vitals data to Kafka
        try:
            producer.send("vitals", value=valid_vitals_data)
            producer.flush()
            print(f"Sent: {valid_vitals_data}")
        except Exception as e:
            print(f"Error sending vitals: {e}")

        time.sleep(1)

# Run the producer
if __name__ == "__main__":
    produce_vitals(producer)
