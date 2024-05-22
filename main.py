import json
import time
import boto3
import pandas as pd

TOPIC = "iot/temperature"
connection = boto3.client("iot-data")

# Load CSV data
data_path = 'data/telemetry_data/iot_telemetry_data.csv'
data = pd.read_csv(data_path)

# Convert DataFrame to list of dictionaries
data_dict = data.to_dict(orient='records')

# Initialize a counter to iterate through data
index = 0

def form_data(record) -> dict:
    return {
        "ts": record.get("ts"),
        "device": record.get("device"),
        "co": record.get("co"),
        "humidity": record.get("humidity"),
        "light": record.get("light"),
        "lpg": record.get("lpg"),
        "motion": record.get("motion"),
        "smoke": record.get("smoke"),
        "temperature": record.get("temp"),
    }

if __name__ == '__main__':
    while True:
        record = data_dict[index]

        telemetry = form_data(record)
        print(f"Published telemetry from device {telemetry['device']}")
        connection.publish(topic=TOPIC, payload=json.dumps(telemetry))

        index = (index + 1) % len(data_dict)

        time.sleep(2)
