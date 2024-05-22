import json
import time

import boto3

TOPIC = "iot/temperature"
connection = boto3.client("iot-data")


def form_data() -> dict:
    return {
        "id": "hardcoded-id",
        "room": "hardcoded-room",
        "date": "hardcoded-dat",
        "temperature": "hardcoded-temperature",
        "location": "hardcoded-location",
        "application_id": "hardcoded-application_id",
    }


if __name__ == '__main__':
    while True:
        telemetry = form_data()
        print(f"Published telemetry from application {telemetry['application_id']}")
        connection.publish(topic=TOPIC, payload=json.dumps(telemetry))
        time.sleep(2)