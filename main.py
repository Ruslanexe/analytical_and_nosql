from awsiot import mqtt_connection_builder

import json
import time

import boto3

TOPIC = "iot/temperature"
connection = boto3.client(
    "iot-data",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=REGION
)


def form_data(data_row: str, application_name: int) -> dict:
    # тут треба замінити поля на свої залежно від датасету
    device_id, room, date, temperature, location = data_row.split(",")
    return {
        "id": device_id,
        "room": room,
        "date": date,
        "temperature": temperature,
        "location": location,
        "application_id": f"{TOPIC}/{application_name}",
    }


if __name__ == '__main__':
    with open("data.csv") as file:
        for index, row in enumerate(file.readlines()):
            telemetry = form_data(row.strip(), index % 10 + 1)
            print(f"Published telemetry from application {telemetry['application_id']}")
            connection.publish(topic=TOPIC, payload=json.dumps(telemetry))
            time.sleep(0.5)