import boto3
import json
import time
import random
import threading
from datetime import datetime

SQS_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/153668968529/MyQueue'


def get_region_from_url(url):
    try:
        return url.split('.')[1]
    except IndexError:
        return 'eu-north-1'


CURRENT_REGION = get_region_from_url(SQS_QUEUE_URL)
sqs = boto3.client('sqs', region_name=CURRENT_REGION)

SENSORS = [
    {
        "id": "sensor-01",
        "type": "Temperature",
        "interval": 0.09,
        "location": {"lat": 12.3543, "lon": 65.6543}
    },
    {
        "id": "sensor-02",
        "type": "Humidity",
        "interval": 0.06,
        "location": {"lat": 41.31, "lon": 65.1234}
    },
    {
        "id": "sensor-03",
        "type": "Ldr",
        "interval": 0.04,
        "location": {"lat": 43.4842, "lon": 23.3234}
    }
]


def run_sensor(sensor_config):
    print(f"Запущено датчик {sensor_config['type']} (ID: {sensor_config['id']})...")

    while True:
        value = 0.0
        if sensor_config['type'] == 'Temperature':
            value = round(random.uniform(4.0, 34.0), 2)
        elif sensor_config['type'] == 'Humidity':
            value = round(random.uniform(43.0, 56.0), 2)
        elif sensor_config['type'] == 'Ldr':
            value = round(random.uniform(542.0, 1231.0), 1)

        payload = {
            "sensor_id": sensor_config['id'],
            "type": sensor_config['type'],
            "value": value,
            "location": sensor_config['location'],
            "timestamp": datetime.now().isoformat()
        }

        try:
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(payload)
            )
            print(f"[{sensor_config['type']}] Sent: {value}")
        except Exception as e:
            print(f"❌ Error: {e}")

        time.sleep(sensor_config['interval'])


if __name__ == "__main__":
    threads = []

    for sensor in SENSORS:
        t = threading.Thread(target=run_sensor, args=(sensor,))
        t.daemon = True
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped.")