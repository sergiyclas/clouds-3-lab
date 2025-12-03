import asyncio
import boto3
import random
import json
from datetime import datetime

with open("config.json", "r") as f:
    cfg = json.load(f)

client = boto3.client("sqs", region_name=cfg["region"])
queue_url = cfg["sqs_url"]

locations = {s["type"]: s["location"] for s in cfg["sensors"]}
device_ids = {s["type"]: s["device_id"] for s in cfg["sensors"]}

sem = asyncio.Semaphore(cfg["number_of_semaphores"])

def build_data(t):
    ranges = {
        "temperature": (1, 30),
        "humidity": (30, 100),
        "pressure": (600, 999),
        "error_sensor_type": (0, 0)
    }
    if t in ranges:
        low, high = ranges[t]
        v = round(random.uniform(low, high), 2)
    else:
        v = None

    return {
        "device_id": device_ids[t],
        "sensor_type": t,
        "value": v,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "location_lat": locations[t]["lat"],
        "location_lon": locations[t]["lon"]
    }

async def push(t, interval):
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(interval / 1000)
        payload = build_data(t)
        async with sem:
            try:
                await loop.run_in_executor(
                    None,
                    lambda: client.send_message(
                        QueueUrl=queue_url,
                        MessageBody=json.dumps(payload)
                    )
                )
            except Exception as e:
                print(f"{t} error: {e}")

async def run_all():
    await asyncio.gather(*[
        asyncio.create_task(push(s["type"], s["interval_ms"]))
        for s in cfg["sensors"]
    ])

if __name__ == "__main__":
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        print("Emulator stopped")
