import datetime
import json
import os
import sys
from pathlib import Path

import requests
from digitemp.device import TemperatureSensor
from digitemp.master import UART_Adapter
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.server_api import ServerApi

USB_SENSOR = '/dev/ttyUSB0'
CURRENT_FILE_PATH = Path(__file__).parent.absolute()
MONGO_DATABASE = "sensors"
MONGO_COLLECTION = "pitemp"
MONGO_GRANULARITY = "minutes"
SENSOR_ID = "pi"

META_FIELD = "sensorId"
TIMESTAMP_FIELD = "timestamp"
TEMPERATURE_FIELD = "temp_f"


def _get_mongo_client() -> MongoClient:
    with open(os.path.join(CURRENT_FILE_PATH, "..", "config.json"), mode="r") as config_file:
        config = json.load(config_file)
        username = config["mongo_username"]
        password = config["mongo_password"]
        host = config["mongo_host"]

    uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    return MongoClient(uri, server_api=ServerApi('1'))


def _ensure_mongo_setup(mongo_client: MongoClient) -> Collection:
    # Ensure we can connect
    try:
        mongo_client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
        sys.exit(1)

    # Ensure collection is set up
    database = mongo_client[MONGO_DATABASE]

    collection_exists = MONGO_COLLECTION in list(x["name"] for x in database.list_collections())
    if not collection_exists:
        database.create_collection(
            MONGO_COLLECTION,
            timeseries={
                "timeField": TIMESTAMP_FIELD,
                "metaField": META_FIELD,
                "granularity": MONGO_GRANULARITY,
            }
        )

    collection = database[MONGO_COLLECTION]
    return collection


def _ping_healthcheck():
    with open(os.path.join(CURRENT_FILE_PATH, "..", "config.json"), mode="r") as config_file:
        config = json.load(config_file)
        healthcheck_url = config["healthcheck_url"]

    response = requests.get(healthcheck_url)
    response.raise_for_status()
    print("Pinged healthcheck")


def main():
    mongo_client = _get_mongo_client()
    collection = _ensure_mongo_setup(mongo_client)

    sensor = TemperatureSensor(UART_Adapter(USB_SENSOR))
    sensor.info()
    temperature_c = sensor.get_temperature()
    temperature_f = _convert_c_to_f(temperature_c)
    print(f"Temperature in F: {temperature_f}")
    collection.insert_one({
        TIMESTAMP_FIELD: datetime.datetime.now(),
        META_FIELD: SENSOR_ID,
        TEMPERATURE_FIELD: temperature_f,
    })
    print("Inserted document to Mongo.")


def _convert_c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32


if __name__ == '__main__':
    main()
