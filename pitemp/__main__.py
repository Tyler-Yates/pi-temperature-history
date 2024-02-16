import datetime
import os.path
import pathlib
import sys
from typing import List, Optional

import pytz
import requests
from digitemp.device import TemperatureSensor
from digitemp.master import UART_Adapter
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.server_api import ServerApi

from pitemp.config import _get_config
from pitemp.db_entry import DbEntry

USB_SENSOR = '/dev/ttyUSB0'
MONGO_DATABASE = "sensors"
MONGO_COLLECTION = "pitemp"
MONGO_GRANULARITY = "minutes"

META_FIELD = "sensorId"
TIMESTAMP_FIELD = "timestamp"
TEMPERATURE_FIELD = "temp_f"

CONFIG = _get_config()
TIMEZONE = pytz.timezone(CONFIG.timezone)

CURRENT_FILE_PATH = pathlib.Path(__file__).parent.resolve()
ROOT_PATH = os.path.abspath(os.path.join(CURRENT_FILE_PATH, ".."))
UNSAVED_ENTRIES_FILE_PATH = os.path.join(ROOT_PATH, "unsaved_entries.txt")
DATETIME_FORMAT = "%d-%m-%Y%H:%M:%S"


def _get_mongo_client() -> MongoClient:
    uri = (f"mongodb+srv://{CONFIG.mongo_username}:{CONFIG.mongo_password}@{CONFIG.mongo_host}"
           f"/?retryWrites=true&w=majority")
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


def _parse_unsaved_entries_line(line: str) -> Optional[DbEntry]:
    if not line:
        return None

    parts = line.split(",")
    if len(parts) != 2:
        print(f"Corrupted unsaved entry line: {line!r}. Skipping.")
        return None

    try:
        timestamp = datetime.datetime.strptime(parts[0], DATETIME_FORMAT)
        temp = float(parts[1])
        return DbEntry(timestamp=timestamp, temp=temp)
    except Exception:
        print(f"Exception parsing unsaved entry line: {line!r}. Skipping.")
        return None


def _get_entry_string(entry: DbEntry) -> str:
    return f"{datetime.datetime.strftime(entry.timestamp, DATETIME_FORMAT)},{entry.temp}"


def _get_unsaved_entries() -> List[DbEntry]:
    unsaved_entries = []

    if not os.path.exists(UNSAVED_ENTRIES_FILE_PATH):
        return unsaved_entries

    with open(UNSAVED_ENTRIES_FILE_PATH, mode="r") as input_file:
        for line in input_file.readlines():
            line = line.strip()
            entry = _parse_unsaved_entries_line(line)

            if entry:
                unsaved_entries.append(entry)

    return unsaved_entries


def _get_current_entry() -> DbEntry:
    sensor = TemperatureSensor(UART_Adapter(USB_SENSOR))
    sensor.info()
    temperature_c = sensor.get_temperature()
    temperature_f = _convert_c_to_f(temperature_c)
    print(f"Temperature in F: {temperature_f}")
    return DbEntry(timestamp=datetime.datetime.now(TIMEZONE), temp=temperature_f)


def _get_entries_to_save_in_database() -> List[DbEntry]:
    # First get any entries that have not been saved. This can happen if the internet goes down for a period of time.
    # It is possible this is just an empty list which is fine.
    entries = _get_unsaved_entries()

    # Now add the current temperature.
    entries.append(_get_current_entry())

    return entries


def _save_entries(entries: List[DbEntry]):
    with open(UNSAVED_ENTRIES_FILE_PATH, mode="w") as output_file:
        lines = [_get_entry_string(x) for x in entries]
        output_file.write("\n".join(lines))


def _ping_healthcheck():
    response = requests.get(CONFIG.healthcheck_url)
    response.raise_for_status()
    print("Pinged healthcheck")


def main():
    entries = _get_entries_to_save_in_database()
    print(f"Saving {len(entries)} entries to the database: {entries}")
    _save_entries(entries)

    mongo_client = _get_mongo_client()
    collection = _ensure_mongo_setup(mongo_client)

    while entries:
        # Grab a single entry to save
        entry = entries.pop()
        collection.insert_one({
            TIMESTAMP_FIELD: entry.timestamp,
            META_FIELD: CONFIG.sensor_id,
            TEMPERATURE_FIELD: entry.temp,
        })
        print("Inserted document to Mongo.")

        # Save the remaining entries in case we fail inserting the next entry
        _save_entries(entries)

    _ping_healthcheck()


def _convert_c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32


if __name__ == '__main__':
    main()
