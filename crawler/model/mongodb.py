from dotenv import load_dotenv
from pymongo import MongoClient
import os

load_dotenv()

MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')

client = MongoClient(host=MONGO_HOST,
                     username=MONGO_USER,
                     password=MONGO_PASSWORD
                    )

db = client['invisible-bike']

def insert_youbike_data_to_mongo(city, data):
    if city == "taipei":
        try:
            collection = db["taipei_temp"]
            collection.insert_one(data)
            return True
        except Exception as e:
            return e

    elif city == "taichung":
        try:
            collection = db["taichung_temp"]
            collection.insert_one(data)
            return True
        except Exception as e:
            return e

    else:
        return False

def insert_weather_data_to_mongo(source, data):
    if source == "precipitation":
        try:
            collection = db["precipitation_temp"]
            collection.insert_one(data)
            return True

        except Exception as e:
            return e

    elif source == "weather":
        try:
            collection = db["weather_temp"]
            collection.insert_one(data)
            return True

        except Exception as e:
            return e

    else:
        return False

