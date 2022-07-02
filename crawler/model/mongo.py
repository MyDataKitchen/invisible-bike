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

db = client['invisible_bike']

def insert_youbike_data_to_mongo(city, data):
    if city == "taipei":
        try:
            db.taipei.insert(data)
            return True
        except Exception as e:
            return e

    elif city == "taichung":
        try:
            db.taichung.insert(data)
            return True
        except Exception as e:
            return e

    else:
        return False

def insert_weather_data_to_mongo(data):
    try:
        db.taipei.insert(data)
        return True

    except Exception as e:
        return e
