from s3 import put_data_to_s3
from datetime import datetime as dt
from dotenv import load_dotenv
from model import get_latest_log, insert_crawler_log
from datetime import timezone
import requests
import json
import os
import time

load_dotenv()

WHEATHER_URL = os.getenv('WHEATHER_URL')
S3_BUCKET = os.getenv('BUCKET')
SQL_TABLE = os.getenv('WHEATHER_TABLE')


def request_data(url):
    respone = requests.get(url)
    data = respone.json()
    updated_time = dt.fromisoformat(data['cwbopendata']['location'][0]['time']['obsTime'][:-6]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    return data, updated_time


def datetime():
    now = dt.now()
    dt_string = now.strftime("%Y%m%d_%H:%M")
    return dt_string


def insert_data_to_s3(bucket, filename, data):
    status = put_data_to_s3(bucket, filename, json.dumps(data))
    return status


if __name__ == '__main__':

    data, updated_time = request_data(WHEATHER_URL)
    latest_log = get_latest_log(SQL_TABLE) # get latest log from mysql

    datetime_request = dt.strptime(updated_time, '%Y-%m-%d %H:%M:%S')
    datetime_log = latest_log[0]['updateTime']

    if datetime_request > datetime_log:
        start_time = time.time()
        path = "wheather_data/"
        filename = f"{ datetime() }_wheather.json"
        aws_respone = insert_data_to_s3(S3_BUCKET, path + filename, data)
        end_time = time.time()
        insert_crawler_log(SQL_TABLE, (filename, updated_time, len(data), (end_time - start_time), 1, json.dumps(aws_respone)))

    else:
        start_time = time.time()
        path = "wheather_data/"
        filename = f"{ datetime() }_wheather.json"
        end_time = time.time()
        insert_crawler_log(SQL_TABLE, (filename, updated_time, len(data), (end_time - start_time), 0, None))