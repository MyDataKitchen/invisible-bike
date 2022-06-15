from s3 import put_data_to_s3
from datetime import datetime as dt
from datetime import timezone, timedelta
from dotenv import load_dotenv
from model import get_latest_log, insert_crawler_log
import requests
import json
import os
import time
import sys

load_dotenv()

YOUBIKE_URL = os.getenv('YOUBIKE_URL')
S3_BUCKET = os.getenv('BUCKET')
SQL_TABLE = os.getenv('YOUBIKE_TABLE')


def request_data(url):
    respone = requests.get(url)
    data = respone.json()
    updated_time = data[0]['srcUpdateTime']
    size = sys.getsizeof(json.dumps(data))
    size = round( size / 1024, 0)
    return data, updated_time, size


def datetime():
    tz = timezone(timedelta(hours=+8))
    now = dt.now(tz)
    dt_string = now.strftime("%Y%m%d_%H:%M")
    return dt_string


def insert_data_to_s3(bucket, filename, data):
    status = put_data_to_s3(bucket, filename, json.dumps(data))
    return status


if __name__ == '__main__':
    date_time = datetime()
    start = time.time()
    data, updated_time, size = request_data(YOUBIKE_URL)
    end = time.time()
    responseTime = end - start

    start = time.time()
    latest_log = get_latest_log(SQL_TABLE) # get latest log from mysql

    datetime_request = dt.strptime(updated_time, '%Y-%m-%d %H:%M:%S')
    datetime_log = latest_log[0]['updateTime']

    if datetime_request > datetime_log:
        path = "ubike_data/"
        filename = f"{ date_time }_youbike.json"
        aws_respone = insert_data_to_s3(S3_BUCKET, path + filename, data)
        end = time.time()
        executionTime = end - start
        insert_crawler_log(SQL_TABLE, (filename, updated_time, len(data), size, responseTime, executionTime, 1, json.dumps(aws_respone)))

    else:
        path = "ubike_data/"
        filename = f"{ date_time }_youbike.json"
        end = time.time()
        executionTime = end - start
        insert_crawler_log(SQL_TABLE, (filename, updated_time, len(data), size, responseTime, executionTime, 0, None))