from s3 import put_data_to_s3
from datetime import datetime as dt
from dotenv import load_dotenv
from model import get_latest_log, insert_crawler_log
import requests
import json
import os
import time

load_dotenv()

YOUBIKE_URL = os.getenv('YOUBIKE_URL')
S3_BUCKET = os.getenv('BUCKET')


def request_data(url):
    respone = requests.get(url)
    data = respone.json()
    updated_time = data[0]['srcUpdateTime']
    return data, updated_time


def datetime():
    now = dt.now()
    dt_string = now.strftime("%Y%m%d_%H:%M")
    return dt_string


def insert_data_to_s3(bucket, filename, data):
    status = put_data_to_s3(bucket, filename, json.dumps(data))
    return status


if __name__ == '__main__':

    data, updated_time = request_data(YOUBIKE_URL)
    latest_log = get_latest_log() # get latest log from mysql

    datetime_request = dt.strptime(updated_time, '%Y-%m-%d %H:%M:%S')
    datetime_log = latest_log[0]['updateTime']

    if datetime_request > datetime_log:
        start_time = time.time()
        path = "ubike_data/"
        filename = f"{ datetime() }_youbike.json"
        aws_respone = insert_data_to_s3(S3_BUCKET, path + filename, data)
        insert_crawler_log((filename, updated_time, len(data), (time.time() - start_time), 1, json.dumps(aws_respone)))

    else:
        start_time = time.time()
        path = "ubike_data/"
        filename = f"{ datetime() }_youbike.json"
        insert_crawler_log((filename, updated_time, len(data), (time.time() - start_time), 0, "None"))