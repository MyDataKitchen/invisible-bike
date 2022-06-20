from dotenv import load_dotenv
import os
import boto3

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')

s3 = boto3.resource('s3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def get_json_from_s3(bucket, key):
    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8').splitlines()
    return data


def put_data_to_s3(bucket, key, data):
    obj = s3.Object(bucket, key)
    response = obj.put(Body=data)
    return response