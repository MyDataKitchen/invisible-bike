from dotenv import load_dotenv
import os
import pymysql

load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')
SQL_TABLE = os.getenv('MYSQL_TABLE')

db = pymysql.connect(host=SQL_HOST,
                     user=SQL_USER,
                     password=SQL_PASSWORD,
                     database=SQL_DATABASE)

def get_latest_log():
    db.ping(reconnect=True)
    query = f"SELECT * FROM {SQL_TABLE} ORDER BY id DESC LIMIT 1;"
    cursor = db.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()
    db.commit()
    cursor.close()
    return results

def insert_crawler_log(params):
    db.ping(reconnect=True)
    query = f"INSERT INTO {SQL_TABLE} (filename, updateTime, dataLength, ExecutionTime, insertStatus, awsRespone) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor = db.cursor()
    cursor.execute(query, params)
    db.commit()
    cursor.close()


