from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')

engine = create_engine(f"mysql+pymysql://{ SQL_USER }:{ SQL_PASSWORD }@{ SQL_HOST }/{ SQL_DATABASE }",
                       pool_size=10, max_overflow=5, pool_pre_ping=True)

def get_marker_data():
    result = engine.execute('SELECT name, lat, lon FROM YoubikeStation')
    rows = result.fetchall()
    result.close()
    return rows

if __name__ == '__main__':
    rows = get_marker_data()
    print(rows)