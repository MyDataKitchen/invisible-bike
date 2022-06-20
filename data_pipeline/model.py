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

# cursor = engine.execute("SELECT filename, updateTime FROM WheatherCrawler WHERE id = %s", '54')
# result = cursor.fetchall()
# cursor.close()
# print(result)

def insert_data_to_event(params):
    try:
        engine.execute(
            "INSERT INTO Event (datetime, stationId, total, availableSpace, emptySpace, status, outPerMinute, inPerMinute) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", params
        )
    except Exception as e:
        return e


def insert_data_to_precipitation(params):
    try:
        engine.execute(
            "INSERT INTO Precipitation (datetime, rainMin10, rainHour1, rainHour3, rainHour6, rainHour12, rainHour24, stationId) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", params
        )
    except Exception as e:
        return e


def insert_data_to_wheather(params):
    try:
        engine.execute(
            "INSERT INTO Wheather (datetime, windDirection, WindSpeed, temperature, uviHour, tempMax, tempMin, totalInsolation, describeId, stationId) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", params
        )
    except Exception as e:
        return e


def insert_data_to_youbike_station(params):
    try:
        engine.execute(
            "INSERT INTO YoubikeStation (stationId, name, address, lat, lon, districtId) VALUES (%s, %s , %s, %s, %s, %s)", params
        )
    except Exception as e:
        return e


def insert_data_to_precipitation_station(params):
    try:
        engine.execute(
            "INSERT INTO PrecipitationStation (stationId, name, lat, lon, districtId) VALUES (%s, %s, %s, %s, %s)", params
        )
    except Exception as e:
        return e


def insert_data_to_wheather_station(params):
    try:
        engine.execute(
            "INSERT INTO WheatherStation (stationId, name, lat, lon, districtId) VALUES (%s, %s, %s, %s, %s)", params
        )
    except Exception as e:
        return e

def insert_data_to_wheather_describe(params):
    try:
        engine.execute(
            "INSERT INTO WheatherDescribe (wheatherPhenomenon) VALUES (%s)", params
        )
    except Exception as e:
        return e


def insert_data_to_district(params):
    try:
        engine.execute(
            "INSERT INTO District (name) VALUES (%s)", params
        )
    except Exception as e:
        return e


def create_table(table):

    def event():
        engine.execute(
            """
            CREATE TABLE IF NOT EXISTS Event(
                id INT AUTO_INCREMENT PRIMARY KEY,
                datetime DATETIME,
                stationId INT,
                total INT,
                availableSpace INT,
                emptySpace INT,
                status INT,
                outPerMinute INT,
                inPerMinute INT
            )
            """
        )
        print("Event table created")


    def precipitation():
        engine.execute(
            """
            CREATE TABLE IF NOT EXISTS Precipitation(
                id INT AUTO_INCREMENT PRIMARY KEY,
                datetime DATETIME,
                rainMin10 FLOAT,
                rainHour1 FLOAT,
                rainHour3 FLOAT,
                rainHour6 FLOAT,
                rainHour12 FLOAT,
                rainHour24 FLOAT,
                stationId varchar(255)
            )
            """
        )
        print("Precipitation table created")


    def wheather():
        engine.execute(
            """
            CREATE TABLE IF NOT EXISTS Wheather(
                id INT AUTO_INCREMENT PRIMARY KEY,
                datetime DATETIME,
                windDirection FLOAT,
                WindSpeed FLOAT,
                temperature FLOAT,
                uviHour FLOAT,
                tempMax FLOAT,
                tempMin FLOAT,
                totalInsolation FLOAT,
                describeId INT,
                stationId varchar(255)
            )
            """
        )
        print("Wheather table created")


    def youbike_station():
        engine.execute(
            """
            CREATE TABLE IF NOT EXISTS YoubikeStation(
                id INT AUTO_INCREMENT PRIMARY KEY,
                stationId VARCHAR(45),
                name VARCHAR(255),
                address VARCHAR(255),
                lat FLOAT,
                lon FLOAT,
                districtId varchar(255)
            )
            """
        )
        print("YoubikeStation table created")


    def precipitation_station():
        engine.execute(
            """
            CREATE TABLE IF NOT EXISTS PrecipitationStation(
                id INT AUTO_INCREMENT PRIMARY KEY,
                stationId VARCHAR(45),
                name VARCHAR(255),
                lat FLOAT,
                lon FLOAT,
                districtId varchar(255)
            )
            """
        )
        print("PrecipitationStation table created")

    def wheather_station():
        engine.execute(
            """
            CREATE TABLE IF NOT EXISTS WheatherStation(
                id INT AUTO_INCREMENT PRIMARY KEY,
                stationId VARCHAR(45),
                name VARCHAR(255),
                lat FLOAT,
                lon FLOAT,
                districtId varchar(255)
            )
            """
        )
        print("WheatherStation table created")


    def wheather_describe():
        engine.execute(
            """
            CREATE TABLE IF NOT EXISTS WheatherDescribe(
                id INT AUTO_INCREMENT PRIMARY KEY,
                wheatherPhenomenon VARCHAR(255)
            )
            """
        )
        print("WheatherDescribe table created")


    def district():
        engine.execute(
            """
            CREATE TABLE IF NOT EXISTS District(
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255)
            )
            """
        )
        print("District table created")


    if table == 'Event':
        event()

    elif table == 'Precipitation':
        precipitation()

    elif table == 'Wheather':
        wheather()

    elif table == 'YoubikeStation':
        youbike_station()

    elif table == 'PrecipitationStation':
        precipitation_station()

    elif table == 'WheatherStation':
        wheather_station()

    elif table == 'WheatherDescribe':
        wheather_describe()

    elif table == 'District':
        district()
    else:
        return "None of the above options"

if __name__ == '__main__':
    create_table('Event')
    create_table('Precipitation')
    create_table('Wheather')
    create_table('YoubikeStation')
    create_table('PrecipitationStation')
    create_table('WheatherDescribe')
    create_table('WheatherStation')
    create_table('District')







