import schedule
import subprocess
import time

def do_youbike_crawling():
    subprocess.run(["python3", "youbike_crawler.py"])

def do_precipitation_crawling():
    subprocess.run(["python3", "precipitation_crawler.py"])

def do_wheather_crawling():
    subprocess.run(["python3", "wheather_crawler.py"])

if __name__ == '__main__':

    schedule.every(1).minutes.do(do_youbike_crawling)
    schedule.every(10).minutes.do(do_precipitation_crawling)
    schedule.every(10).minutes.do(do_wheather_crawling)

    while True:
        schedule.run_pending()
        time.sleep(1)

