import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from mysql.connector import connect, Error
from datetime import datetime, date
import time
import os
import argparse
from dotenv import load_dotenv
import logging


def parse_func(url: str) -> list:
    session = requests.Session()
    retry = Retry(connect=15, total=5, backoff_factor=0.1,
                  status_forcelist=[500, 502, 503, 504])
    session.mount(url, HTTPAdapter(max_retries=retry))
    if not session.get(url).ok:
        logging.info(f"CODE: {session.get(url)}")
    return session.get(url).json()


def to_normal_time(data):
    for i in data:
        utc3 = i["l"] + 10800
        norm = datetime.utcfromtimestamp(utc3).strftime('%H:%M:%S')
        i["l"] = norm
    return data


def add_data(data: list, ids: list):
    new_arr = []
    for i in data:
        if i["bi"] in ids:
            i.pop("date")
            new_arr.append(i)
    return new_arr


def id_list_parsing(ids: str):
    return ids.strip().split(",")


def data_to_DB(data: list):
    create_orders_table_query = """
        CREATE TABLE IF NOT EXISTS orders(
            id INT AUTO_INCREMENT PRIMARY KEY,
            userId INT,
            telega VARCHAR(100),
            date DATE,
            time TIME,
            coin VARCHAR(100),
            bprice FLOAT,
            sprice FLOAT,
            order_size FLOAT,
            profit FLOAT,
            profit_percent FLOAT,
            trade VARCHAR(100)
        )
        """

    with connection.cursor() as cursor:
        cursor.execute(create_orders_table_query)
        connection.commit()

    with connection.cursor() as cursor:

        for i in data:
            trade = "manual"
            if i["a"] == 1:
                trade = "auto"
            try:
                cursor.execute("""INSERT INTO orders 
                    (id, userId, telega, date, time, coin, bprice, sprice, order_size, profit, profit_percent, trade)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
                    i["i"], i["bi"], i["t"], date.today().strftime("%Y-%m-%d"), i["l"], i["c"], i["b"], i["s"], i["o"],
                    i["u"], i["p"], trade))
                connection.commit()
                logging.info("ID: {i} Time: {l} Telegram: {t}".format(**i))
            except Error as e:
                print(e)


if __name__ == "__main__":
    load_dotenv()

    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(funcName)s || %(message)s",
                        handlers=[logging.FileHandler("../log/parse.log"), logging.StreamHandler()], level=logging.INFO)

    URL = "https://stat.moon-bot.com/getTrades"

    parser = argparse.ArgumentParser(description="The format is: python parse.py -i 404,222,111",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--ids", help="search by user id")

    args = vars(parser.parse_args())
    id_list = [int(i) for i in args["ids"].split(",")]

    connection = connect(host=os.getenv("HOST"),
                         user=os.getenv("USERNAME"),
                         password=os.getenv("PASSWORD"),
                         database=os.getenv("DBNAME"))

    logging.info(f"MySQL connection: {connection.is_connected()}")

    while True:
        try:
            try:
                arr = parse_func(URL)
            except Error as e:
                print(e)
                time.sleep(10)
                continue

            arr = add_data(arr, id_list)
            arr = to_normal_time(arr)
            data_to_DB(arr)
            time.sleep(0.3)
        except KeyboardInterrupt as kie:
            logging.info(f"Program was stopped : KeyboardInterrupt")
            break
