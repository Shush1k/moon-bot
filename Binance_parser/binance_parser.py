import requests
import time
import psycopg2
from psycopg2 import Error
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv
from os import getenv


def postgres_insert(data):
    try:
        conn = psycopg2.connect(
            host=getenv("HOST"),
            database=getenv("DB_NAME"),
            user=getenv("DB_USER"),
            password=getenv("DB_PASSWORD"))

        cursor = conn.cursor()
        postgres_insert_query = """UPDATE mbcurrencies SET price = %s WHERE symbol = %s"""
        cursor.executemany(postgres_insert_query, data)

        conn.commit()
        count = cursor.rowcount
        print(count, "Rows successfully updated in mbcurrencies")
    except psycopg2.OperationalError as exn:
        print(exn)
        time.sleep(180)
    except (Exception, Error) as error:
        print("Error on UPDATE mbcurrencies | PostgresSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()


def parse_func(url: str) -> list:
    session = requests.Session()
    retry = Retry(connect=15, total=5, backoff_factor=0.1,
                  status_forcelist=[500, 502, 503, 504])
    session.mount(url, HTTPAdapter(max_retries=retry))
    if not session.get(url).ok:
        print(f"CODE: {session.get(url)}")
    return session.get(url).json()


def clean_data(data):
    res = []
    for i in data:
        res.append((i["price"], i["symbol"],))
    return res


if __name__ == "__main__":
    load_dotenv()
    CURRENCIES = '["BTCUSDT","BNBUSDT","BUSDUSDT","LTCUSDT","BTCRUB"]'
    FREQUENCY = 300  # time in seconds

    URL = "https://data.binance.com/api/v3/ticker/price?symbols="
    URL += CURRENCIES
    while True:
        postgres_data = None
        try:
            try:
                postgres_data = parse_func(URL)
                postgres_data = clean_data(postgres_data)
            except Exception as e:
                print(e)
                time.sleep(10)

            if postgres_data:
                postgres_insert(postgres_data)

            time.sleep(FREQUENCY)
        except KeyboardInterrupt as kie:
            print(f"Program was stopped : KeyboardInterrupt")
            break
