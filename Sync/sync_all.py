import sqlite3
import psycopg2
from psycopg2 import OperationalError
from utils import add_extra_data
import configparser
import time

# Connect to SQLite and Postgres databases
config = configparser.ConfigParser()
config.read('config.ini')

BOT_NAME = config['bot']['name']
CUR = config['bot']['currency']

SQLITE_DB = config['SQLite']['name']


while True:
    try:
        sqlite_conn = sqlite3.connect(SQLITE_DB)

        postgres_conn = psycopg2.connect(
                        host=config['POSTGRES']['host'],
                        database=config['POSTGRES']['database'],
                        user=config['POSTGRES']['user'],
                        password=config['POSTGRES']['password'])

        

        # Retrieve ids from SQLite where CloseDate is not zero
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT ID FROM Orders WHERE CloseDate != 0")
        id_list = sqlite_cursor.fetchall() # id_list - список id закрытых ордеров
        # Iterate through rows and synchronize with Postgres
        postgres_cursor = postgres_conn.cursor()
        postgres_cursor.execute("SELECT id FROM orders WHERE bot_name = %s AND id IN %s", (BOT_NAME, tuple(id_list),))
        existing_rows = postgres_cursor.fetchall()

        # Result of id difference between 2 databases
        difference_ids = list(set(id_list) ^ set(existing_rows))
        if difference_ids:
            flat_difference_ids = [item for tpl in difference_ids for item in tpl]
            sql_text = """SELECT ID,exOrderID,BuyPrice,CloseDate,BuyDate,SellSetDate,Quantity,SellPrice,SpentBTC,GainedBTC,ProfitBTC,Source,Channel,ChannelName,Status,Comment,BaseCurrency,BoughtQ,BTC1hDelta,Exchange1hDelta,FName,deleted,Emulator,Imp,BTC24hDelta,Exchange24hDelta,bvsvRatio,BTC5mDelta,IsShort,SignalType,SellReason,Pump1H,Dump1H,d24h,d3h,d1h,d15m,d5m,d1m,dBTC1m,PriceBug,Vd1m,Lev,hVol,dVol,TaskID,hVolF,Coin FROM Orders WHERE CloseDate != 0 AND ID IN({seq})""".format(
                    seq=','.join(['?']*len(flat_difference_ids)))
            sqlite_cursor.execute(sql_text, flat_difference_ids)
            rows = sqlite_cursor.fetchall() # rows - список закрытых ордеров
            rows = add_extra_data(rows, BOT_NAME, CUR)
            # 3,4,5
            rows = [list(ele) for ele in rows]
            ut3_to_utc = 10800
            for row in rows:
                row[3] = row[3] - ut3_to_utc
                row[4] = row[4] - ut3_to_utc
                row[5] = row[5] - ut3_to_utc

            postgre_sql = """INSERT INTO orders ( id, ex_order_id, buy_price, close_date, buy_date, sell_set_date, quantity, sell_price, spent_btc, gained_btc, profit_btc, source, channel, channel_name, status, comment, base_currency, boughtq, btc_1h_delta, exchange_1h_delta, fname, deleted, emulator, imp, btc_24h_delta, exchange_24h_delta, bvsv_ratio, btc_5m_delta, is_short, signal_type, sell_reason, pump_1h, dump_1h, d24h, d3h, d1h, d15m, d5m, d1m, d_btc_1m, pricebug, vd1m, lev, h_vol, d_vol, task_id, h_vol_f, coin, bot_name, bot_currency
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id, bot_name) DO NOTHING"""
            postgres_cursor.executemany(postgre_sql, rows)
            postgres_conn.commit()
            count = postgres_cursor.rowcount
            print(count, ": count of added rows")
        else:
            print("...")
        
    except OperationalError as e:
        print(f"Exception: {e}")
        # Wait for 5 minutes before trying to connect again
        time.sleep(300)
    finally:
        postgres_conn.close()
        sqlite_conn.close()

    time.sleep(3000)