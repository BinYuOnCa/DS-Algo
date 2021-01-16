import pandas as pd
from io import StringIO
import psycopg2
import finnhub
import db_utility as util
import logging
import time
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from configparser import ConfigParser



if __name__ == '__main__':
    # Setup finnhub client
    table = 'stock_daily'
    finnhub_client = finnhub.Client ( api_key="bv4f2qn48v6qpatdiu3g" )

    try:
        conn = util.cursor_setup()
        cur = conn.cursor()
        dropcommand = ("DROP TABLE IF EXISTS " + table)

        sqlcommand = "CREATE TABLE IF NOT EXISTS " + table + " (" \
                                                             "symbol   VARCHAR(50) NOT NULL, " \
                                                             "close    FLOAT NOT NULL, " \
                                                             "high     FLOAT NOT NULL, " \
                                                             "low      FLOAT NOT NULL, " \
                                                             "open     FLOAT NOT NULL, " \
                                                             "time     INT NOT NULL, " \
                                                             "volume   FLOAT NOT NULL) "





        cur.execute(dropcommand)
        conn.commit()
        cur.execute(sqlcommand)

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            cur.close()
            conn.close()


    symbols = pd.read_csv("sec_list_1000.csv").to_numpy()
    index = 0
    conn = util.cursor_setup ()
    cur = conn.cursor ()
    print(datetime.datetime.now())
    for symbol in symbols:
        print(symbol)
        res = finnhub_client.stock_candles(symbol, 'D',  979527600, 1610679600)
        index +=1
        time.sleep(1)
        if res['s'] == 'no_data':
            print("The symbol "+symbol+" has no data")
        else:
            res.pop('s', None)

            df = pd.DataFrame(res)
            #df['symbol']=symbol[0]
            #df.drop(df.columns[0], axis=1)
            df.insert(0, 'symbol', symbol[0], allow_duplicates=True)


            #print(df)
            df2 = pd.DataFrame()

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cur.copy_from(buffer, table, sep=",")
            conn.commit()





    cur.close ()
    conn.close()






    print(datetime.datetime.now())
    print("End of code.")

