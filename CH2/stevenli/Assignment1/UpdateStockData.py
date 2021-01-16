import pandas as pd
from io import StringIO
import psycopg2
import finnhub
import db_utility as util
import logging
import time
import datetime
from configparser import ConfigParser

###
# First, check if the table is existing, otherwise log an warning
# Querey the symbol from table, get the latest time
# index the symbol and time
# from the last end date +1 day, fetch stock candles and insert into table
# If it is daily data, only one row, simply insert into
# if it is one minute data, use copy_from to insert into ???
#
def UpdateData():
    conn = None
    try:
        conn = util.cursor_setup()
        cur = conn.cursor()
        # create table one by one
        sqlcommand = "SELECT 1 FROM information_schema.tables WHERE " \
                     "table_schema='public' AND table_name='stock_daily'"
        cur.execute(sqlcommand)
        if cur.fetchone()[0]:
            print("table exist")
            # Create index in daily table
            sqlcommand= "DROP INDEX IF EXISTS idx_time"
            cur.execute ( sqlcommand )
            conn.commit ()
            sqlcommand = "CREATE INDEX idx_time ON stock_daily(symbol);"
            cur.execute(sqlcommand)
            conn.commit ()
            # Is this SELECT query effective enough??
            sqlcommand = "SELECT symbol, time, close FROM stock_daily WHERE symbol='MGNX' ORDER BY time desc "
            cur.execute ( sqlcommand )
            print(cur.fetchone())
            print(cur.fetchone()[0])
            print(cur.fetchone()[1])
            #today = datetime.now()
            #print("today is "+today.strftime())
            #print("today unxi time is: "+util.convert_time(today))


            # commit the changes
            conn.commit()
        else:
            print("The table is not existing.")
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


UpdateData()