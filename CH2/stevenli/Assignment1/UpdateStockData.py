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
#
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
            sqlcommand = "CREATE INDEX "
            # commit the changes
            conn.commit()
            cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


UpdateData()