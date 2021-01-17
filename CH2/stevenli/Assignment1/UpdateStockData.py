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
            #sqlcommand= "DROP INDEX IF EXISTS idx_time"
            #cur.execute ( sqlcommand )
            #conn.commit ()
            #get next epoch date in python
            sqlcommand = "CREATE INDEX IF NOT EXISTS idx_time ON stock_daily(symbol);"
            cur.execute(sqlcommand)
            conn.commit ()
            # Is this SELECT query effective enough??
            sqlcommand = "SELECT symbol, time, close FROM stock_daily WHERE symbol='MGNX' ORDER BY time desc "
            cur.execute ( sqlcommand )
            record = cur.fetchone()
            #print(record)
            #print(record[0])
            print(record[1])
            # Get start date
            start_time = int(record[1])+60
            print(start_time)
            # Get current date as end_date
            end_time = util.convertDate_Unix(datetime.datetime.utcnow())
            print(end_time)
            finnhub_client = finnhub.Client(api_key="bv4f2qn48v6qpatdiu3g")
            res = finnhub_client.stock_candles('MGNX', 'D', int(start_time), int(end_time))
            time.sleep(1)
            if res['s'] == 'no_data':
                print("The symbol " + 'MGNX' + " has no data")
            else:
                res.pop('s', None)

                df = pd.DataFrame(res)
                print(df)

                df.insert(0, 'symbol', 'MGNX', allow_duplicates=True)



                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                cur.copy_from(buffer, 'stock_daily', sep=",")
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

#  Verify the target db table is existing
def check_Table(table_name):
    try:
        conn = util.cursor_setup()
        cur = conn.cursor()
        # create table one by one
        sqlcommand = "SELECT 1 FROM information_schema.tables WHERE " \
                     "table_schema='public' AND table_name='"+table_name+"'"
        cur.execute(sqlcommand)
        if cur.fetchone()[0]:
            db_exist = 1

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

# retrieve the latest timestamp for a symbol
def retrieve_latestSymbol(symbol, table_name):
    try:
        conn = util.cursor_setup()
        cur = conn.cursor()
        sqlcommand = "CREATE INDEX IF NOT EXISTS idx_time ON "+table_name+" (symbol);"
        cur.execute(sqlcommand)
        conn.commit()
        # Is this SELECT query effective enough??
        sqlcommand = "SELECT symbol, time, close FROM "+table_name+" WHERE symbol='"+symbol+"' ORDER BY time desc "
        cur.execute(sqlcommand)
        record = cur.fetchone()
        # Here, what if there are duplicate records???
        return record
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


# update symbol
def Add_SymbolData(record, resolution, symbol, table_name):
    # set the start time is 60 seconds after the latest time stamp
    if record is not None:
        start_time = int(record[1]) + 60
        # Get current date as end_date
        end_time = util.convertDate_Unix(datetime.datetime.utcnow())
        # call Finnhub candle
        finnhub_client = finnhub.Client(api_key="bv4f2qn48v6qpatdiu3g")
        res = finnhub_client.stock_candles(symbol, resolution, int(start_time), int(end_time))
        time.sleep(1)
        # check if return value is no_data
        if res['s'] == 'no_data':
            print("The symbol " + symbol + " has no data")
        else:
            # remove stat column from df
            res.pop('s', None)

            df = pd.DataFrame(res)
            # insert symbol as first column in df
            df.insert(0, 'symbol', symbol, allow_duplicates=True)

            util.copyfrom_stringIO(df, table_name)
    else:
        print(symbol + "has no data")

if __name__ == '__main__':
    db_exist = 0
    table_name='stock_daily'
    csv_file = 'sec_list_1000.csv'
    resolution = 'D'

    check_Table(table_name)



    #symbols = pd.read_csv(csv_file, nrows=3).to_numpy()
    symbols = pd.read_csv(csv_file).to_numpy()
    print(symbols)
    for symbol in symbols:
        # Update daily data
        #print(symbol[0])
        record = retrieve_latestSymbol(symbol[0], table_name)
        #print(record)
        Add_SymbolData(record, resolution, symbol[0], table_name)

        # Update one minute data
        record = retrieve_latestSymbol(symbol[0], 'stock_minute')
        #print(record)
        Add_SymbolData(record, '1', symbol[0], 'stock_minute')




