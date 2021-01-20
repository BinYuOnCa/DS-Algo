import pandas as pd
from io import StringIO
import psycopg2
import finnhub
import CH2.stevenli.Assignment1.db_utility as util
import logging
import time
import datetime


def Recalculate_Stock(split_ratio, symbol,resolution, start_time, end_time):
    start_time = 979527600
    end_time = util.convertDate_Unix(datetime.datetime.utcnow())
    finnhub_client = finnhub.Client(api_key="bv4f2qn48v6qpatdiu3g")
    res = finnhub_client.stock_candles(symbol, resolution, start_time, int(end_time))
    res.pop('s', None)
    df = pd.DataFrame(res)
    df.insert(0, 'symbol', symbol, allow_duplicates=True)

    if split_ratio > 1:
        ratio = 1 / (1 + split_ratio)
        df['c'] = df['c'] * ratio
        df['h'] = df['h'] * ratio
        df['l'] = df['h'] * ratio
        df['o'] = df['o'] * ratio

    elif split_ratio < 1:
        ratio = 1 / split_ratio
        df['c'] = df['c'] * ratio
        df['h'] = df['h'] * ratio
        df['l'] = df['h'] * ratio
        df['o'] = df['o'] * ratio

    return df






symbol = 'MGNX'
split_ratio = 2
start_time  = 979527600
end_time = util.convertDate_Unix(datetime.datetime.utcnow())
resolution = 'D'
dailytable = 'stock_daily'
df = Recalculate_Stock(split_ratio, symbol, resolution, start_time, end_time)

sqlcommand = "DELETE FROM "+dailytable+" WHERE symbol = '"+symbol+"'"
util.execute_sql(sqlcommand)
util.copyfrom_stringIO(df, dailytable)

print("END")




