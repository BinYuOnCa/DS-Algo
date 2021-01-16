import effects
import logging
import configparser
from datetime import datetime


class FinnhubQuery:
    def __init__(self):
        pass

    def restful_candles(self, symbol: str, resolution: str, start: str, end: str):
        finnhub_restful = effects.FinnhubRESTful()
        with finnhub_restful(symbol=symbol, resolution=resolution, start=start, end=end) as finnhub_res:
            return finnhub_res.result

    def api_candles(self, symbol: str, resolution: str, start: str, end: str):
        with effects.FinnhubClient() as finnhub:
            # 1, 5, 15, 30, 60, D, W, M
            data = finnhub.client.stock_candles(symbol, resolution, start, end)
            if(data["s"] == "ok"):
                return list(zip(data['o'], data['c'], data['h'], data['l'], data['v'], data['t']))
            elif(data["s"] == "no_data"):
                return []
            else:
                raise Exception("Failed to load candles" + data["s"])


class OldDataQuery:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read_file(open('application.conf'))
        db_config = config['DATABASE']

        self.table_dict = {"1": db_config['table_1'], "D": db_config['table_d']}
        self.base_time = datetime(1970, 1, 1)
        self.cadnles_foramt = """SELECT symbol, open, close, high, low, volume, \
            tick FROM {table} WHERE symbol = '{symbol}' ORDER BY tick DESC limit 1"""

    def latest_candle(self, symbol: str, resolution: str):
        with effects.PostgresqlStore() as db:
            cur = db.conn.cursor()
            sql = self.cadnles_foramt.format(table=self.table_dict[resolution], symbol=symbol)
            try:
                cur.execute(sql)
                data = cur.fetchall()
                return data
            except Exception as error:
                logging.error(error)
            cur.close()


if __name__ == "__main__":
    data = OldDataQuery().latest_candle('GOOG', 'D')
    print(data)
