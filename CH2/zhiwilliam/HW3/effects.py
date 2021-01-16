import os
import finnhub
import configparser
import psycopg2
from twilio.rest import Client
import requests


class FinnhubClient:
    def __init__(self, api_key=os.environ.get("FINNHUB_API_KEY")):
        self.api_key = api_key
        self.client = None

    def __enter__(self):
        self.client = finnhub.Client(self.api_key)
        return self

    def __exit__(self, type, value, traceback):
        self.client.close()


class FinnhubRESTful:
    def __init__(self, api_key=os.environ.get("FINNHUB_API_KEY")):
        self.query = """https://finnhub.io/api/v1/stock/candle?symbol={symbol}\
            &resolution={resolution}&from={start}&to={end}&token=bv83fhf48v6vtpa0fjkg"""
        self.api_key = api_key

    def __call__(self, **kwargs):
        self.request = requests.get(self.query.format(symbol=kwargs['symbol'],
                                                      resolution=kwargs['resolution'],
                                                      start=kwargs['start'],
                                                      end=kwargs['end']))
        return self

    def __enter__(self):
        self.result = self.request.json()
        return self

    def __exit__(self, type, value, traceback):
        self.request.close()


class PostgresqlStore:
    def __init__(self, db_password=os.environ.get("DB_PASSWORD")):
        config = configparser.ConfigParser()
        config.read_file(open('application.conf'))
        db_config = config['DATABASE']
        self.db_host = db_config['host']
        self.db_port = int(db_config['port'])
        self.db_user = db_config['user']
        self.db_database = db_config['dbname']
        self.db_password = str(db_password)

    def __enter__(self):
        self.conn = psycopg2.connect(database=self.db_database, host=self.db_host,
                                     port=self.db_port, user=self.db_user, password=self.db_password)
        return self

    def __exit__(self, type, value, traceback):
        self.conn.close()


class SMS:
    def __init__(self, sid=os.environ.get("TWILIO_ACCOUNT_SID"), token=os.environ['TWILIO_AUTH_TOKEN']):
        self.account_sid = os.environ['TWILIO_ACCOUNT_SID']
        self.auth_token = os.environ['TWILIO_AUTH_TOKEN']

    def __enter__(self):
        self.client = Client(self.account_sid, self.auth_token)
        return self

    def __exit__(self, type, value, traceback):
        pass


if __name__ == "__main__":
    finnhub_restful = FinnhubRESTful()
    with finnhub_restful(symbol="GOOG", resolution="D", start=1605543327, end=1605629727) as finnhub_res:
        print(finnhub_res.result)
