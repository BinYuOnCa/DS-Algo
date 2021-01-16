from query import FinnhubQuery, OldDataQuery
from persistent import SaveData
from datetime import datetime
from effects import SMS
import sys
import getopt
# from decimal import Decimal
import csv
import pytz
import sched
import time
import logging


logging.basicConfig(filename='etl.log', level=logging.INFO)
est = pytz.timezone('US/Eastern')


def getCurrentTick(symbols, resolution, latest_tick, api: str = 'api'):
    finnhub = FinnhubQuery()
    now = datetime.now().astimezone(est).replace(tzinfo=None)
    now_seconds = int((now - datetime(1970, 1, 1)).total_seconds())
    try:
        if(api == 'api'):
            return finnhub.api_candles(symbols, resolution, latest_tick, now_seconds)
        else:
            return finnhub.restful_candles(symbols, resolution, latest_tick, now_seconds)
    except Exception as error:
        logging.error(error)


def load_and_etl(symbol, resolution, api):
    db_tick_index = 6
    # db_open_index = 1
    tick_index = 5
    # open_index = 0

    latest_candle = OldDataQuery().latest_candle(symbol, resolution)
    print(latest_candle)
    if(latest_candle is not None and len(latest_candle) > 0):
        # Get latest tick time, compare it with retrieved tick and detect split/merge
        latest_tick = latest_candle[0][db_tick_index]
        candles_data = getCurrentTick(symbol, resolution, latest_tick)
        # same_tick_candle_data = list(filter(lambda x: (x[tick_index] == latest_tick), candles_data))
        # if(len(same_tick_candle_data) > 0):
        #     new_open = Decimal(same_tick_candle_data[0][open_index])
        #     old_open = latest_candle[0][db_open_index]
        #     if (abs(new_open - old_open) > 0.01):
        #         factor = new_open / old_open
        #         : update
        save_data = list(filter(lambda x: (x[tick_index] > latest_tick), candles_data))
        if(len(save_data) > 0):
            SaveData().candles(symbol, save_data)
    else:
        candles_data = getCurrentTick(symbol, resolution, '1605629727')
        SaveData().candles(symbol, candles_data)


def getArgs(argv):
    help_info = "loaddata.py -r <1 or D> -a <rest or api>"
    try:
        opts, _ = getopt.getopt(argv, "hr:a:", ["resolution="])
    except getopt.GetoptError:
        print(help_info)
        sys.exit(2)
    if(len(opts) == 0):
        print(help_info)
        sys.exit(2)

    args = {}
    for opt, arg in opts:
        if opt == '-h':
            print(help_info)
            sys.exit()
        elif opt in ("-r", "--resolution"):
            if(arg != "1" and arg != "D"):
                print(help_info)
            else:
                args['resolution'] = arg
        elif opt in ("-a", "--api"):
            if(arg != "api" and arg != "rest"):
                print(help_info)
            else:
                args['api'] = arg
    return args


if __name__ == "__main__":
    args = getArgs(sys.argv[1:])
    resolution = args['resolution']
    api = 'api'
    if ('api' in args):
        api = args['api']

    s = sched.scheduler(time.time, time.sleep)
    logging.debug("Getting candle data in " + resolution)
    with open('sec_list_1000.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        delay = 0
        for row in spamreader:
            s.enter(delay, 1, load_and_etl, argument=(row[0], resolution, api))
            delay = delay + 1
            if(delay % 60 == 59):  # just in case
                delay = delay + 1
    s.run()

    with SMS() as sms:
        sms.client.messages \
                .create(
                     body="Load data from finnhub done.",
                     from_='+12566693745',
                     to='+16475220400'
                 )
