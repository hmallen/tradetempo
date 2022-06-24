import configparser
from bson import Int64
import cryptowatch as cw
import datetime
from bson import Decimal128
import json
import logging
import time
import sys

from google.protobuf.json_format import MessageToJson
from pymongo import MongoClient

# logging.basicConfig()
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')
logging.getLogger("cryptowatch").setLevel(logging.DEBUG)
# logger = logging.getLogger('__main__')
# logger.setLevel(logging.DEBUG)

config = configparser.RawConfigParser()
config.read('.credentials.cfg')

# Set your API Key
cw.api_key = config['cryptowatch']['api_key']

# cw.stream.subscriptions = ["markets:*:trades", "markets:*:ohlc"]
# cw.stream.subscriptions = ["assets:60:book:snapshots"]
# cw.stream.subscriptions = ["assets:60:book:spread"]
# cw.stream.subscriptions = ["assets:60:book:deltas"]
# cw.stream.subscriptions = ["markets:*:ohlc"]
# cw.stream.subscriptions = ["markets:*:trades"]
cw.stream.subscriptions = ["markets:579:trades"]

config.read('settings.cfg')

db = MongoClient(config['mongodb']['host'])[config['mongodb']['db']]
trades = db[config['mongodb']['collection']]


# What to do with each trade update
def handle_trades_update(trade_update):
    trade_json = json.loads(MessageToJson(trade_update))
    print(trade_json)

    for trade in trade_json['marketUpdate']['tradesUpdate']['trades']:
        trade_formatted = {
            "timestamp": datetime.datetime.fromtimestamp(int(trade['timestamp'])),
            "price": Decimal128(trade['priceStr']),
            "amount": Decimal128(trade['amountStr']),
            "timestampNano": Int64(trade['timestampNano']),
            "externalId": trade['externalId'],
            "orderSide": trade['orderSide'].strip('SIDE')
        }
        insert_result = trades.insert_one(trade_formatted)
        logging.debug(f"insert_result.inserted_id: {insert_result.inserted_id}")


# What to do with each candle update
def handle_intervals_update(interval_update):
    print(interval_update)


# What to do with each orderbook spread update
def handle_orderbook_snapshot_updates(orderbook_snapshot_update):
    print(orderbook_snapshot_update)


# What to do with each orderbook spread update
def handle_orderbook_spread_updates(orderbook_spread_update):
    print(orderbook_spread_update)


# What to do with each orderbook delta update
def handle_orderbook_delta_updates(orderbook_delta_update):
    print(orderbook_delta_update)


if __name__ == '__main__':
    logger = logging.getLogger('__main__')
    logger.setLevel(logging.DEBUG)
    
    cw.stream.on_orderbook_delta_update = handle_orderbook_delta_updates
    cw.stream.on_orderbook_spread_update = handle_orderbook_spread_updates
    cw.stream.on_orderbook_snapshot_update = handle_orderbook_snapshot_updates
    cw.stream.on_intervals_update = handle_intervals_update
    cw.stream.on_trades_update = handle_trades_update

    # Start receiving
    cw.stream.connect()

    while True:
        try:
            time.sleep(0.1)

        except KeyboardInterrupt:
            # Stop receiving
            cw.stream.disconnect()
            break

        except Exception as e:
            logger.exception(e)
            time.sleep(1)
