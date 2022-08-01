import configparser
import cryptowatch as cw
# import datetime
from bson import Decimal128, Int64
# import json
import logging
import time
import sys

# from google.protobuf.json_format import MessageToJson
from pymongo import MongoClient

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


config = configparser.RawConfigParser()
config.read(".credentials.cfg")
cw.api_key = config["cryptowatch"]["api_key"]
config.read("settings.cfg")

db = MongoClient(
    host=config["mongodb"]["host"],
    port=int(config["mongodb"]["port"]),
    directConnection=True,
)[config["mongodb"]["db"]]
trades = db[config["mongodb"]["collection"]]


def handle_trades_update(trade_update):
    received_timestamp = time.time_ns()

    market_msg = ">>> Market#{} Exchange#{} Pair#{}: {} New Trades".format(
        trade_update.marketUpdate.market.marketId,
        trade_update.marketUpdate.market.exchangeId,
        trade_update.marketUpdate.market.currencyPairId,
        len(trade_update.marketUpdate.tradesUpdate.trades),
    )
    print(market_msg)
    for trade in trade_update.marketUpdate.tradesUpdate.trades:
        trade_msg = "\tID:{} TIMESTAMP:{} TIMESTAMPNANO:{} PRICE:{} AMOUNT:{}".format(
            trade.externalId,
            trade.timestamp,
            trade.timestampNano,
            trade.priceStr,
            trade.amountStr,
        )
        print(trade_msg)

    # print_trade(trade_formatted)


cw.stream.on_trades_update = handle_trades_update


def start_stream():
    cw.stream.connect()


if __name__ == "__main__":
    start_stream()
