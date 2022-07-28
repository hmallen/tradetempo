import asyncio
import configparser
import datetime
import json
import logging
import signal
import time
import websockets

from dydx3.constants import WS_HOST_MAINNET
from pymongo import MongoClient
from bson import Decimal128

from pprint import pprint

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

config = configparser.RawConfigParser()
config.read("settings.cfg")

db = MongoClient(
    host=config["mongodb"]["host"],
    port=int(config["mongodb"]["port"]),
    directConnection=True,
)[config["mongodb"]["db"]]
trades = db[config["mongodb"]["collection"]]

max_len = {"exchange": 4, "amount": 0, "base": 0, "price": 0, "quote": 0, "side": 4}


def build_subscription(trades_msg):
    sub_ready = None

    try:
        for trade in trades_msg["contents"]["trades"]:
            if len(trade["size"]) > max_len["amount"]:
                max_len["amount"] = len(trade["size"])
            if len(trades_msg["id"].split("-")[0]) > max_len["base"]:
                max_len["base"] = len(trades_msg["id"].split("-")[0])
            if len(trade["price"]) > max_len["price"]:
                max_len["price"] = len(trade["price"])
            if len(trades_msg["id"].split("-")[1]) > max_len["quote"]:
                max_len["quote"] = len(trades_msg["id"].split("-")[1])
            logger.debug(f"(loop)  max_len: {max_len}")
        logger.debug(f"(final) Smax_len: {max_len}")

        sub_ready = True

    except Exception as e:
        logger.exception(e)

        sub_ready = False

    finally:
        return sub_ready


def print_trade(trade_formatted):
    # print(f"{trade_formatted['orderSide']:{max_len['side']}} | {trade_formatted['baseCurrency'].upper():{max_len['base']}} | {str(trade_formatted['amount']):{max_len['amount']}} @ {str(trade_formatted['price']):{max_len['price']}} {trade_formatted['quoteCurrency'].upper():{max_len['quote']}} | {trade_formatted['exchange']:{max_len['exchange']}} | {trade_formatted['market']}")
    print(
        f"{trade_formatted['orderSide']:{max_len['side']}} | {trade_formatted['baseCurrency'].upper():{max_len['base']}} | {str(trade_formatted['amount']):{max_len['amount']}} @ {'{:.2f}'.format(trade_formatted['price'].to_decimal()):{max_len['price'] + 2}} {trade_formatted['quoteCurrency'].upper():{max_len['quote']}} | {trade_formatted['exchange']:{max_len['exchange']}} | {trade_formatted['market']}"
    )


async def message_handler(msg):
    trade_json = json.loads(msg)

    if trade_json["type"] == "channel_data":
        received_timestamp = time.time_ns()
        id = trade_json["id"]
        exchange = "DYDX"
        market = trade_json["id"]
        currencies = trade_json["id"].split("-")
        base_currency = currencies[0]
        quote_currency = currencies[1]

        for trade in trade_json["contents"]["trades"]:
            trade_formatted = {
                "received_timestamp": received_timestamp,
                "id": id,
                "exchange": exchange,
                "market": market,
                "timestamp": datetime.datetime.fromisoformat(
                    trade["createdAt"].rstrip("Z")
                ),
                "price": Decimal128(trade["price"]),
                "amount": Decimal128(trade["size"]),
                "orderSide": trade["side"],
                "baseCurrency": base_currency,
                "quoteCurrency": quote_currency,
                "liquidation": trade["liquidation"],
            }

            insert_result = trades.insert_one(trade_formatted)

            logger.debug(f"insert_result.inserted_id: {insert_result.inserted_id}")

            print_trade(trade_formatted)

    elif trade_json["type"] == "subscribed":
        if build_subscription(trade_json):
            logger.info("Calculated lengths for trade print output.")
        else:
            logger.error("Error while calculating lengths for trade print output.")


async def ws_client(asset):
    ws_request = {
        "type": "subscribe",
        "channel": "v3_trades",
        "id": f"{asset.upper()}-USD",
    }
    logger.debug(f"ws_request: {ws_request}")

    async with websockets.connect(WS_HOST_MAINNET) as websocket:
        await websocket.send(json.dumps(ws_request))

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, loop.create_task, websocket.close())

        async for message in websocket:
            await message_handler(message)


if __name__ == "__main__":
    try:
        asyncio.run(ws_client("BTC"))

    except KeyboardInterrupt:
        logger.info("Exit signal received.")
