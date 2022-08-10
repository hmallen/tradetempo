import asyncio
from bson import Decimal128
import configparser
import datetime
import json
import logging
import os
import signal
import sys
import time
import traceback

import websockets

from motor.motor_asyncio import AsyncIOMotorClient
from dydx3.constants import WS_HOST_MAINNET

import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

os.chdir(sys.path[0])

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("logs/wsdydx.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

config_path = "settings.cfg"
config = configparser.RawConfigParser()
config.read(config_path)

_db = None

## AsyncIO Functions ##


async def process_trade(trade_message):
    received_timestamp = time.time_ns()

    id = trade_message["id"]
    exchange = "dydx"
    market = trade_message["id"].lower()
    currencies = trade_message["id"].split("-")
    base_currency = currencies[0].lower()
    quote_currency = currencies[1].lower()

    for trade in trade_message["contents"]["trades"]:
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
            "side": trade["side"].lower(),
            "base": base_currency,
            "quote": quote_currency,
            "liquidation": trade["liquidation"],
        }

        global _db
        insert_result = await _db[config["mongodb"]["collection"]].insert_one(
            trade_formatted
        )
        logger.debug(f"insert_result.inserted_id: {insert_result.inserted_id}")


async def consumer_handler(websocket: websockets.WebSocketClientProtocol):
    global _db
    _db = AsyncIOMotorClient(
        host=config["mongodb"]["host"],
        port=int(config["mongodb"]["port"]),
        directConnection=True,
    )[config["mongodb"]["db"]]

    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, loop.create_task, websocket.close())
    except NotImplementedError:
        logger.warning("Windows sucks and won't add the signal handler.")

    async for message in websocket:
        # try:
        trade_json = json.loads(message)

        if trade_json["type"] == "channel_data":
            asyncio.create_task(process_trade(trade_message=trade_json))

        # except asyncio.CancelledError:
        #     logger.debug("CancelledError raised.")
        #     break


async def consume(subscription_request):
    exception_count = 0
    async for websocket in websockets.connect(WS_HOST_MAINNET, compression=None):
        try:
            await websocket.send(json.dumps(subscription_request))
            await consumer_handler(websocket)

        except asyncio.CancelledError as e:
            exception_count += 1
            logger.exception(
                f"CancelledError raised: {e}\nCount = {exception_count}\nTraceback: {traceback.format_exc()}"
            )
            continue


def start_stream(asset):
    ws_request = {
        "type": "subscribe",
        "channel": "v3_trades",
        "id": f"{asset.upper()}-USD",
    }
    logger.debug(f"ws_request: {ws_request}")

    try:
        asyncio.run(consume(ws_request))

    except KeyboardInterrupt:
        logger.info("Exit signal received.")


if __name__ == "__main__":
    start_stream("BTC")
