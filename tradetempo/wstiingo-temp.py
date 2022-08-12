import asyncio
import configparser
import datetime
import functools
import logging
import os
import signal
import sys
import time
import traceback

import simplejson as json
import uvloop
import websockets
from bson import Decimal128, Int64
from motor.motor_asyncio import AsyncIOMotorClient

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

os.chdir(sys.path[0])

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# file_handler = logging.FileHandler("logs/wstiingo.log")
file_handler = logging.FileHandler("../logs/wstiingo.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

config = configparser.RawConfigParser()
# config.read(".credentials.cfg")
config.read("../.credentials.cfg")
# auth_header = {"authorization": config["tiingo"]["api_key"]}
tiingo_key = config["tiingo"]["api_key"]
# config.read("settings.cfg")
config.read("../settings.cfg")

_db = None
subscription_id = None


async def message_router(message):
    processed_nano = time.time_ns()

    message_type = message["messageType"]
    if message_type == "A":
        data = message["data"]

        quote_data = {
            "updateType": data[0],
            "timestamp": data[1],
            "timestampNano": data[2],
            "ticker": data[3],
            "bidSize": data[4],
            "bidPrice": data[5],
            "midPrice": data[6],
            "askPrice": data[7],
            "askSize": data[8],
            "lastPrice": data[9],
            "lastSize": data[10],
            "halted": data[11],
            "afterHours": data[12],
            "intermarketSweepOrder": data[13],
            "oddlot": data[14],
            "nmsRule611": data[15],
            "processedNano": processed_nano
        }

    elif message_type == "H":
        logger.debug(f"Heartbeat @ {processed_nano}")

    elif message_type == "I":
        if "subscriptionId" in message["data"]:
            global _subscription_id
            _subscription_id = message["data"]["subscription_id"]
        else:
            logger.warning(
                f"Unknown data: {message['data']} in messageType=I @ {processed_nano}"
            )

    elif message_type == "U":
        pass

    elif message_type == "E":
        logger.error(f"Encountered")

    elif message_type == "D":
        pass


async def consumer_handler(websocket: websockets.WebSocketClientProtocol):
    global _db
    _db = AsyncIOMotorClient(
        host=config["mongodb"]["host"],
        port=int(config["mongodb"]["port"]),
        directConnection=True,
    )[config["mongodb"]["db"]]

    async for message in websocket:
        await message_router(message=json.loads(message))


async def consume(subscription_request):
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(
        signal.SIGTERM, functools.partial(stop_stream, signal.SIGTERM, loop)
    )

    async for websocket in websockets.connect(
        "wss://api.tiingo.com/iex", compression=None
    ):
        try:
            logger.debug(
                "Connection established. Sending subscription request: {}".format(
                    subscription_request
                )
            )
            await websocket.send(json.dumps(subscription_request))
            await consumer_handler(websocket)

        except websockets.ConnectionClosed:
            logger.debug("Continuing after encountering websockets.ConnectionClosed.")
            continue


def stop_stream(signame, loop):
    logger.debug(f"Got signal {signame}. Stopping event loop.")
    loop.stop()


def start_stream(tickers):
    if type(tickers) != list:
        tickers = [tickers]

    ws_request = {
        "eventName": "subscribe",
        "authorization": tiingo_key,
        "eventData": {"thresholdLevel": 5, "tickers": tickers},
    }
    logger.debug(f"ws_request: {ws_request}")

    try:
        asyncio.run(consume(ws_request))

    except KeyboardInterrupt:
        logger.info("Exit signal received.")


if __name__ == "__main__":
    start_stream(tickers=["tqqq", "qqq", "spy", "uso", "ndaq", "dja"])
