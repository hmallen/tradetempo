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
import websockets
from bson import Decimal128, Int64
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import CollectionInvalid

from pathlib import Path

os.chdir(f"{Path(__file__).resolve().parent}/..")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("logs/wstiingo.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if sys.platform != "win32":
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    logger.warning("Module uvloop not compatible with Windows. Skipping import.")

config = configparser.RawConfigParser()
config.read(".credentials.cfg")
# config.read("../.credentials.cfg")
tiingo_key = config["tiingo"]["api_key"]
config.read("settings.cfg")
# config.read("../settings.cfg")

trades_collection = config["mongodb"]["wstiingo_collection"]
latency_collection = config["mongodb"]["latency_collection"]

_db = None
subscription_id = None


async def log_latency(websocket):
    while True:
        t0 = time.perf_counter()
        pong_waiter = await websocket.ping()
        await pong_waiter
        t1 = time.perf_counter()
        latency = t1 - t0
        logger.debug("Connection latency: %.3f seconds", latency)
        _db[latency_collection].insert_one(
            {
                "timestamp": datetime.datetime.utcnow(),
                "source": f"{Path(__file__)}",
                "latency": latency,
            }
        )

        await asyncio.sleep(int(config["logging"]["log_latency_interval"]))


async def message_router(message):
    timestamp = datetime.datetime.utcnow().isoformat()

    timeseries_message = {"metadata": {}, "timestamp": timestamp, "data": {}}

    message_type = message["messageType"]
    if message_type == "A":
        data = message["data"]

        timeseries_message["metadata"] = {
            "messageType": message_type,
            "exchange": message["service"],
            "updateType": data[0],
            "ticker": data[3],
        }

        timeseries_message["data"] = {
            "timestamp": data[1],
            "timestampNano": data[2],
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
        }

        insert_result = await _db[trades_collection].insert_one(timeseries_message)
        logger.debug(f"insert_result.inserted_id: {insert_result.inserted_id}")

    elif message_type == "H":
        logger.debug(f"Heartbeat @ {timestamp}")

    elif message_type == "I":
        if "subscriptionId" in message["data"]:
            global _subscription_id
            _subscription_id = message["data"]["subscriptionId"]
        else:
            logger.warning(
                f"Unknown data: {message['data']} in messageType=I @ {timestamp}"
            )

    elif message_type == "U":
        logger.info(f"Received info message: {message}")

    elif message_type == "E":
        logger.error(f"Received error message: {message}")

    elif message_type == "D":
        logger.info(f"Receivfed delete message: {message}")


async def consumer_handler(websocket: websockets.WebSocketClientProtocol):
    global _db
    _db = AsyncIOMotorClient(
        host=config["mongodb"]["host"],
        port=int(config["mongodb"]["port"]),
        directConnection=True,
        retryWrites=False,
    )[config["mongodb"]["db"]]

    try:
        await _db.create_collection(
            trades_collection,
            timeseries={
                "timeField": "timestamp",
                "metaField": "metadata",
                "granularity": "seconds",
            },
        )
        logger.info("Created new timeseries collection.")
    except CollectionInvalid:
        logger.info("Found existing timeseries collection.")

    async for message in websocket:
        await message_router(message=json.loads(message))


async def consume(subscription_request):
    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        loop.add_signal_handler(
            signal.SIGTERM, functools.partial(stop_stream, signal.SIGTERM, loop)
        )

    async for websocket in websockets.connect(
        "wss://api.tiingo.com/iex", compression=None
    ):
        try:
            asyncio.create_task(log_latency(websocket))

            logger.debug(
                f"Connection established. Sending subscription request: {subscription_request}"
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
