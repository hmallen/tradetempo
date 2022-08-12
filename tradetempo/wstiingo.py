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

if sys.platform != "win32":
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    logger.warning('Module uvloop not compatible with Windows. Skipping import.')

config = configparser.RawConfigParser()
# config.read(".credentials.cfg")
config.read("../.credentials.cfg")
# auth_header = {"authorization": config["tiingo"]["api_key"]}
tiingo_key = config["tiingo"]["api_key"]
# config.read("settings.cfg")
config.read("../settings.cfg")

_db = None
subscription_id = None


async def log_latency(websocket):
    while True:
        t0 = time.perf_counter()
        pong_waiter = await websocket.ping()
        await pong_waiter
        t1 = time.perf_counter()
        logger.info("Connection latency: %.3f seconds", t1 - t0)

        await asyncio.sleep(10)


async def message_router(message):
    global _db
    print(json.dumps(message, indent=4))


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
    if sys.platform != "win32":
        loop.add_signal_handler(
            signal.SIGTERM, functools.partial(stop_stream, signal.SIGTERM, loop)
        )

    async for websocket in websockets.connect(
        "wss://api.tiingo.com/iex", compression=None
    ):
        try:
            asyncio.create_task(log_latency(websocket))

            logger.debug(f"Connection established. Sending subscription request: {subscription_request}")

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
