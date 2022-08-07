import asyncio
import configparser

import datetime

import json
import logging
from motor.motor_asyncio import AsyncIOMotorClient
import os
import signal
import sys
import time
import websockets

from bson import Decimal128, Int64

import traceback

import functools

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

file_handler = logging.FileHandler("logs/wstiingo.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

config = configparser.RawConfigParser()
config.read(".credentials.cfg")
tiingoapi_key = config["tiingo"]["api_key"]
config.read("settings.cfg")

_db = None


async def message_router(message):
    global _db


async def consumer_handler(websocket: websockets.WebSocketClientProtocol):
    global _db
    _db = AsyncIOMotorClient(
        host=config["mongodb"]["host"],
        port=int(config["mongodb"]["port"]),
        directConnection=True,
    )[config["mongodb"]["db"]]
    
    async for message in websocket:
        trade_json = json.loads(message)

        if trade_json["type"] == "channel_data":
            await message_router(message=trade_json)


async def consume(subscription_request):
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(
        signal.SIGTERM, functools.partial(stop_stream, signal.SIGTERM, loop))

    async for websocket in websockets.connect("wss://api.tiingo.com/iex", compression=None):
        try:
            logger.debug(
                "Connection established. Sending subscription request: {}".format(
                    subscription_request
                )
            )
            await websocket.send(json.dumps(subscription_request))
            await consumer_handler(websocket)
        
        except websockets.ConnectionClosed:
            logger.debug('Continuing after encountering websockets.ConnectionClosed.')
            continue


def stop_stream(signame, loop):
    logger.debug(f"Got signal {signame}. Stopping event loop.")
    loop.stop()

def start_stream(asset):
    ws_request = {
        'eventName':'subscribe',
        'authorization':'5a49e00626b92d679eb3b7465c7196a63364bf00',
        'eventData': {
            'thresholdLevel': 5
        }
    }

    logger.debug(f"ws_request: {ws_request}")

    try:
        asyncio.run(consume(ws_request))

    except KeyboardInterrupt:
        logger.info("Exit signal received.")


if __name__ == "__main__":
    start_stream("XYZ123FIXMEEEEEEEEEEEEEE")