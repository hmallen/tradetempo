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

# from websockets import WebSocketClientProtocol
import websockets

from motor.motor_asyncio import AsyncIOMotorClient
from dydx3.constants import WS_HOST_MAINNET

os.chdir(sys.path[0])

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

config_path = "../settings.cfg"
config = configparser.ConfigParser()
config.read(config_path)

_db = None

## AsyncIO Functions ##


async def process_trade(db, trade_message):
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

        insert_result = await db[config["mongodb"]["collection"]].insert_one(
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
    # trade_collection = _db[config["mongodb"]["collection"]]

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, loop.create_task, websocket.close())

    async for message in websocket:
        try:
            trade_json = json.loads(message)

            if trade_json["type"] == "channel_data":
                await asyncio.create_task(process_trade(trade_message=trade_json))

        except asyncio.CancelledError:
            logger.debug("CancelledError raised.")
            break


async def consume(subscription_request):
    async with websockets.connect(WS_HOST_MAINNET) as websocket:
        await websocket.send(json.dumps(subscription_request))
        await consumer_handler(websocket)


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
