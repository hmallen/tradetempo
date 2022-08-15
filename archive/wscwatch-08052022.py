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

import cryptowatch as cw

import traceback

from google import protobuf
from google.protobuf.json_format import MessageToJson

# from cryptowatch.utils import log
from cryptowatch.errors import APIKeyError
from cryptowatch.utils import forge_stream_subscription_payload
from cryptowatch.stream.proto.public.stream import stream_pb2
from cryptowatch.stream.proto.public.client import client_pb2

from tradetempo.utils.cwatchhelper import MarketInfo

os.chdir(sys.path[0])

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("logs/wscwatch.log")
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

config = configparser.RawConfigParser()
config.read(".credentials.cfg")
cw.api_key = config["cryptowatch"]["api_key"]
config.read("settings.cfg")

_db = None


async def message_router(message):
    try:
        if message == b"\x01":
            logger.debug(f"Heartbeat received: {message}")
            return

        stream_message = stream_pb2.StreamMessage()
        stream_message.ParseFromString(message)

        if str(stream_message.marketUpdate.intervalsUpdate):
            await intervals_update(stream_message)
        elif str(stream_message.marketUpdate.tradesUpdate):
            await trades_update(json.loads(MessageToJson(stream_message)))
        elif str(stream_message.marketUpdate.orderBookUpdate):
            await orderbook_snapshot_update(stream_message)
        elif str(stream_message.marketUpdate.orderBookDeltaUpdate):
            await orderbook_delta_update(stream_message)
        elif str(stream_message.marketUpdate.orderBookSpreadUpdate):
            await orderbook_spread_update(stream_message)
        else:
            logger.debug(stream_message)

    except protobuf.message.DecodeError as ex:
        logger.error("Could not decode this message: {}".format(message))
        logger.error(traceback.format_exc())
    except Exception as ex:
        logger.error(traceback.format_exc())


async def trades_update(trades_update):
    received_timestamp = time.time_ns()
    exchange_id = Int64(trades_update["marketUpdate"]["market"]["exchangeId"])
    market_id = Int64(trades_update["marketUpdate"]["market"]["marketId"])
    currency_pair_id = Int64(trades_update["marketUpdate"]["market"]["currencyPairId"])

    for trade in trades_update["marketUpdate"]["tradesUpdate"]["trades"]:
        trade_formatted = {
            "receivedTimestamp": received_timestamp,
            "exchangeId": exchange_id,
            "marketId": market_id,
            "currencyPairId": currency_pair_id,
            # "market": XYZ,
            # "exchange": XYZ,
            # "base": XYZ,
            # "quote": XYZ,
            "side": trade["orderSide"].rstrip("SIDE").lower(),
            "price": Decimal128(trade["priceStr"]),
            "amount": Decimal128(trade["amountStr"]),
            "timestamp": datetime.datetime.fromtimestamp(int(trade["timestamp"])),
            "timestampNano": Int64(trade["timestampNano"]),
            "priceStr": trade["priceStr"],
            "amountStr": trade["amountStr"],
            "externalId": trade["externalId"],
        }

        global _db
        insert_result = await _db[config["mongodb"]["collection"]].insert_one(
            trade_formatted
        )
        logger.debug(f"insert_result.inserted_id: {insert_result.inserted_id}")


async def intervals_update(intervals_update):
    pass


async def orderbook_spread_update(orderbook_spread_update):
    pass


async def orderbook_delta_update(orderbook_delta_update):
    pass


async def orderbook_snapshot_update(orderbook_snapshot_update):
    pass


async def consumer_handler(websocket: websockets.WebSocketClientProtocol):
    global _db
    _db = AsyncIOMotorClient(
        host=config["mongodb"]["host"],
        port=int(config["mongodb"]["port"]),
        directConnection=True,
    )[config["mongodb"]["db"]]

    try:
        loop = asyncio.get_running_loop()
        stop = loop.create_future()
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
    except NotImplementedError:
        logger.warning("Windows sucks and won't add the signal handler.")

    async for message in websocket:
        # try:
        # asyncio.create_task(message_router(message))
        await message_router(message)

        # except asyncio.CancelledError:
        #     logger.debug("CancelledError raised.")
        #     continue

        # except KeyboardInterrupt:
        #     logger.info('Exit signal received.')
        #     break
        #
        # Cleanup?? --> Tasks, Loop, Mongo, Signals


async def consume(ws_url, subs_payload):
    exception_count = 0
    async for websocket in websockets.connect(ws_url, compression=None):
        try:
            logger.debug(
                "Connection established. Sending subscriptions payload: {}".format(
                    subs_payload
                )
            )
            await websocket.send(subs_payload)
            await consumer_handler(websocket)

        except asyncio.exceptions.CancelledError as e:
            exception_count += 1
            logger.exception(
                f"CancelledError raised: {e}\nCount = {exception_count}\nTraceback: {traceback.format_exc()}"
            )
            continue


def start_stream(assets, count):
    market_info = MarketInfo()
    top_markets = market_info.get_top_markets(assets=assets, count=count)
    subscription_list = market_info.build_subscriptions(
        sub_type="trades", markets=[mkt["id"] for mkt in top_markets]
    )

    if cw.api_key:
        DSN = "{}?apikey={}&format=binary".format(cw.ws_endpoint, cw.api_key)
    else:
        raise APIKeyError(
            "An API key is required to use the Cryptowatch Websocket API.\n"
            "You can create one at https://cryptowat.ch/account/api-access"
        )
    logger.debug("DSN used: {}".format(DSN))

    subs_payload = forge_stream_subscription_payload(subscription_list, client_pb2)
    logger.debug(f"subs_payload: {subs_payload}")

    # try:
    asyncio.run(consume(DSN, subs_payload))

    # except KeyboardInterrupt:
    #     logger.info("Exit signal received.")


if __name__ == "__main__":
    start_stream(["btc", "eth"], count=4)
