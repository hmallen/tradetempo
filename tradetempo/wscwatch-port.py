# import websocket
import traceback
# import threading
from google import protobuf

import asyncio
import configparser
import json
import logging
from motor.motor_asyncio import AsyncIOMotorClient
import os
import signal
import sys
import websockets

import cryptowatch as cw
# from cryptowatch.utils import log
from cryptowatch.errors import APIKeyError
from cryptowatch.utils import forge_stream_subscription_payload
from cryptowatch.stream.proto.public.stream import stream_pb2
from cryptowatch.stream.proto.public.client import client_pb2

os.chdir(sys.path[0])

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

config = configparser.RawConfigParser()
config.read('../settings.cfg')


def _on_open(ws):
    subs_payload = forge_stream_subscription_payload(subscriptions, client_pb2)
    logger.debug(
        "Connection established. Sending subscriptions payload: {}".format(
            subs_payload
        )
    )
    ws.send(subs_payload)


def _on_error(ws, error):
    logger.error(str(error))


def _on_close(ws, close_status_code, close_reason):
    logger.debug("Connection closed.")


def on_market_update(message):
    try:
        if message == b"\x01":
            logger.debug(f"Heartbeat received: {message}")
            return
        stream_message = stream_pb2.StreamMessage()
        stream_message.ParseFromString(message)
        if str(stream_message.marketUpdate.intervalsUpdate):
            on_intervals_update(stream_message)
        elif str(stream_message.marketUpdate.tradesUpdate):
            on_trades_update(stream_message)
        elif str(stream_message.marketUpdate.orderBookUpdate):
            on_orderbook_snapshot_update(stream_message)
        elif str(stream_message.marketUpdate.orderBookDeltaUpdate):
            on_orderbook_delta_update(stream_message)
        elif str(stream_message.marketUpdate.orderBookSpreadUpdate):
            on_orderbook_spread_update(stream_message)
        else:
            logger.debug(stream_message)
    except protobuf.message.DecodeError as ex:
        logger.error("Could not decode this message: {}".format(message))
        logger.error(traceback.format_exc())
    except Exception as ex:
        logger.error(traceback.format_exc())


def on_trades_update(trades_update):
    pass


def on_intervals_update(intervals_update):
    pass


def on_orderbook_spread_update(orderbook_spread_update):
    pass


def on_orderbook_delta_update(orderbook_delta_update):
    pass


def on_orderbook_snapshot_update(orderbook_snapshot_update):
    pass


async def consumer_handler(websocket: websockets.WebSocketClientProtocol):
    db = AsyncIOMotorClient(
        host=config["mongodb"]["host"],
        port=int(config["mongodb"]["port"]),
        directConnection=True,
    )[config["mongodb"]["db"]]
    trade_collection = db[config["mongodb"]["collection"]]

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, loop.create_task, websocket.close())

    async for message in websocket:
        try:
            print(message.decode())
            """trade_json = json.loads(message)

            print(json.dumps(trade_json, indent=4))

            if trade_json["type"] == "channel_data":
                await asyncio.create_task(
                    process_trade(
                        trade_message=trade_json,
                        mongo_collection=trade_collection,
                    )
                )"""

        except asyncio.CancelledError:
            logger.debug("CancelledError raised.")
            break



async def consume(ws_url, subscription_list):
    subs_payload = forge_stream_subscription_payload(subscription_list, client_pb2)
    logger.debug(f"subs_payload: {subs_payload.decode()}")

    async with websockets.connect(ws_url) as websocket:
        logger.debug(
            "Connection established. Sending subscriptions payload: {}".format(
                subs_payload
            )
        )
        # await websocket.send(json.dumps(subs_payload))
        await websocket.send(subs_payload)
        await consumer_handler(websocket)


def start_stream(subscription_list):
    if cw.api_key:
        DSN = "{}?apikey={}&format=binary".format(
            cw.ws_endpoint, cw.api_key
        )
    else:
        raise APIKeyError(
            "An API key is required to use the Cryptowatch Websocket API.\n"
            "You can create one at https://cryptowat.ch/account/api-access"
        )
    logger.debug("DSN used: {}".format(DSN))

    try:
        asyncio.run(consume(DSN, subscription_list))
    
    except KeyboardInterrupt:
        logger.info("Exit signal received.")

    """global _ws
    _ws = websocket.WebSocketApp(
        DSN,
        on_message=on_market_update,
        on_error=_on_error,
        on_close=_on_close,
        on_open=_on_open,
    )
    wst = threading.Thread(
        target=_ws.run_forever,
        # kwargs={"ping_timeout": ping_timeout, "ping_interval": ping_interval},
        kwargs=keyword_args,
    )
    wst.daemon = False
    wst.start()
    if use_rel:
        rel.signal(2, rel.abort)  # Keyboard Interrupt
        rel.dispatch()

    logger.debug(
        "Ping timeout used: {}. Ping interval used: {}".format(
            ping_timeout, ping_interval
        ),
        is_debug=True,
    )


def disconnect():
    global _ws
    _ws.close()"""


if __name__ == '__main__':
    from cwatchhelper import MarketInfo

    market_info = MarketInfo()
    top_markets = market_info.get_top_markets(assets=["btc", "eth"], count=4)
    subscriptions = market_info.build_subscriptions(
        sub_type='trades',
        markets=[mkt['id'] for mkt in top_markets]
    )

    start_stream(subscriptions)
