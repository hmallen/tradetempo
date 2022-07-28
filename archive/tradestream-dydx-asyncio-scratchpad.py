import asyncio
import json
import logging
import signal
import websockets

from dydx3.constants import WS_HOST_MAINNET

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


"""async def client():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as websocket:
        # Close the connection when receiving SIGTERM.
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGTERM, loop.create_task, websocket.close())

        # Process messages received on the connection.
        async for message in websocket:
            pass

asyncio.run(client())

async def consumer_handler(websocket):
    async for message in websocket:
        await consumer(message)

async for websocket in websockets.connect(...):
    try:
        pass
    except websockets.ConnectionClosed:
        continue"""

from pprint import pprint


async def trade_handler(msg):
    pprint(msg)


async def ws_client(asset):
    ws_request = {
        "type": "subscribe",
        "channel": "v3_trades",
        "id": f"{asset.upper()}-USD",
    }
    logger.debug(f"ws_request: {ws_request}")

    async with websockets.connect(WS_HOST_MAINNET) as websocket:
        await websocket.send(json.dumps(ws_request))

        # loop = asyncio.get_running_loop()
        # loop.add_signal_handler(
        #     signal.SIGTERM, loop.create_task, websocket.close())

        # loop.run_until_complete(self.loop.create_task(self._websocket.close()))\

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, loop.create_task, websocket.close())

        # while True:
        # try:
        async for message in websocket:
            await trade_handler(message)

        # loop.run_until_complete()

        """except websockets.ConnectionClosed:
            logger.debug("ConnectionClosed")
            # continue

        except websockets.ConnectionClosedOK:
            logger.debug("ConnectionClosedOK")
            # break"""


try:
    # asyncio.run(ws_client("BTC"))
    # loop = asyncio.get_event_loop()
    # loop.add_signal_handler(signal.SIGTERM, loop.create_task, websocket.cl)
    # loop.run_until_complete(ws_client("BTC"))
    # loop.run_until_complete(loop.create_task(websockets.close()))
    asyncio.run(ws_client("BTC"))

except KeyboardInterrupt:
    logger.info("Exit signal received.")
    # asyncio.create_task(websockets.close())
