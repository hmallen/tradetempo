import asyncio
import json
import logging
import websockets

from dydx3.constants import WS_HOST_MAINNET

logger = logging.getLogger()


class dydxWebsocket:
    def __init__(self, asset):
        # Currently only handles a single asset
        self.request_trades = {
            "type": "subscribe",
            "channel": "v3_trades",
            "id": f"{asset.upper()}-USD",
        }

    async def main(self):
        # Note: This doesn't work with Python 3.9.
        async with websockets.connect(WS_HOST_MAINNET) as websocket:

            await websocket.send(json.dumps(self.request_trades))
            logger.debug(f"> {self.request_trades}")

            while True:
                try:
                    res = await websocket.recv()
                    print(f"< {res}")

                except websockets.ConnectionClosed:
                    continue

    """async def msg_handler(self, websocket):
        while True:
            msg = await websocket.recv()
            print(msg)
    
    async def main(self):
        async with websockets.serve(dydxWebsocket.msg_handler, WS_HOST_MAINNET):
            await asyncio.Future()"""

    def start_stream(self):
        # asyncio.get_event_loop().run_until_complete(dydxWebsocket.main())
        asyncio.run(dydxWebsocket.main(self))


if __name__ == "__main__":
    dydx_ws = dydxWebsocket(assets="btc")
    dydx_ws.start_stream()
