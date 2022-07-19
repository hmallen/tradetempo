import asyncio
import json
import logging
import websockets

from dydx3.constants import WS_HOST_MAINNET

logger = logging.getLogger()


class dydxWebsocket:

    def __init__(self, assets):
        # Currently only handling a single asset
        self.request_trades = {
            'type': 'subscribe',
            'channel': 'v3_trades',
            'id': 'BTC-USD'
        }

    async def main(self):
        # Note: This doesn't work with Python 3.9.
        async with websockets.connect(WS_HOST_MAINNET) as websocket:

            await websocket.send(json.dumps(self.request_trades))
            logger.debug(f'> {self.request_trades}')

            while True:
                res = await websocket.recv()
                print(f'< {res}')


if __name__ == '__main__':
    dydx_ws = dydxWebsocket(assets='btc')

    # asyncio.get_event_loop().run_until_complete(dydxWebsocket.main())
    asyncio.run(dydx_ws.main())
