"""Example for connecting to private WebSockets with an existing account.
Usage: python -m examples.websockets
"""

import asyncio
import json
import websockets

from dydx3 import Client
from dydx3.helpers.request_helpers import generate_now_iso
from dydx3.constants import API_HOST_ROPSTEN
from dydx3.constants import NETWORK_ID_ROPSTEN
from dydx3.constants import WS_HOST_ROPSTEN
from web3 import Web3

# Ganache test address.
ETHEREUM_ADDRESS = "0x48f5Bed6a6425602EF1bAd9c26835746D45f869a"

# Ganache node.
WEB_PROVIDER_URL = "http://127.0.0.1:8545"

NETWORK_ID = 3

"""client = Client(
    network_id=NETWORK_ID_ROPSTEN,
    host=API_HOST_ROPSTEN,
    default_ethereum_address=ETHEREUM_ADDRESS,
    web3=Web3(Web3.HTTPProvider(WEB_PROVIDER_URL)),
)"""

client = Client(
    network_id=NETWORK_ID,
    host=API_HOST_ROPSTEN,
    default_ethereum_address=ETHEREUM_ADDRESS,
    web3=Web3(Web3.HTTPProvider(WEB_PROVIDER_URL)),
)

now_iso_string = generate_now_iso()
signature = client.private.sign(
    request_path="/ws/accounts",
    method="GET",
    iso_timestamp=now_iso_string,
    data={},
)

req = {
    "type": "subscribe",
    "channel": "v3_accounts",
    "accountNumber": "0",
    "apiKey": client.api_key_credentials["key"],
    "passphrase": client.api_key_credentials["passphrase"],
    "timestamp": now_iso_string,
    "signature": signature,
}

req_trades = {"type": "subscribe", "channel": "v3_trades", "id": "ETH-USD"}


async def main():
    # Note: This doesn't work with Python 3.9.
    async with websockets.connect(WS_HOST_ROPSTEN) as websocket:

        await websocket.send(json.dumps(req_trades))
        print(f"> {req_trades}")

        while True:
            res = await websocket.recv()
            print(f"< {res}")


asyncio.get_event_loop().run_until_complete(main())
