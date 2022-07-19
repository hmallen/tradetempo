'''Example for onboarding an account and accessing private endpoints.

Usage: python -m examples.onboard
'''

from dydx3 import Client
from dydx3.constants import API_HOST_ROPSTEN
from dydx3.constants import NETWORK_ID_ROPSTEN
from web3 import Web3

# Ganache test address.
# ETHEREUM_ADDRESS = '0x22d491Bde2303f2f43325b2108D26f1eAbA1e32b'
ETHEREUM_ADDRESS = '0xea598540A5f16fEa962cCf16D6c6a5b3A2a08E28'

# Ganache node.
WEB_PROVIDER_URL = 'http://127.0.0.1:8545'

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
    default_ethereum_address='0x48f5Bed6a6425602EF1bAd9c26835746D45f869a',
    web3=Web3(Web3.HTTPProvider(WEB_PROVIDER_URL)),
)

# Set STARK key.
stark_key_pair_with_y_coordinate = client.onboarding.derive_stark_key()
client.stark_private_key = stark_key_pair_with_y_coordinate['private_key']
(public_x, public_y) = (
    stark_key_pair_with_y_coordinate['public_key'],
    stark_key_pair_with_y_coordinate['public_key_y_coordinate'],
)

# Onboard the account.
onboarding_response = client.onboarding.create_user(
    stark_public_key=public_x,
    stark_public_key_y_coordinate=public_y,
)
print('onboarding_response', onboarding_response)

# Query a private endpoint.
accounts_response = client.private.get_accounts()
print('accounts_response', accounts_response)
