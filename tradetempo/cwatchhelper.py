import logging
import os
import sys
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MarketInfo:
    def __init__(self):
        pass

    def get_top_markets(self, assets, count=25):
        if type(assets) is not list:
            assets = list(assets)

        url = f"https://billboard.service.cryptowat.ch/markets?page=1&limit={count}&volumeInAssets=usd&sort=volume&sortAsset=usd&onlyBaseAssets={','.join(assets)}"
        logger.debug(f"url: {url}")

        result = requests.get(url)
        logger.debug(f"result: {result}")

        if result.status_code == 200:
            asset_info = result.json()["result"]["rows"]
        else:
            asset_info = f"Status Code: {result.status_code}"

        return asset_info
