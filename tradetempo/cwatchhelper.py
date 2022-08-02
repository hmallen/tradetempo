import logging
import os
import sys
import traceback
import requests

os.chdir(sys.path[0])

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


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
    
    def build_subscriptions(self, sub_type, markets):
        if type(markets) != list:
            markets = [markets]
        
        subscription_list = []

        if sub_type == 'trades':
            [subscription_list.append(f"markets:{mkt['id']}:trades") for mkt in markets]

        else:
            logger.error(f'Unrecognized or unimplemented subscription type: {sub_type}')
        
        # except:
        #     logger.exception(traceback.format_exc())
        
        # finally:
        return subscription_list    
