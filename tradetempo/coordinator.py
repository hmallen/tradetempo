import configparser
import logging
import sys

from tradetempo.cwatchhelper import MarketInfo

logging.basicConfig(
    format="%(asctime)s-%(name)s-%(levelname)s: %(message)s")
# logging.getLogger("cryptowatch").setLevel(logging.DEBUG)

logger = logging.getLogger('__main__')
logger.setLevel(logging.DEBUG)
# stream_handler = logging.StreamHandler()
# stream_handler.setLevel(logging.DEBUG)
# logger.addHandler(stream_handler)

config = configparser.RawConfigParser()
config.read('settings.cfg')
sub_count = int(config['cryptowatch']['subscription_count'])
monitored_assets = config['cryptowatch']['monitored_assets']
if ',' in monitored_assets:
    monitored_assets = monitored_assets.split(',')
    monitored_assets = [asset.rstrip(' ') for asset in monitored_assets]
logger.debug(f"monitored_assets: {monitored_assets}")


if __name__ == '__main__':
    market_info = MarketInfo()
    top_markets = market_info.get_top_markets(
        monitored_assets, count=sub_count)
