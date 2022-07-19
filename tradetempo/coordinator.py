import configparser
from re import sub
from bson import Int64
import cryptowatch as cw
import datetime
from bson import Decimal128
import json
import logging
import time
import requests
import sys
from pprint import pprint

from google.protobuf.json_format import MessageToJson
from pymongo import MongoClient

from tradetempo.cwatchhelper import MarketInfo, SubscriptionManager

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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

config = configparser.RawConfigParser()
config.read('.credentials.cfg')
cw.api_key = config['cryptowatch']['api_key']


if __name__ == '__main__':
    market_info = MarketInfo()
    top_markets = market_info.get_top_markets(
        monitored_assets, count=sub_count)

    subscription_manager = SubscriptionManager()
    subscription_info = subscription_manager.build_subscriptions(top_markets)

    cw.stream.subscriptions = subscription_info['subscriptions']

    logger.info('Connecting to stream.')
    # Start receiving
    cw.stream.connect()

    exception_count = 0
    while True:
        try:
            time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info('Exit signal received.')
            # Stop receiving
            cw.stream.disconnect()
            logger.info('Disconnected from stream.')
            break

        except Exception as e:
            exception_count += 1
            logger.exception(e)
            with open('errors.log', 'a') as error_file:
                error_file.write(
                    f"{datetime.datetime.now().strftime('%md-%dd-%YY %HH%MM%SS')} - Exception - {e}")
            time.sleep(5)
            cw.stream.connect()
            logger.info('Reconnected to stream.')

    logger.info(f"Exception Count: {exception_count}")
    logger.info('Exiting.')
