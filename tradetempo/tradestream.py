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

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("cryptowatch").setLevel(logging.INFO)

config = configparser.RawConfigParser()

config.read('settings.cfg')

db = MongoClient(config['mongodb']['host'])[config['mongodb']['db']]
trades = db[config['mongodb']['collection']]

sub_count = int(config['cryptowatch']['subscription_count'])

config = configparser.RawConfigParser()
config.read('.credentials.cfg')

# Set your API Key
cw.api_key = config['cryptowatch']['api_key']

max_len = {
    'exchange': 0,
    'amount': 0,
    'base': 0,
    'price': 0,
    'quote': 0
}


class MarketInfo:
    def __init__(self):
        pass

    def get_top_markets(self, assets, count=25):
        if type(assets) is not list:
            assets = list(assets)

        url = f"https://billboard.service.cryptowat.ch/markets?page=1&limit={count}&volumeInAssets=usd&sort=volume&sortAsset=usd&onlyBaseAssets={','.join(assets)}"

        result = requests.get(url)

        if result.status_code == 200:
            asset_info = result.json()['result']['rows']
        else:
            asset_info = f"Status Code: {result.status_code}"

        return asset_info


# What to do with each trade update
def handle_trades_update(trade_update):
    trade_json = json.loads(MessageToJson(trade_update))

    received_timestamp = time.time_ns()
    id = trade_json['marketUpdate']['market']['marketId']
    exchange = sub_reference[id]['exchange']
    market = sub_reference[id]['market']
    base_currency = sub_reference[id]['base']
    quote_currency = sub_reference[id]['quote']

    for trade in trade_json['marketUpdate']['tradesUpdate']['trades']:
        trade_formatted = {
            "received_timestamp": received_timestamp,
            "id": id,
            "exchange": exchange,
            "market": market,
            "timestamp": datetime.datetime.fromtimestamp(int(trade['timestamp'])),
            "price": Decimal128(trade['priceStr']),
            "amount": Decimal128(trade['amountStr']),
            "timestampNano": Int64(trade['timestampNano']),
            # "externalId": trade['externalId'],
            "orderSide": trade['orderSide'].rstrip('SIDE')
        }

        insert_result = trades.insert_one(trade_formatted)

        logging.debug(
            f"insert_result.inserted_id: {insert_result.inserted_id}")
        # print(gi
        #     f"{trade_formatted['externalId']} - {trade_formatted['orderSide'].rstrip('SIDE')} - {trade_formatted['amount']} @ {trade_formatted['price']}")
        # print(
        #     f"{{{:<{max_len['exchange']}}.format(trade_formatted['exchange'])}} - {trade_formatted['orderSide']} {trade_formatted['amount']} {base_currency.upper()} @ {trade_formatted['price']} {quote_currency.upper()}"
        # )
        # pprint(trade_formatted)
        print(
            f"{trade_formatted['exchange']} - {trade_formatted['orderSide']} {trade_formatted['amount']} {base_currency.upper()} @ {trade_formatted['price']} {quote_currency.upper()}"
        )


# What to do with each candle update
def handle_intervals_update(interval_update):
    print(interval_update)


# What to do with each orderbook spread update
def handle_orderbook_snapshot_updates(orderbook_snapshot_update):
    print(orderbook_snapshot_update)


# What to do with each orderbook spread update
def handle_orderbook_spread_updates(orderbook_spread_update):
    print(orderbook_spread_update)


# What to do with each orderbook delta update
def handle_orderbook_delta_updates(orderbook_delta_update):
    print(orderbook_delta_update)

# cw.stream.subscriptions = ["markets:*:trades", "markets:*:ohlc"]
# cw.stream.subscriptions = ["assets:60:book:snapshots"]
# cw.stream.subscriptions = ["assets:60:book:spread"]
# cw.stream.subscriptions = ["assets:60:book:deltas"]
# cw.stream.subscriptions = ["markets:*:ohlc"]
# cw.stream.subscriptions = ["markets:*:trades"]
# cw.stream.subscriptions = ["markets:579:trades"]


cw.stream.on_orderbook_delta_update = handle_orderbook_delta_updates
cw.stream.on_orderbook_spread_update = handle_orderbook_spread_updates
cw.stream.on_orderbook_snapshot_update = handle_orderbook_snapshot_updates
cw.stream.on_intervals_update = handle_intervals_update
cw.stream.on_trades_update = handle_trades_update

sub_reference = {}


if __name__ == '__main__':
    logger = logging.getLogger('__main__')
    # logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)

    logger.info('Getting top markets.')
    market_info = MarketInfo()
    top_markets = market_info.get_top_markets(['btc', 'eth'], count=sub_count)
    # [pprint(mkt) for mkt in top_markets]
    # sys.exit()

    logger.info('Building subscription list.')
    subscription_list = []
    btc_market_count = 0
    eth_market_count = 0
    other_market_count = 0

    print('\n--- MARKETS ---\n')
    for market in top_markets:
        if market['baseObj']['symbol'] == 'btc':
            btc_market_count += 1
        elif market['baseObj']['symbol'] == 'eth':
            eth_market_count += 1
        else:
            other_market_count += 1
        print(market['symbol'])
        subscription_list.append(f"markets:{market['id']}:trades")
        sub_reference[str(market['id'])] = {
            'exchange': market['exchangeObj']['name'],
            'market': market['instrumentObj']['v3_slug'],
            'base': market['instrumentObj']['base'],
            'quote': market['instrumentObj']['quote']
        }
        if len(sub_reference[str(market['id'])]['exchange']) > max_len['exchange']:
            max_len['exchange'] = len(
                sub_reference[str(market['id'])]['exchange'])
        if len(str(max(market['liquidity']['ask'], market['liquidity']['bid']))) > max_len['amount']:
            max_len['amount'] = len(
                str(max(market['liquidity']['ask'], market['liquidity']['bid'])))
        if len(sub_reference[str(market['id'])]['base']) > max_len['base']:
            max_len['base'] = len(sub_reference[str(market['id'])]['base'])
        if len(str(market['summary']['price']['high'])) > max_len['price']:
            max_len['price'] = len(str(market['summary']['price']['high']))
        if len(sub_reference[str(market['id'])]['quote']) > max_len['quote']:
            max_len['quote'] = len(sub_reference[str(market['id'])]['quote'])
    pprint(max_len)
    # sys.exit()

    print(f"BTC Markets:   {btc_market_count}")
    print(f"ETH Markets:   {eth_market_count}")
    print(f"Other Markets: {other_market_count}")

    cw.stream.subscriptions = subscription_list
    # time.sleep(5)

    logger.info('Connecting to stream.')
    # Start receiving
    cw.stream.connect()

    while True:
        try:
            time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info('Exit signal received.')
            # Stop receiving
            cw.stream.disconnect()
            break

        except Exception as e:
            logger.exception(e)
            time.sleep(5)
            logger.info('Reconnecting to stream.')
            cw.stream.connect()
