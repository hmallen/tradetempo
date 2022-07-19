import configparser
from re import sub
from bson import Int64
import cryptowatch as cw
import datetime
from bson import Decimal128
import json
import logging
import time
import sys

from google.protobuf.json_format import MessageToJson
from pymongo import MongoClient

from tradetempo.tickertape import TickerTape

logger = logging.getLogger()

config = configparser.RawConfigParser()
config.read('settings.cfg')

# cw.stream.subscriptions = ["markets:*:trades", "markets:*:ohlc"]
# cw.stream.subscriptions = ["assets:60:book:snapshots"]
# cw.stream.subscriptions = ["assets:60:book:spread"]
# cw.stream.subscriptions = ["assets:60:book:deltas"]
# cw.stream.subscriptions = ["markets:*:ohlc"]
# cw.stream.subscriptions = ["markets:*:trades"]
# cw.stream.subscriptions = ["markets:579:trades"]


class CryptowatchStream:

    def __init__(self, top_markets):
        db = MongoClient(config['mongodb']['host'])[config['mongodb']['db']]
        self.trades = db[config['mongodb']['collection']]

        cw.stream.on_orderbook_delta_update = CryptowatchStream.handle_orderbook_delta_updates
        cw.stream.on_orderbook_spread_update = CryptowatchStream.handle_orderbook_spread_updates
        cw.stream.on_orderbook_snapshot_update = CryptowatchStream.handle_orderbook_snapshot_updates
        cw.stream.on_intervals_update = CryptowatchStream.handle_intervals_update
        cw.stream.on_trades_update = CryptowatchStream.handle_trades_update

        sub_reference = {}
        subscription_list = []
        max_len = {
            'exchange': 0,
            'amount': 0,
            'base': 0,
            'price': 0,
            'quote': 0,
            'side': 4
        }

        btc_market_count = 0
        eth_market_count = 0
        other_market_count = 0

        for market in top_markets:
            if market['baseObj']['symbol'] == 'btc':
                btc_market_count += 1
            elif market['baseObj']['symbol'] == 'eth':
                eth_market_count += 1
            else:
                other_market_count += 1

            logger.debug(f"market['symbol']: {market['symbol']}")

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
                max_len['base'] = len(
                    sub_reference[str(market['id'])]['base'])
            if len(str(market['summary']['price']['high'])) > max_len['price']:
                max_len['price'] = len(
                    str(market['summary']['price']['high']))
            if len(sub_reference[str(market['id'])]['quote']) > max_len['quote']:
                max_len['quote'] = len(
                    sub_reference[str(market['id'])]['quote'])
        logger.debug(f"max_len: {max_len}")

        logger.info(f"BTC Markets:   {btc_market_count}")
        logger.info(f"ETH Markets:   {eth_market_count}")
        logger.info(f"Other Markets: {other_market_count}")

        self.subscription_info = {
            'subscriptions': subscription_list,
            'reference': sub_reference
        }

        self.ticker_tape = TickerTape(max_lengths=max_len)

    # What to do with each trade update
    def handle_trades_update(self, trade_update):
        trade_json = json.loads(MessageToJson(trade_update))

        received_timestamp = time.time_ns()
        id = trade_json['marketUpdate']['market']['marketId']
        exchange = self.subscription_info['reference'][id]['exchange']
        market = self.subscription_info['reference'][id]['market']
        base_currency = self.subscription_info['reference'][id]['base']
        quote_currency = self.subscription_info['reference'][id]['quote']

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
                "orderSide": trade['orderSide'].rstrip('SIDE'),
                "baseCurrency": base_currency,
                "quoteCurrency": quote_currency
            }

            insert_result = self.trades.insert_one(trade_formatted)

            logging.debug(
                f"insert_result.inserted_id: {insert_result.inserted_id}")

            self.ticker_tape.tick(trade_formatted)

    # What to do with each candle update

    def handle_intervals_update(self, interval_update):
        print(interval_update)

    # What to do with each orderbook spread update

    def handle_orderbook_snapshot_updates(self, orderbook_snapshot_update):
        print(orderbook_snapshot_update)

    # What to do with each orderbook spread update

    def handle_orderbook_spread_updates(self, orderbook_spread_update):
        print(orderbook_spread_update)

    # What to do with each orderbook delta update

    def handle_orderbook_delta_updates(self, orderbook_delta_update):
        print(orderbook_delta_update)

    def start_stream(self):
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


if __name__ == '__main__':
    pass
