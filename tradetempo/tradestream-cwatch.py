import configparser
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

from cwatchhelper import MarketInfo

from pprint import pprint

# logger = logging.getLogger("cryptowatch")
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# logging.getLogger("cryptowatch").setLevel(logging.DEBUG)

# log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(log_format)
# stream_handler.setLevel(logging.DEBUG)
# logger.addHandler(stream_handler)

config = configparser.RawConfigParser()
config.read(".credentials.cfg")
cw.api_key = config["cryptowatch"]["api_key"]

config.read("settings.cfg")

db = MongoClient(
    host=config["mongodb"]["host"],
    port=int(config["mongodb"]["port"]),
    directConnection=True,
)[config["mongodb"]["db"]]
trades = db[config["mongodb"]["collection"]]

# cw.stream.subscriptions = ["markets:*:trades", "markets:*:ohlc"]
# cw.stream.subscriptions = ["assets:60:book:snapshots"]
# cw.stream.subscriptions = ["assets:60:book:spread"]
# cw.stream.subscriptions = ["assets:60:book:deltas"]pymongo.errors.ServerSelectionTimeoutError: Could not reach any servers in
# cw.stream.subscriptions = ["markets:*:ohlc"]
# cw.stream.subscriptions = ["markets:*:trades"]
# cw.stream.subscriptions = ["markets:579:trades"]

# What to do with each trade update


def handle_trades_update(trade_update):
    trade_json = json.loads(MessageToJson(trade_update))

    received_timestamp = time.time_ns()
    id = trade_json["marketUpdate"]["market"]["marketId"]
    exchange = sub_reference[id]["exchange"]
    market = sub_reference[id]["market"]
    base_currency = sub_reference[id]["base"]
    quote_currency = sub_reference[id]["quote"]

    for trade in trade_json["marketUpdate"]["tradesUpdate"]["trades"]:
        trade_formatted = {
            "received_timestamp": received_timestamp,
            "id": id,
            "exchange": exchange,
            "market": market,
            "timestamp": datetime.datetime.fromtimestamp(int(trade["timestamp"])),
            "price": Decimal128(trade["priceStr"]),
            "amount": Decimal128(trade["amountStr"]),
            "timestampNano": Int64(trade["timestampNano"]),
            "orderSide": trade["orderSide"].rstrip("SIDE"),
            "baseCurrency": base_currency,
            "quoteCurrency": quote_currency,
        }

        insert_result = trades.insert_one(trade_formatted)

        logger.debug(f"insert_result.inserted_id: {insert_result.inserted_id}")

        print_trade(trade_formatted)


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


cw.stream.on_orderbook_delta_update = handle_orderbook_delta_updates
cw.stream.on_orderbook_spread_update = handle_orderbook_spread_updates
cw.stream.on_orderbook_snapshot_update = handle_orderbook_snapshot_updates
cw.stream.on_intervals_update = handle_intervals_update
cw.stream.on_trades_update = handle_trades_update

sub_reference = {}

max_len = {"exchange": 0, "amount": 0, "base": 0, "price": 0, "quote": 0, "side": 4}


def build_subscription(top_markets):
    sub_ready = None

    subscription_list = []

    btc_market_count = 0
    eth_market_count = 0
    other_market_count = 0

    try:
        for market in top_markets:
            if market["baseObj"]["symbol"] == "btc":
                btc_market_count += 1
            elif market["baseObj"]["symbol"] == "eth":
                eth_market_count += 1
            else:
                other_market_count += 1

            logger.debug(f"market['symbol']: {market['symbol']}")

            subscription_list.append(f"markets:{market['id']}:trades")
            sub_reference[str(market["id"])] = {
                "exchange": market["exchangeObj"]["name"],
                "market": market["instrumentObj"]["v3_slug"],
                "base": market["instrumentObj"]["base"],
                "quote": market["instrumentObj"]["quote"],
            }
            if len(sub_reference[str(market["id"])]["exchange"]) > max_len["exchange"]:
                max_len["exchange"] = len(sub_reference[str(market["id"])]["exchange"])
            if (
                len(str(max(market["liquidity"]["ask"], market["liquidity"]["bid"])))
                > max_len["amount"]
            ):
                max_len["amount"] = len(
                    str(max(market["liquidity"]["ask"], market["liquidity"]["bid"]))
                )
            if len(sub_reference[str(market["id"])]["base"]) > max_len["base"]:
                max_len["base"] = len(sub_reference[str(market["id"])]["base"])
            if len(str(market["summary"]["price"]["high"])) > max_len["price"]:
                max_len["price"] = len(str(market["summary"]["price"]["high"]))
            if len(sub_reference[str(market["id"])]["quote"]) > max_len["quote"]:
                max_len["quote"] = len(sub_reference[str(market["id"])]["quote"])
            logger.debug(f"(loop)  max_len: {max_len}")
        logger.debug(f"(final) Smax_len: {max_len}")

        logger.info(f"BTC Markets:   {btc_market_count}")
        logger.info(f"ETH Markets:   {eth_market_count}")
        logger.info(f"Other Markets: {other_market_count}")

        cw.stream.subscriptions = subscription_list

        sub_ready = True

    except Exception as e:
        logger.exception(e)

        sub_ready = False

    finally:
        return sub_ready


def print_trade(trade_formatted):
    # print(f"{trade_formatted['orderSide']:{max_len['side']}} | {trade_formatted['baseCurrency'].upper():{max_len['base']}} | {str(trade_formatted['amount']):{max_len['amount']}} @ {str(trade_formatted['price']):{max_len['price']}} {trade_formatted['quoteCurrency'].upper():{max_len['quote']}} | {trade_formatted['exchange']:{max_len['exchange']}} | {trade_formatted['market']}")
    print(
        f"{trade_formatted['orderSide']:{max_len['side']}} | {trade_formatted['baseCurrency'].upper():{max_len['base']}} | {str(trade_formatted['amount']):{max_len['amount']}} @ {'{:.2f}'.format(trade_formatted['price'].to_decimal()):{max_len['price'] + 2}} {trade_formatted['quoteCurrency'].upper():{max_len['quote']}} | {trade_formatted['exchange']:{max_len['exchange']}} | {trade_formatted['market']}"
    )


def start_stream(ping_timeout=20, ping_interval=70, enable_trace=False, use_rel=False):
    cw.stream.connect(
        ping_timeout=ping_timeout,
        ping_interval=ping_interval,
        enable_trace=enable_trace,
        use_rel=use_rel,
    )

    exception_count = 0
    while True:
        try:
            time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("Exit signal received.")
            # Stop receiving
            cw.stream.disconnect()
            logger.info("Disconnected from stream.")
            break

        except Exception as e:
            exception_count += 1
            logger.exception(e)
            with open("errors.log", "a") as error_file:
                error_file.write(
                    f"{datetime.datetime.now().strftime('%md-%dd-%YY %HH%MM%SS')} - Exception - {e}"
                )
            time.sleep(5)
            cw.stream.connect(
                ping_timeout=ping_timeout,
                ping_interval=ping_interval,
                enable_trace=enable_trace,
                use_rel=use_rel,
            )
            logger.info("Reconnected to stream.")

    logger.info(f"Exception Count: {exception_count}")
    logger.info("Exiting.")


def main(assets):
    if type(assets) != list:
        assets = [assets]

    market_info = MarketInfo()
    top_markets = market_info.get_top_markets(
        assets, count=config["cryptowatch"]["subscription_count"]
    )

    if not build_subscription(top_markets):
        logger.error("Failed to build subscription.")
        sys.exit(1)
    # sys.exit()

    start_stream(enable_trace=False)


if __name__ == "__main__":
    main(["btc", "eth"])
