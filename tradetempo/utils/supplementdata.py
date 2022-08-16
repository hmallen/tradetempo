import configparser
import logging
import os
import sys
import traceback

import requests
import simplejson as json
from pymongo import MongoClient, UpdateOne

from pathlib import Path

os.chdir(f"{Path(__file__).resolve().parent}/../..")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("logs/utils-supplementdata.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

config = configparser.RawConfigParser()
config.read("settings.cfg")


class SupplementData:
    def __init__(
        self, db, collection, skip_exchange_update=False, skip_market_update=False, skip_pair_update=False
    ):
        self.db = MongoClient(
            host=config["mongodb"]["host"],
            port=int(config["mongodb"]["port"]),
            directConnection=True,
        )[db]

        self.collection = collection

        if not skip_exchange_update:
            logger.info("Updating exchange reference.")
            exchanges = requests.get("https://api.cryptowat.ch/exchanges").json()[
                "result"
            
            bulk_ops = []
            for exchange in exchanges:
                """update_result = self.db[
                    config["mongodb"]["cryptowatch_collection"]
                ].update_one(
                    {"type": "exchange", "id": exchange["id"]},
                    {"$set": exchange},
                    upsert=True,
                )"""
                
                bulk_ops.append(
                    UpdateOne(
                        {"type": "exchange", "id": exchange["id"]},
                        {"$set": exchange},
                        upsert=True,
                    )
                )
            logger.debug(f"Beginning bulk exchange update of {len(bulk_ops)} operations.")
            bulk_result = self.db[config["mongodb"]["cryptowatch_collection"]].bulk_write(bulk_ops)
            logger.info(f"New Exchanges: {bulk_result.inserted_count} / Updated Exchanges: {bulk_result.modified_count}")
            # logger.debug(
            #     f"Added {len(update_result.upserted_ids) if update_result.upserted_ids is not None else 0} exchanges."
            # )
        else:
            logger.warning("Skipping exchange update.")

        if not skip_market_update:
            logger.info("Updating market reference.")
            markets = requests.get("https://api.cryptowat.ch/markets").json()["result"]
            """update_count = 0
            for market in markets:
                update_result = self.db[
                    config["mongodb"]["cryptowatch_collection"]
                ].update_one(
                    {"type": "market", "id": market["id"]},
                    {"$set": market},
                    upsert=True,
                )
                update_count += 1
                logger.debug(f"[market] update_count: {update_count}")"""
            
            bulk_ops = []
            for market in markets:
                bulk_ops.append(
                    UpdateOne(
                        {"type": "market", "id": market["id"]},
                        {"$set": market},
                        upsert=True
                    )
            logger.debug(f"Beginning bulk market update of {len(bulk_ops)} operations.")
            bulk_result = self.db[config["mongodb"]["cryptowatch_collection"]].bulk_write(bulk_ops)
            logger.info(f"New Markets: {bulk_result.inserted_count} / Updated Markets: {bulk_result.modified_count}")

            # logger.debug(
            #     f"Added {len(update_result.upserted_ids) if update_result.upserted_ids is not None else 0} markets."
            # )
        else:
            logger.warning("Skipping market update.")
        
        if not skip_pair_update:
            logger. info("Updating pair reference.")
            pairs = requests.get("https://api.cryptowat.ch/pairs").json()["result"]

        self.reference = {"exchange": {}, "market": {}}

        logger.info("Building reference for exchanges in database.")
        db_exchangeId = self.db[self.collection].distinct("exchangeId")
        for idx, exch in enumerate(db_exchangeId):
            if exch is None:
                db_exchangeId.pop(idx)
        logger.debug(f"db_exchangeId: {db_exchangeId}")
        for id in db_exchangeId:
            exchange_doc = self.db[
                config["mongodb"]["cryptowatch_collection"]
            ].find_one({"type": "exchange", "id": int(id)})

            self.reference["exchange"][id] = exchange_doc["symbol"]

        from pprint import pprint        
        
        logger.info("Building reference for markets in database.")
        db_marketId = self.db[self.collection].distinct("marketId")
        for mkt in enumerate(db_marketId):
            if mkt is None:
                db_marketId.pop(idx)
        logger.debug(f"db_marketId: {db_marketId}")
        for id in db_marketId:
            market_doc = self.db[config["mongodb"]["cryptowatch_collection"]].find_one(
                {"type": "market", "id": int(id)}
            )

            if market_doc is not None:
                market_info = {}
                market_info["exchange"] = market_doc["exchange"]
                market_info["market"] = market_doc["pair"]
                pair_val = market_doc["pair"]
                if "futures" in market_info["market"]:
                    market_info["futures"] = True
                else:
                    market_info["futures"] = False
                if "-" in pair_val:
                    pair_val = pair_val.split("-")[0]
                if pair_val.endswith("usd"):
                    quote = "usd"
                elif pair_val.endswith("eur"):
                    quote = "eur"
                elif pair_val.endswith("btc"):
                    quote = "btc"
                elif pair_val.endswith("eth"):
                    quote = "eth"
                elif pair_val.endswith("usdt"):
                    quote = "usdt"
                elif pair_val.endswith("usdc"):
                    quote = "usdc"
                else:
                    logger.error(
                        f"Unable to determine quote currency of market pair {market_doc['pair']}."
                    )
                    quote = ""
                market_info["quote"] = quote
                market_info["base"] = pair_val.rstrip(quote)

                self.reference["market"][id] = market_info

                if "market" not in market_doc:
                    market_doc.update(market_info)
                    update_result = self.db[config['mongodb']['cryptowatch_collection']].update_one(
                        {"_id": market_doc["_id"]},
                        {"$set": market_doc}
                    )
                    if update_result.modified_count > 0:
                        logger.debug(f"Updated reference for market {market_doc['id']}.")

            else:
                logger.warning(f"Market ID {id} not found. Skipping.")


if __name__ == "__main__":
    supp = SupplementData(
        db=config["mongodb"]["db"],
        collection=config["mongodb"]["collection"],
        skip_market_update=True,
    )
