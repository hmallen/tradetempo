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

cwatch_coll = config["mongodb"]["cryptowatch_collection"]


class SupplementData:
    def __init__(
        self,
        db,
        collection
    ):
        self.db = MongoClient(
            host=config["mongodb"]["host"],
            port=int(config["mongodb"]["port"]),
            directConnection=True,
            retryWrites=False,
        )[db]

        self.collection = collection

        logger.info("Updating exchange reference.")
        exchanges = requests.get("https://api.cryptowat.ch/exchanges").json()[
            "result"
        ]

        bulk_ops = []
        for exchange in exchanges:
            bulk_ops.append(
                UpdateOne(
                    {"type": "exchange", "id": exchange["id"]},
                    {"$set": exchange},
                    upsert=True,
                )
            )
        logger.debug(
            f"Beginning bulk exchange update of {len(bulk_ops)} operations."
        )
        bulk_result = self.db[cwatch_coll].bulk_write(bulk_ops)
        logger.info(
            f"New Exchanges: {bulk_result.inserted_count} / Updated Exchanges: {bulk_result.modified_count}"
        )

        logger.info("Updating market reference.")
        markets = requests.get("https://api.cryptowat.ch/markets").json()["result"]

        bulk_ops = []
        for market in markets:
            bulk_ops.append(
                UpdateOne(
                    {"type": "market", "id": market["id"]},
                    {"$set": market},
                    upsert=True,
                )
            )
        logger.debug(f"Beginning bulk market update of {len(bulk_ops)} operations.")
        bulk_result = self.db[cwatch_coll].bulk_write(bulk_ops)
        logger.info(
            f"New Markets: {bulk_result.inserted_count} / Updated Markets: {bulk_result.modified_count}"
        )
        
        logger.info("Updating pair reference.")
        pairs = requests.get("https://api.cryptowat.ch/pairs").json()["result"]

        bulk_ops = []
        for pair in pairs:
            bulk_ops.append(
                UpdateOne(
                    {"type": "pair", "id": pair["id"]}, {"$set": pair}, upsert=True
                )
            )
        logger.debug(f"Beginning bulk pair update of {len(bulk_ops)} operations.")
        bulk_result = self.db[cwatch_coll].bulk_write(bulk_ops)
        logger.info(
            f"New Pairs: {bulk_result.inserted_count} / Updated Pairs: {bulk_result.modified_count}"
        )

        self.reference = {"exchange": {}, "market": {}, "pair": {}}

        logger.info("Building exchangeId reference.")
        db_exchangeId = self.db[self.collection].distinct("exchangeId")
        for idx, exch in enumerate(db_exchangeId):
            if exch is None:
                db_exchangeId.pop(idx)
        logger.debug(f"db_exchangeId: {db_exchangeId}")
        for id in db_exchangeId:
            self.add_reference(type="exchange", id=id)

        logger.info("Building marketId reference.")
        db_marketId = self.db[self.collection].distinct("marketId")
        for mkt in enumerate(db_marketId):
            if mkt is None:
                db_marketId.pop(idx)
        logger.debug(f"db_marketId: {db_marketId}")
        for id in db_marketId:
            self.add_reference(type="market", id=id)

        logger.info("Building currencyPairId reference.")
        db_currencyPairId = self.db[self.collection].distinct("currencyPairId")
        for idx, pair in enumerate(db_currencyPairId):
            if pair is None:
                db_currencyPairId.pop(idx)
        logger.debug(f"db_currencyPairId: {db_currencyPairId}")
        for id in db_currencyPairId:
            self.add_reference(type="pair", id=id)
    

    def add_reference(self, type, id):
        ref_doc = self.db[cwatch_coll].find_one({"type": type, "id": int(id)})

        if type == "exchange" or type == "pair":
            self.reference[type][id] = ref_doc["symbol"]

        elif type == "market":
            if ref_doc is not None:
                market_info = {}
                market_info["exchange"] = ref_doc["exchange"]
                market_info["market"] = ref_doc["pair"]
                pair_val = ref_doc["pair"]
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
                        f"Unable to determine quote currency of market pair {ref_doc['pair']}."
                    )
                    quote = ""
                market_info["quote"] = quote
                market_info["base"] = pair_val.rstrip(quote)

                self.reference["market"][id] = market_info

                if "market" not in ref_doc:
                    ref_doc.update(market_info)
                    update_result = self.db[cwatch_coll].update_one(
                        {"_id": ref_doc["_id"]}, {"$set": ref_doc}
                    )
                    if update_result.modified_count > 0:
                        logger.debug(
                            f"Updated reference for market {ref_doc['id']}."
                        )
            
            else:
                logger.warning(f"Market ID {id} not found. Skipping.")


if __name__ == "__main__":
    supp = SupplementData(
        db=config["mongodb"]["db"],
        collection=config["mongodb"]["collection"],
    )

    from pprint import pprint

    pprint(supp.reference)