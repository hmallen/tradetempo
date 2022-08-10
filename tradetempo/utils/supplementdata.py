import configparser
import logging
import os
import sys
import traceback

import requests
import simplejson as json
from pymongo import MongoClient

os.chdir(sys.path[0])

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
    def __init__(self, db, collection, process_new=False):
        self.db = MongoClient(
            host=config["mongodb"]["host"],
            port=int(config["mongodb"]["port"]),
            directConnection=True,
        )[db]

        logger.info("Updating exchange and market reference collections.")
        exchanges = requests.get("https://api.cryptowat.ch/exchanges").json()["result"]
        insert_result = self.db[config["mongodb"]["exchange_collection"]].insert_many(
            exchanges.json()["result"]
        )
        logger.debug(
            f"Added {len(insert_result.upserted_ids) if insert_result.upserted_ids is not None else 0} exchanges."
        )
        markets = requests.get("https://api.cryptowat.ch/markets").json()["result"]
        insert_result = self.db[config["mongodb"]["market_collection"]].insert_many(
            markets.json()["result"]
        )
        logger.debug(
            f"Added {len(insert_result.upserted_ids) if insert_result.upserted_ids is not None else 0} markets."
        )

        logger.info("")
    
    def add_reference(self, exchange_id=None, market_id=None):
        add_succeeded = False

        try:
            if exchange_id is None and market_id is None:
                logger.warning("Must supply either exchange_id, market_id, or both.")

            if exchange_id is not None:
                if type(exchange_id) != list:
                    exchange_id = [str(id) for id in exchange_id]
                
                for exchange in self.exchange_reference:
                    if exchange["id"] in exchange_id:
                        self.saved_reference[exchange_id["id"]]

            if market_id is not None:
                if type(market_id) != list:
                    market_id = [market_id]
        
        except Exception as e:
            logger.exception(f"Exception in SupplementData.add_reference: {e}")
            logger.exception(traceback.format_exc())
    
    def analyze_collection(self):
        pass
    
    def supplement_document(self, doc_id):
        pass


if __name__ == "__main__":
    pass
