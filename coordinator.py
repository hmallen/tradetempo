import configparser
import logging
import sys

from multiprocessing import Process

import tradetempo.wscwatch
import tradetempo.wsdydx

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


if __name__ == "__main__":
    config = configparser.RawConfigParser()
    config.read("settings.cfg")

    monitored_assets = {
        "cryptowatch": config["cryptowatch"]["monitored_assets"],
        "dydx": config["dydx"]["monitored_assets"],
    }

    ws_cwatch = Process(
        target=tradetempo.wscwatch.main, args=(monitored_assets["cryptowatch"],)
    )
    ws_dydx = Process(target=tradetempo.wsdydx.main, args=(monitored_assets["dydx"],))

    logger.info("Starting Cryptowatch websocket process.")
    ws_cwatch.start()
    logger.info("Starting DYDX websocket process.")
    ws_dydx.start()

    logger.info("Joining Cryptowatch websocket process.")
    ws_cwatch.join()
    logger.info("Joining DYDX websocket process.")
    ws_dydx.join()
