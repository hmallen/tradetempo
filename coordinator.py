import configparser
import logging
import os
import sys
import traceback

from multiprocessing import Process

import tradetempo.wscwatch
import tradetempo.wsdydx

os.chdir(sys.path[0])

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("logs/coordinator.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

config_path = "settings.cfg"
config = configparser.RawConfigParser()
config.read(config_path)


if __name__ == "__main__":
    # try:
    monitored_assets = {
        "cryptowatch": [
            val.strip(" ")
            for val in config["cryptowatch"]["monitored_assets"].split(",")
        ],
        "dydx": config["dydx"]["monitored_assets"],
    }
    logger.debug(f"monitored_assets: {monitored_assets}")

    ws_cwatch = Process(
        target=tradetempo.wscwatch.start_stream,
        args=(
            monitored_assets["cryptowatch"],
            config["cryptowatch"]["subscription_count"],
        ),
    )
    ws_dydx = Process(
        target=tradetempo.wsdydx.start_stream, args=(monitored_assets["dydx"],)
    )

    logger.info("Starting Cryptowatch websocket process.")
    ws_cwatch.start()
    logger.info("Starting DYDX websocket process.")
    ws_dydx.start()

    logger.info("Joining Cryptowatch websocket process.")
    ws_cwatch.join()
    logger.info("Joining DYDX websocket process.")
    ws_dydx.join()

    logger.info("Processes stopped.")

    # except Exception as e:
    #     logger.exception(traceback.format_exc())

    # finally:
    #     logger.info("Exiting.")
