import configparser
import logging
import os
import sys
import traceback

from multiprocessing import Process

import tradetempo.wscwatch
import tradetempo.wsdydx
import tradetempo.wstiingo

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
        "tiingo": [val.strip(" ") for val in config["tiingo"]["tickers"].split(",")],
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
    ws_tiingo = Process(
        target=tradetempo.wstiingo.start_stream, args=(monitored_assets["tiingo"],)
    )

    logger.info("Starting Cryptowatch websocket process.")
    ws_cwatch.start()
    logger.info("Starting DYDX websocket process.")
    ws_dydx.start()
    logger.info("Starting Tiingo websocket process.")
    ws_tiingo.start()

    try:
        logger.debug("[try] - start")

        logger.info("Joining Cryptowatch websocket process.")
        ws_cwatch.join()
        logger.info("Joining DYDX websocket process.")
        ws_dydx.join()
        logger.info("Joining Tiingo websocket process.")
        ws_tiingo.join()

        logger.info("Processes stopped.")

        logger.debug("[try] - end")

    except KeyboardInterrupt:
        logger.info("Exit signal received.")

    except Exception as e:
        logger.exception(traceback.format_exc())

    finally:
        logger.debug("[finally] - start")

        logger.info("Joining Cryptowatch websocket process.")
        ws_cwatch.join()
        logger.info("Joining DYDX websocket process.")
        ws_dydx.join()
        logger.info("Joining Tiingo websocket process.")
        ws_tiingo.join()

        logger.info("Processes stopped. Exiting.")

        logger.debug("[finally] - end")
