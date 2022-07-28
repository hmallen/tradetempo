import configparser
import logging
import sys

from multiprocessing import Process

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

config = configparser.RawConfigParser()
config.read("settings.cfg")
sub_count = int(config["cryptowatch"]["subscription_count"])
monitored_assets = config["cryptowatch"]["monitored_assets"]
if "," in monitored_assets:
    monitored_assets = monitored_assets.split(",")
    monitored_assets = [asset.rstrip(" ") for asset in monitored_assets]
logger.debug(f"monitored_assets: {monitored_assets}")


if __name__ == "__main__":
    pass
