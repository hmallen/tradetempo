import logging
import os
import sys

logger = logging.getLogger()


class TickerTape:

    def __init__(self, max_lengths):
        self.max_lengths = max_lengths

    def tick(self, trade_formatted):
        print(f"{trade_formatted['orderSide']:{self.max_lengths['side']}} | {trade_formatted['baseCurrency'].upper():{self.max_lengths['base']}} | {str(trade_formatted['amount']):{self.max_lengths['amount']}} @ {str(trade_formatted['price']):{self.max_lengths['price']}} {trade_formatted['quoteCurrency'].upper():{self.max_lengths['quote']}} | {trade_formatted['market']:{self.max_lengths['exchange']}} | {trade_formatted['market']}")
