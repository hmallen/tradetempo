#!/bin/bash

nohup python tradetempo/tradestream.py > logs/tradestream.log 2>&1 &
echo "Trade stream started. Logging output to logs/tradestream.log."