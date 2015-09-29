#!/bin/bash

NUM_OF_REQUESTS=1000
NUM_OF_CONCURRENT_REQUESTS=100
SERVER="127.0.0.1"
PORT=54569
CHANNEL=liveshow
TOURID=8F9A7C09F3

if [ -n "$1" ]; then
    NUM_OF_REQUESTS="$1"
fi

if [ -n "$2" ]; then
    NUM_OF_CONCURRENT_REQUESTS="$2"
fi

ab -k -n $NUM_OF_REQUESTS -c $NUM_OF_CONCURRENT_REQUESTS -H "channel: $CHANNEL" -H "tourid: $TOURID" -H "tag: test" http://$SERVER:$PORT/api/poll
