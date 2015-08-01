#!/bin/bash

NUM_OF_REQUESTS=1000
NUM_OF_CONCURRENT_REQUESTS=100

if [ -n "$1" ]; then
    NUM_OF_REQUESTS="$1"
fi

if [ -n "$2" ]; then
    NUM_OF_CONCURRENT_REQUESTS="$2"
fi

ab -n $NUM_OF_REQUESTS -c $NUM_OF_CONCURRENT_REQUESTS -p post_file -H "channel: test_channel" http://127.0.0.1:34569/api/post

