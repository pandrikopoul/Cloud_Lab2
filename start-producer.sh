#!/bin/bash

USAGE="
Usage: start-producer.sh <topic>

    topic: The topic where messages are to be produced.
"

if ! (( $# > 0 )); then
    echo "$USAGE"
    exit 1
fi

topic="$1"


docker build docker build -t experiment-producer -f Dockerfile_producer.txt .




for i in {1..3}; do
    docker run \
        --rm \
        -v $(pwd)/data:/usr/src/data \
        experiment-producer \
        --topic "$topic" &
done
