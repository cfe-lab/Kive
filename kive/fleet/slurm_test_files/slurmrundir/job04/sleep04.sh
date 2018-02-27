#!/bin/bash

trap "exit 1" SIGINT SIGTERM

echo 'this goes to stdout' > /dev/stdout
echo 'this goes to stderr' > /dev/stderr

COUNTER=0
while [ $COUNTER -lt 5 ]; do
    sleep 2
    let COUNTER=COUNTER+1
done

echo 'finished sleeping...'

pwd


