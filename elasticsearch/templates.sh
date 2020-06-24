#!/bin/bash

PREFIX="templates/"
SUFFIX=".template.json"

for TEMPLATE in $PREFIX*$SUFFIX; do
    FILENAME=$(basename $TEMPLATE)
    NAME=${FILENAME/$SUFFIX/}
    curl -XPUT $ES_SCHEME://$ES_HOST:$ES_PORT/_template/$ES_INDEX_PREFIX-$NAME -H 'Content-Type: application/json' -d "@$TEMPLATE"
    echo " $FILENAME"
done
