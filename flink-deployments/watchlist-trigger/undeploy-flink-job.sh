#!/bin/bash

kubectl delete -f watchlist-trigger.yaml

docker stop watchlist-trigger-app
docker stop delta_quickstart # Only when dev mode
