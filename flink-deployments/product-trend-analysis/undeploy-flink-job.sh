#!/bin/bash

kubectl delete -f product-trend-analysis-hourly.yaml

docker stop product-views-trend-app
docker stop delta_quickstart # Only when dev mode