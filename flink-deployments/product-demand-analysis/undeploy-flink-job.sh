#!/bin/bash

kubectl delete -f product-demand-analysis-daily.yaml

docker stop product-demand-daily-app
docker stop delta_quickstart # Only when dev mode