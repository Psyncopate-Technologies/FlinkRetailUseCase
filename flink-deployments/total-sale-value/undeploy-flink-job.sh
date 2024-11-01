#!/bin/bash

kubectl delete -f total-sale-value-products-hourly.yaml

docker stop total-sale-value-app
docker stop delta_quickstart # Only when dev mode