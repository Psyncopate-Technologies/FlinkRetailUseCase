#!/bin/bash

kubectl delete -f top-viewed-product-by-users-hourly.yaml

docker stop top-viewed-product-personalized-app
docker stop delta_quickstart # Only when dev mode