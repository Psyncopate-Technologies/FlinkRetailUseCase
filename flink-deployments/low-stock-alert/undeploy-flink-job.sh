#!/bin/bash

kubectl delete -f low_stock_alert.yaml

docker stop low-inventory-alert-app
docker stop delta_quickstart # Only when dev mode