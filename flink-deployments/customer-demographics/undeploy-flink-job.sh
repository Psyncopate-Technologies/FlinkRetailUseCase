#!/bin/bash

kubectl delete -f customer_order_demographics.yaml

docker stop customer-order-demographics-app
docker stop delta_quickstart # Only when dev mode