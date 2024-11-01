#!/bin/bash

kubectl apply -f top-viewed-product-by-users-hourly.yaml

# Define the service name and namespace (if applicable)
SERVICE_NAME="top-viewed-product-personalized-rest"
NAMESPACE="default"  # Change this if your service is in a different namespace

# Function to check if the service exists
check_service_exists() {
  kubectl get svc "$SERVICE_NAME" -n "$NAMESPACE" >/dev/null 2>&1
}

# Wait for the service to be available
echo "Waiting for the service $SERVICE_NAME to be available..."
while ! check_service_exists; do
  echo "Service $SERVICE_NAME not found. Waiting for 15 seconds..."
  sleep 15
done

# Port-forward the service
echo "Service $SERVICE_NAME found. Starting port-forwarding..."
kubectl port-forward svc/"$SERVICE_NAME" 8081