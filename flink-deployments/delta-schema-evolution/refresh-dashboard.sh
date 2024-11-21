#!/bin/bash

# Get the command-line argument
COMMAND=$1

# Function to copy files from the last provisioned pod
copy_from_pod() {
  # Get the last provisioned pod name
  POD_NAME=$(kubectl get pods --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')

  # Check if POD_NAME is not empty
  if [[ -z "$POD_NAME" ]]; then
    echo "No pods found!"
    exit 1
  fi

  # Define the source path in the pod and the destination path on the local filesystem
  SOURCE_PATH="/mount"  # Update with the actual path in the pod
  DESTINATION_PATH="./delta-tables"  # Update with your local path

  # Copy the content from the pod to the local filesystem
  kubectl cp "$POD_NAME:$SOURCE_PATH" "$DESTINATION_PATH"

  # Check if the copy command was successful
  if [[ $? -eq 0 ]]; then
    echo "Successfully copied content from pod $POD_NAME."
  else
    echo "Failed to copy content from pod $POD_NAME."
    exit 1
  fi
}

# Logic based on the command passed
case "$COMMAND" in
  all)
    copy_from_pod  # Call the function to copy files
    # Fall through to run the Docker command
    ;;
  refresh)
    # Do not run the Docker command
    echo "Refresh option selected. Skipping Docker run."
    copy_from_pod
    exit 0
    ;;
  jupyter)
    # Run only the Docker command
    echo "Running Jupyter Server in dev mode..."
    docker run -d -v ./jupyter-notebook/work-dir:/opt/spark/work-dir -v ./delta-tables:/opt/spark/delta-tables --name delta_quickstart --rm -it -p 8888-8889:8888-8889 deltaio/delta-docker:latest
    exit 0
    ;;
  voila)
    # Run only the Docker command
    echo "Running Voila Server..."
    docker run -d -v ./delta-tables:/opt/spark/delta-tables --name low-inventory-alert-app -p 8866:8866 low-inventory-alert
    exit 0
    ;;
  *)
    echo "Invalid command. Please use 'all', 'refresh', or 'jupyter'."
    exit 1
    ;;
esac

# Run the Docker command if 'all' was selected
echo "Running Voila server..."
#docker run -d -v ./jupyter-notebook/work-dir:/opt/spark/work-dir -v ./delta-tables:/opt/spark/delta-tables --name delta_quickstart --rm -it -p 8888-8889:8888-8889 deltaio/delta-docker:latest
docker run -d -v ./delta-tables:/opt/spark/delta-tables --name low-inventory-alert-app -p 8866:8866 low-inventory-alert



