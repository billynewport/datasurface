#!/bin/bash

# Set the path to the Python script relative to the /app directory
SCRIPT_PATH="data_surface/api/datasurface.py"

# Check out the specified branch from the GitHub repository into /app/repo
# The eco.py file should be in /app/repo after this
git clone -b "$BRANCH" "$REPO_URL" repo

# Run the Python script and pass any command-line arguments
# Specify port at 50010 for the GRPC server
python -m "$SCRIPT_PATH" /app/repo 50010 "$@"
