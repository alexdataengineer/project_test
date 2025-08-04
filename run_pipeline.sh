#!/bin/bash

# Data Pipeline Execution Script

echo "Setting up environment..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Set Spark environment variables
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Run the pipeline
echo "Executing data pipeline..."
python data_pipeline.py

echo "Pipeline execution completed." 