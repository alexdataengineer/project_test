#!/bin/bash

# Test Execution Script

echo "Setting up test environment..."

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

# Run tests
echo "Running test suite..."
python -m pytest test_pipeline.py -v

echo "Test execution completed." 