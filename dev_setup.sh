#!/bin/bash

# Usage: ./dev_setup.sh
# Set up and activate a Python 3.10 virtual environment
python3.10 -m venv venv
source venv/bin/activate
# Upgrade pip to latest version
pip install --upgrade pip
# Install airflow with constraints
pip install apache-airflow==2.7.2 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.10.txt"
# Install other dependencies
pip install -r requirements-airflow.txt
# Install dev dependencies
pip install -r requirements-dev.txt
