#!/bin/bash

# Usage: ./dev_setup.sh
# Set up and activate a Python 3.10 virtual environment
python3.10 -m venv venv
source venv/bin/activate
# Upgrade pip to latest version
pip install --upgrade pip
# Install airflow with constraints. py-orca pins this version
pip install apache-airflow==2.10.5 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt"
# Install the local/ script dependencies
pip install -r local/requirements.txt
# Install dev dependencies
pip install -r requirements-dev.txt
