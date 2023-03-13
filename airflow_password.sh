#!/bin/bash
aws --output text secretsmanager get-secret-value --secret-id airflow_password --query SecretString --region us-east-1
