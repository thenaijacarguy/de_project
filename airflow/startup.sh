#!/bin/bash
# This script runs when the Airflow containers start.
# It installs the Python packages our pipeline scripts need.
pip install -r /opt/airflow/project/airflow/requirements.txt --quiet