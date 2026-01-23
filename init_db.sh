#!/usr/bin/env bash

# 1. Wait for Postgres to be ready
echo "Waiting for Postgres to be fully ready..."
until pg_isready -h postgres -p 5432 -U airflow; do
  echo -n '.'
  sleep 1
done

# 2. Wait for Airflow Application to load (a simple sleep)
echo "Postgres is ready. Sleeping to allow Airflow application to load..."
sleep 15

# 3. Run Initialization and User Creation
echo "Running Airflow DB Initialization and User Creation..."
airflow db init
airflow users create --username admin --firstname John --lastname Doe --role Admin --email admin@example.com -p admin

echo "Airflow Initialization Complete."