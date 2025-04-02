# The initial DataPlatform

This is intended to be the default DataPlatform available with DataSurface. It's a locally hosted DataPlatform running on services such as GitLab, Postgres, Airflow and Kafka Connect.

## Postgres

Postgres is used for storing the staging tables and the MERGE tables.

## Airflow

Airflow is used to schedule the python jobs.

## Kafka Connect

Kafka Connect is used to copy data from kafka to postgres staging tables. Kafka connect also allows CDC using products like Debezium.

## Python Jobs

Python jobs are used to read from the staging tables and write to the MERGE tables.

## Gitlab

Gitlab is used to store the DSL and run the validation actions from pull requests.

## Terraform

Terraform is used to deploy the DataPlatform to the local machine.

## Docker

Docker is used to containerize the DataPlatform.

