# The initial DataPlatform

This is intended to be the default DataPlatform available with DataSurface. It's a locally hosted DataPlatform running on services such as GitLab, Postgres, Airflow and Kafka Connect.

## Postgres

Postgres is used for storing the staging tables and the MERGE tables.

## Airflow

Airflow is used to schedule the python jobs.

## Kafka Connect

Kafka Connect is used to copy data from kafka to postgres staging tables. Kafka connect also allows CDC using products like Debezium. It will use
Mongey's terraform plugin to create the kafka connectors needed for the graph supplied to the platform.

## Python Jobs

Python jobs are used to read from the staging tables and write to the MERGE tables. We can use PythonOperators in Airflow to run these jobs.

## Gitlab

Gitlab is used to store the DSL and run the validation actions from pull requests.

## Terraform

Terraform with the Mongey terraform plugin will be used to create the kafka connectors needed. This is described at <https://github.com/Mongey/terraform-provider-kafka-connect>

## Docker

Docker maybe be used to containerize the DataPlatform. We may pivot from PythonOperators to DockerOperators in Airflow if that makes sense.

## Datastore ingestion meta data

Datastore owners may be using their own kafka broker/topics to publish events. We need to connect those topics to the connectors in our specified
Kafka connect cluster which may be different.

## DataPlatform meta data

The SimpleDataPlatform needs:

1. The name of the DataPlatform
2. The documentation for the DataPlatform
3. The credential store for the DataPlatform
4. Postgres database. This is the merge and staging store for the DataPlatform
5. The Kafka connect cluster connection details.
