# Private DataPlatform Docker configuration

This file will describe how to setup a local environment which can run a working DataSurface solution using the private DataPlatform. This consists
of various docker containers running the following components.

* AirFlow for job scheduling
* Postgres for database sources
* Debenzium for CDC
* Spark 4.0 and iceberg for data processing and storage.
* Minio for local S3 storage
* Kafa Connect for streaming data
* Gitlab for DataSurface repository and Airflow repository for DAG

Each of these environments will be setup in its own docker container and linked together using docker-compose. The following sections will describe how to setup each of these components.

## "Global Docker environment"

A virtual network will be configured which is attached to all these containers. This allows the containers to communicate with each other using
host names. This is done by creating a network in docker and attaching each container to this network.
