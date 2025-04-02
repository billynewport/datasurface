# Data surface ROAD MAP

The plan is to have the model working and a suite of DataPlatforms written for it to use. Initially, it was planned to have cloud platforms supported initially but the cost of developing these and testing them is high. So, we pivoted to a private DataPlatform as the initial delivery. This will use Kafka Connect with Minio for storage and typical local databases such as Oracle/DB2/Postgres and so on. It will use Debezium for CDC with Kafka connect.

Once thats working then we will revisit cloud. Confluent should be an easy one given it's basically managed Kafka connect on AWS/GCP or Azure. Ideally, we'd like these to be a portable as possible, built around Kubernetes/Kafka Connect and so on. This will allow the DataPlatform to be run on any cloud or on prem.

## Phase 1: GitHub model **(COMPLETE)**

This is the basic model, the github action handlers, model validations, model authorization checks, backwards compatibility. This is currently functional as of end Feb 2024. Documentation is a major deliverable in this phase also.

## Phase 2: REST API to model **(Complete)**

We're building a REST API to the datasurface models. This is being packaged as a scalable docker container which uses a checked out github version of the model and makes it available using a REST API. This scales very easily. We just make a stateless cluster that runs on Kubernetes or similar. We just run as many instances as we need for the load. The APIs are written in python using FastAPI and pydantic for the JSON objects returned to the callers. This is necessary for non python consumers of the model such as Java Spark Jobs which need the schemas for data being ingested and so on. See [here](docs/REST_API.md) for more information.

The way this would be used is as a sidekick container in a Kubernetes job for example. The sidekick or init container runs with a copy of the model checked out of the repository. Then it runs on a localhost and the job can query the model easily. If the job is written in python then it can be consumed directly.

## Phase 2: Private Compute Batch/Streaming Support using Kafka Connect/Confluent (Started March 2025, IN PROGRESS)

This is a powerful DataPlatform that uses a Postgres instance to store staging data and merge tables. It will support Kafka connect to import data from a variety of sources and support live and milestoned data. This is the ZeroDataPlatform and is used to flesh out DataSurface during its development. This should be easy to port to Athena/Azure and Snowflake. Providing a cloud native version which runs on columnar storage databases also. This will use Airflow as a job scheduler. It uses Kafka Connect/Confluence to ingest data from a variety of sources.

You can read about it [here](docs/zero/README.md).

## Phase 3: Amazon AWS support

This will be a port of the ZeroDataPlatform to AWS leveraging AWS Glue for job scheduling and Redshift for storage. It will leverage AWS DMS for CDC.

## Phase 3:  Batch Microsoft Azure Data Platform

This will be a port of the ZeroDataPlatform to Azure leveraging Azure Data Factory for job scheduling and Azure SQL for storage. It will leverage Azure Event Hubs for CDC.

## Phase 4: Federated Data platform support

This allows an ecosystem to be distributed across data producers and consumers using different platforms and have it work seamlessly. The Ecosystem manages the data pipelines across multiple data platforms as well as enforces governance restrictions on what data can be stored where.
