# Data surface ROAD MAP

The plan is to have the model working and a suite of DataPlatforms written for it to use. Initially, it was planned to have cloud platforms supported initially but the cost of developing these and testing them is high. So, we pivoted to a private DataPlatform, YellowDataPlatform, as the initial delivery. This will use Kafka Connect with Minio for storage and typical local databases such as Oracle/DB2/Postgres and so on. It will use Debezium for CDC with Kafka connect. Why Yellow? Why not? We'll be calling the first set of DataPlatforms after the primary colors.

Once thats working then we will revisit cloud. Confluent should be an easy one given it's basically managed Kafka connect on AWS/GCP or Azure. Ideally, we'd like these to be a portable as possible, built around Kubernetes/Kafka Connect and so on. This will allow the DataPlatform to be run on any cloud or on prem.

## Phase 1: GitHub model **(COMPLETE)**

This is the basic model, the github action handlers, model validations, model authorization checks, backwards compatibility. This is currently functional as of end Feb 2024. Documentation is a major deliverable in this phase also.

## Phase 2: REST API to model **(Complete)**

We're building a REST API to the datasurface models. This is being packaged as a scalable docker container which uses a checked out github version of the model and makes it available using a REST API. This scales very easily. We just make a stateless cluster that runs on Kubernetes or similar. We just run as many instances as we need for the load. The APIs are written in python using FastAPI and pydantic for the JSON objects returned to the callers. This is necessary for non python consumers of the model such as Java Spark Jobs which need the schemas for data being ingested and so on. See [here](docs/REST_API.md) for more information.

The way this would be used is as a sidekick container in a Kubernetes job for example. The sidekick or init container runs with a copy of the model checked out of the repository. Then it runs on a localhost and the job can query the model easily. If the job is written in python then it can be consumed directly.

## Phase 3: YellowDataPlatform Batch/Streaming Support using Postgres,Kafka Connect/Confluent (Started March 2025, IN PROGRESS)

This is a powerful DataPlatform that uses a Postgres instance to store staging data and merge tables. It will support direct SQL ingestion as well asKafka connect to import data from a variety of sources and support live and milestoned data. This is the YellowDataPlatform and is used to flesh out DataSurface during its development. This should be easy to port to Athena/Azure and Snowflake. Providing a cloud native version which runs on columnar storage databases also. This will use Airflow as a job scheduler. It uses Kafka Connect/Confluence to ingest data from a variety of sources. It is designed around Docker Swarm for the container orchestration.

You can read about it [here in the design docs](docs/yellow_dp/README.md).

## Phase 4: SecurityModule implementation

Security is about who can read data in a Workspace. There is no single solution to this problem that will work for all use cases. We will provide an extensible framework for SecurityModules to be written and used. We will provide a default implementation. DataPlatforms and SecurityModules are independent components. A SecurityModule can work with compatible DataPlatforms. Compatible here means that the SecurityModule and the DataPlatform need to share compatible DataContainers for hosting Workspaces. For example, a DataPlatform which uses Snowflake databases for Workspaces will be compatible with any SecurityModule which supports Snowflake. The initial security module will support PostGres in line with the initial DataPlatform also supporting Postgres. By explicitly choosing a SQL based DataContainer, this work should be easier to extend to similar SQL databases.

## Conclusion of Phase 4 and 5 is a working system

Once we have a working DataPlatform and SecurityModule, users will be able to use DataSurface to build end to end systems.

## Future DataPlatforms

Once the SimpleDataPlatform is working then we can start to look at other DataPlatforms. The candidates are:

* Databricks

  This will use their Schema catalog and Delta lake for storage. It will try to keep using SQL rather than switching to Spark.

* AWS Glue/Athena

  This will use their Schema registry and S3 for storage. It will use their Glue job scheduler. We will try to use iceberg tables and Athena for the SQL engine

* Snowflake

  This will use their Schema registry and their Object storage for storage. It will use their job scheduler.

* Azure SQL

  This will use their Schema registry and their Blob storage for storage. It will use their Data Factory for job scheduling.

* Kubernetes as well as Docker Swarm
