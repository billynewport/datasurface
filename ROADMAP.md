# Data surface ROAD MAP

The plan is to have the model working and a suite of DataPlatforms written for it to use. Initially, it was planned to have cloud platforms supported initially but the cost of developing these and testing them is high. So, we pivoted to a private DataPlatform as the initial delivery. This will use Kafka Connect with Minio for storage and typical local databases such as Oracle/DB2/Postgres and so on. It will use Debezium for CDC with Kafka connect.

Once thats working then we will revisit cloud. Confluent should be an easy one given it's basically managed Kafka connect on AWS/GCP or Azure. Ideally, we'd like these to be a portable as possible, built around Kubernetes/Kafka Connect and so on. This will allow the DataPlatform to be run on any cloud or on prem.

## Phase 1: GitHub model **(COMPLETE)**

This is the basic model, the github action handlers, model validations, model authorization checks, backwards compatibility. This is currently functional as of end Feb 2024. Documentation is a major deliverable in this phase also.

## Phase 2: REST API to model **(Complete)**

We're building a REST API to the datasurface models. This is being packaged as a scalable docker container which uses a checked out github version of the model and makes it available using a REST API. This scales very easily. We just make a stateless cluster that runs on Kubernetes or similar. We just run as many instances as we need for the load. The APIs are written in python using FastAPI and pydantic for the JSON objects returned to the callers. This is necessary for non python consumers of the model such as Java Spark Jobs which need the schemas for data being ingested and so on. See [here](docs/REST_API.md) for more information.

The way this would be used is as a sidekick container in a Kubernetes job for example. The sidekick or init container runs with a copy of the model checked out of the repository. Then it runs on a localhost and the job can query the model easily. If the job is written in python then it can be consumed directly.

## Phase 2: Private Compute Batch/Streaming Support using Kafka Connect/Confluent (IN PROGRESS)

This is a simple DataPlatform that uses a Postgres instance to store staging data and merge tables. It will support Kafka connect to import data from a variety of sources and support live and milestoned data. This is the ZeroDataPlatform and is mostly used to flesh out DataSurface. This should be easy to port to Athena/Azure and Snowflake. Providing a cloud native version which runs on columnar storage databases also. This will use Airflow as a job scheduler.

You can read about it [here](docs/zero/README.md).

## Phase 2: Amazon AWS support

This will be a batch data platform that can run on AWS. It will support AWS Glue, AWS Aurora, Athena, Lakehouse, and AWS Redshift. This is in progress and will be the first DataPlatform delivered. You can read about it [here](docs/aws/design.md). The intention is to render a terraform IaC definition of the total pipeline in to a github branch which Terraform is watching. As the branch changes, Terraform will apply the changes to the infrastructure.

Thus, we have a github action looking for changes on the main branch and then generating the IaC for its, committing that to a different branch in Github which terraform is watching. This will allow the infrastructure to be updated in a controlled manner.

## Phase 2:  Batch Microsoft Azure Data Platform (paused)

This will allow an ecosystem to be rendered on top of Microsoft Azure. It will allow data stored in Azure data containers to be fed to Azure hosted data containers for consumers to use. It will automatically provision tables, entitlements and feed the data consumers need. It will be batch based. Expect latencies measured in the single digit minutes. This will support for Azure SQL Server instances, Azure managed SQL server instances and Lakehouse. It will support CDC as the primary ingestion mechanism.

This DataPlatform will generate a bicep definition of the total pipeline for an Ecosystem which can then be used to maintain the data pipelines for the ecosystem on Azure. Periodically, the operation team can generate another bicep definition of the Ecosystem and then push this against the current infrastructure. Once the current infrastructure has been updated to match the new bicep definition then another revision can be pushed.

## Phase 3: Federated Data platform support

This allows an ecosystem to be distributed across data producers and consumers using different platforms and have it work seamlessly. The Ecosystem manages the data pipelines across multiple data platforms as well as enforces governance restrictions on what data can be stored where.
