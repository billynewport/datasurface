# Data surface ROAD MAP

## Phase 1: GitHub model **(COMPLETE)**

This is the basic model, the github action handlers, model validations, model authorization checks, backwards compatibility. This is currently functional as of end Feb 2024. Documentation is a major deliverable in this phase also.

## Phase 2: REST API to model (IN PROGRESS)

We're building a REST API to the datasurface models. This is being packaged as a scalable docker container which uses a checked out github version of the model and makes it available using a REST API. This scales very easily. We just make a stateless cluster that runs on Kubernetes or similar. We just run as many instances as we need for the load. The APIs are written in python using FastAPI and pydantic for the JSON objects returned to the callers. This is necessary for non python consumers of the model such as Java Spark Jobs which need the schemas for data being ingested and so on. See [here](docs/REST_API.md) for more information.

## Phase 2: Amazon AWS support (IN PROGRESS)

This will be a batch data platform that can run on AWS. It will support AWS Glue, AWS Aurora, Athena, Lakehouse, and AWS Redshift. This is in progress and will be the first DataPlatform delivered. You can read about it [here](docs/aws/design.md). The intention is to render a terraform IaC definition of the total pipeline in to a github branch which Terraform is watching. As the branch changes, Terraform will apply the changes to the infrastructure.

Thus, we have a github action looking for changes on the main branch and then generating the IaC for its, committing that to a different branch in Github which terraform is watching. This will allow the infrastructure to be updated in a controlled manner.

## Phase 2: Streaming/Data pipeline

The intention is to use Apache Beam or similar technology to run a unified streaming/batch pipeline when possible. The project will try to use Python/Mojo as much as possible also.

## Phase 2:  Batch Microsoft Azure Data Platform

This will allow an ecosystem to be rendered on top of Microsoft Azure. It will allow data stored in Azure data containers to be fed to Azure hosted data containers for consumers to use. It will automatically provision tables, entitlements and feed the data consumers need. It will be batch based. Expect latencies measured in the single digit minutes. This will support for Azure SQL Server instances, Azure managed SQL server instances and Lakehouse. It will support CDC as the primary ingestion mechanism.

This DataPlatform will generate a bicep definition of the total pipeline for an Ecosystem which can then be used to maintain the data pipelines for the ecosystem on Azure. Periodically, the operation team can generate another bicep definition of the Ecosystem and then push this against the current infrastructure. Once the current infrastructure has been updated to match the new bicep definition then another revision can be pushed.

## Phase 2: Private Compute Batch Support

This will be a batch data platform that can run on private compute.

## Phase 3: Federated Data platform support

This allows an ecosystem to be distributed across data producers and consumers using different platforms and have it work seamlessly. The Ecosystem manages the data pipelines across multiple data platforms as well as enforces governance restrictions on what data can be stored where.

## Phase: Streaming Microsoft Azure Data Platform

This will allow an ecosystem to be rendered top of Microsoft Azure. Streaming and batch consumers will be supported at this time. Consumers can have data delivered with low latency when required and in batch mode when not. Apache Beam will likely feature in this for implementing a streaming pipeline including transformations.

## Phase: Azure SnowFlake support as a data container

This will support Snowflake using either external tables using lakehouse or native tables. Based on experience, external tables will be supported first.

## Phase: Milestoned data support

This will produce fully milestoned versions of data from data producers. This will maintain a linked list of records for every key in every dataset. The linked list will provide the latest record if one exists as well as every version of the record for that key that existed before.
