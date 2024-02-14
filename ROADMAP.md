# Data surface ROAD MAP

## Phase 1: GitHub model working

This is the basic model, the github action handlers, model validations, model authorization checks, backwards compatibility. This is currently 90% there as of mid Feb 2024.

## Phase: Batch Microsoft Azure Data Platform

This will allow an ecosystem to be rendered on top of Microsoft Azure. It will allow data stored in Azure data containers to be fed to Azure hosted data containers for consumers to use. It will automatically provision tables, entitlements and feed the data consumers need. It will be batch based. Expect latencies measured in the single digit minutes. This will support for Azure SQL Server instances, Azure managed SQL server instances and Lakehouse. It will support CDC as the primary ingestion mechanism.

This DataPlatform will generate a bicep definition of the total pipeline for an Ecosystem which can then be used to maintain the data pipelines for the ecosystem on Azure. Periodically, the operation team can generate another bicep definition of the Ecosystem and then push this against the current infrastructure. Once the current infrastructure has been updated to match the new bicep definition then another revision can be pushed.

## Phase: Amazon AWS support

This will be a batch data platform that can run on AWS. It will support AWS Aurora, Athena, Lakehouse, and AWS Redshift.

## Phase: Federated Data platform support

This allows an ecosystem to be distributed across data producers and consumers using different platforms and have it work seamlessly. The Ecosystem manages the data pipelines across multiple data platforms as well as enforces governance restrictions on what data can be stored where.

## Phase: Streaming Microsoft Azure Data Platform

This will allow an ecosystem to be rendered top of Microsoft Azure. Streaming and batch consumers will be supported at this time. Consumers can have data delivered with low latency when required and in batch mode when not. Apache Beam will likely feature in this for implementing a streaming pipeline including transformations.

## Phase: Azure SnowFlake support as a data container

This will support Snowflake using either external tables using lakehouse or native tables. Based on experience, external tables will be supported first.

## Phase: Milestoned data support

This will produce fully milestoned versions of data from data producers. This will maintain a linked list of records for every key in every dataset. The linked list will provide the latest record if one exists as well as every version of the record for that key that existed before.


