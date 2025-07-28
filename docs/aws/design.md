# Amazon AWS Batch Data Pipeline design

This document describes the philisophy behind the design of the first AWS Batch DataPlatform. It isn't intended to be the best DataSurface data platform. It's intended to show how a particular design philosophy can be implemented with a DataSurface Data platform.

## Design Philosophy

This will be a conventional batch data pipeline. It will ingest data from sources in to a staging area. The staging area will then be processing in to a set of tables which allow the data to be replayed to other data containers used by consumers. Data Transformations will be supported by any data container supported by the data platform.

The pipeline will be fully generated from the Ecosystem model. The subset of that model which was selected to run on this platform will be rendered as a Terraform IaC project and kept up to date in a Github Repository. Terraform will be configured to watch that repo and apply all changes to an AWS account.

This means that when the ecosystem model is modified then the IaC will be updated and the AWS resources will be updated. This is a very powerful way to manage the data platform. It allows the data platform to be managed by the data producers and consumers.

This will result in:

* Ingestion jobs to use DMS to get data from sources are store LOAD and CDC data in S3 staging buckets. A Trigger will then load a Spark Job to process these staging files in to an Iceberg Table for each dataset ingested.
* Export job for Snowflake External tables. These will either create the external table OR refresh it to point at the latest metadata files
* Export job for Redshift External tables. These will either create the table or refresh it
* Data Transformer jobs for either SnowFlake or Redshift or Spark. These will run the transformer code against the Workspace asset and then ingest the resulting output datasets.

## Ingestion and staging

The data platform uses AWS DMS to ingest data from sources in to an S3 based staging area. AWS DMS is a self managed OR serverless service that can be used to move data from a source to a target. The source can be a database, a data warehouse or a data lake. The target can be a database, a data warehouse or a data lake. The data platform uses S3 as the target for the DMS service. The data platform uses the DMS service to move data from the source to the staging area.

AWS DMS brings the following benefits to the data platform:

* It's a managed service. This means the data platform doesn't have to manage the infrastructure for the service.
* It supports many source connectors such as Oracle, SQL Server, MySQL, PostgreSQL, MongoDB, Amazon Aurora, Amazon Redshift, Amazon S3, and Amazon DynamoDB. These sources can also be on-premises or in the cloud.

AWS DMS creates objects in S3 staging initially for the full load. Load staging files have approximately the same schema as the source table. Once full load is finished then DMS starts sending CDC staging objects. These differ from load staging files because they have an additional column "Op" as the first column. This indicates whether the record is an insert or a new record or an update or delete of existing records. The ingestion process has 2 phases. The full load and then CDC ingestion. The job determines which phase that it is in by looking at the LOAD and CDC files. The CDC phase is only entered when all the LOAD files present in staging have been processed. We don't expect CDC files to appear until all LOAD files have
already been placed in staging. Thus, as long as there are unprocessed LOAD files then we do not consider CDC files.

## How the DataPlatform interacts with the DataSurface model

The code described below should use the schema defined by the Data Producer in the Ecosystem model on the live branch of the repository.

This platform will fork the DataSurface repository at this point and then generate the IaC translation for that fork. This will be used to create the AWS resources for the data platform.

The ecosystem model will continue to be modified. At some point, this DataPlatform will fork the live branch again and then generate the new IaC translation for the new fork. The new IaC translation will be used to update the AWS resources for the data platform.

## Processing

```mermaid
graph TD
    SRC[Source Data Container] -->|AWS DMS| S3(Staging Area)
    S3 -->|AWS Glue| T1(Iceberg Table 1)
    S3 -->|AWS Glue| T2(Iceberg Table 2)
    S3 -->|AWS Glue| T3(Iceberg Table 3)

    T1 -->|AWS Glue Export| ADB(Aurora USA)
    T2 -->|AWS Glue Export| ADB
    T1 -->|AWS Glue Export| RS(Redshift)

    T1 -->|AWS Glue Export| ADB2(Aurora Europe)
    T2 -->|AWS Glue Export| ADB2
    T3 -->|AWS Glue Export| ADB2
```

This could be optimized differently if its determined that there will be many others of the data outside the primary AWS region. It may make sense to use a second DataPlatform instance, distinct from the first, so that data is buffered once between the first region and the other regions. This would save on network egres costs during multiple hydrations and ongoing change pushing.

We can imagine such graphs holding thousands of sources, hundreds of thousands of tables and thousands of data consumers hosted on tens or hundreds of data containers suited for their use cases. All running on a common data platform that can be managed centrally at a much lower cost than the traditional bespoke pipeline approach.

## Managing Iceberg compaction for tables used for snapshots and deltas

AWS Glue does not handle compaction of these tables. This is necessary to avoid poor read performance. The timing of when we do compaction is important. When we need to hydrate a consumer data container like an AWS Aurora database or any kind of database with an independent storage then we first need to send a snapshot and then all the deltas from the snapshot onwards. If we compact the iceberg tables before we have sent all the deltas then we will have to send the entire snapshot again. This is not efficient. We need to compact the iceberg tables after we have sent all the deltas from the snapshot onwards. We need to be able to do this in a way that doesn't affect the performance of the data platform. Some hydrations may take days to complete. Therefore, we need a able to delay compaction while we are hydrating a consumer data container. We can accomplish this with a soft locking mechanism. If present then this is disable compaction. When compaction starts then it also locks the table meaning hydrations will be blocked until compaction finishes. This is similar to a multi-reader/single writer lock. Hydrations will obtain a read lock and compaction will obtain a write lock.

If consumers are using a Workspace directly mapped to the iceberg tables then this is difficult. There is no way to know whether there are long running queries using data files which may be deleted when compaction is executed. Thus, we will configure compaction to run at 24 hour or longer intervals which allows query run times of up to 24 hours or more. This is a limitation of the iceberg table format. It's not a limitation of the data platform.

## Federating with other DataPlatforms

Data producers have their data in a production database. This means the database does not have unlimited capacity and especially when doing initial full snapshots, the load on a production database can become unacceptable. This is the reason for using iceberg tables as a buffer between consumer data containers and the producer data containers. It decouples them and consumer hydration or rehydration events can be serviced from the intermediate iceberg tables and leave the producer data container in peace.

Data producers will pick a Dataplatform as its primary ingestion service. This means that if a consumer wants data from a producer and the consumer is using the same Dataplatform as the producer then thats easy, they can connect. But, if the consumer is using a different DataPlatform then if we are going to limit the ingestion load on the producer to a single DataPlatform then the two DataPlatforms need to cooperate. The consumer data platform will pull data from the iceberg buffer tables in the AWS case. This also means that consumer data latencies will not be better than the latencies possible from the data producer dataplatform. This means here:

```mermaid

graph TD
    A[Producer Data Platform] -->|Ingest| B(Iceberg Buffer)
    B -->|Export| C[Consumer Data Platform]
```

Thus, choosing which DataPlatform is the primary can have an impact on the overall latench in a multiple DataPlatform system.

It's also possible to use an AWS DataPlatform for the data producers and consumers. Later, some new data consumers can be added which want to use a data container provided by a different cloud vendor. There are many ways to handle this. One approach is to make a buffer in the second cloud vendor, The data is moved from AWS to the buffer (which could be iceberg based also), Once, it's in the buffer then its distributed from there to the data containers on the second cloud vendor. This minimize network egress costs because data is just transferred to the second vendor once and then fanned out from there to the data containers on the second cloud vendor.

This applies whether there is two cloud vendors or five cloud vendors. There will be a federating DataPlatform which manages this over multiple data platforms.

## Parameters for DataPlatform

The DataPlatform will have the following parameters:

* The DataPlatform instance name
* The DataPlatform AWS Region
* The DataPlatform AWS Account
* The DataPlatform AWS IAM Role
* The Staging AWS Bucket (Name, IAM Role and prefix)
* The IceBerg AWS Bucket (Name, IAM Role and prefix)
* The AWS Glue Database Name and IAM Role.

The DataPlatform will use these parameters to create the AWS resources for the data platform. More than one DataPlatform can be created, for example, one for development, one for production. Governance Zone policies can restrict non production data stores to using non production DataPlatforms.
