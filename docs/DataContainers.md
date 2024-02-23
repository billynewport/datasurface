# DataContainers

DataSurface imagines data being persisted across the enterprise using DataContainers. DataContainers are persistent stores for data which allow client access through an API. Examples of common data containers would be:

* SQL Database
* Amazon S3 object storage
* Snowflake Columnar database
* FTP Server

These are all objects which can persist data. There are subclasses of DataContainer for these specific types of container. They have many features:

* Addressable (clients can connect somehow)
* Possible service side encryption
* Possible client side encryption
* Owned by an infrastructure vendor
* Physically located in one or more InfrastructureLocations owned by that vendor

DataSurface allows GovernanceZones to define policies that constrain the data containers which can store data or a certain class of data. For example, only store high private data on datacontainers which support client side encryption (prevents a cloud vendor from seeing the data)

## Data Producers have a DataContainer

Data producers specify in their CaptureMetaData a Datacontainer which holds the data thats available to the ecosystem. The producer provides metadata indicating how data can be ingested from the DataContainer persisting their data.

## Consumer Workspaces

Consumers want their data available using a DataContainer which is suited to their usecase. Consumers also specify how the data platforms can connect to the DataContainer being used for a Workspace. It's also possible for consumers to use a managed DataContainer. Here, the Dataplatform is responsible to assign the Workspace to one of many DataContainers available. The DataPlatform then assigns multiple Workspaces to a single Datacontainer and moves Workspaces around over a set of DataContainers to maintain high DataContainer utilization, lower costs and maintain consumer query performance.

Again, GovernanceZones may constrain the DataContainers that can be used to persist data under their governance.

## Policies that can filter DataContainers available for persisting data

* StoragePolicy
* InfrastructureVendorPolicy
* InfrastructureLocationPolicy
