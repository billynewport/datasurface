# Workspaces

A consumer of data of an ecosystem must define a Workspace within a Team within a GovernanceZone. Users can choose to have Teams for managing just workspaces or Teams which manage Datastores and Workspaces, its a choice.

A consumer uses a Workspace to define the data that they require as well as how they want the data to be delivered to them. A consumer describes the delivery requirements of the workspace using a WorkspacePlatformConfig. This allows the consumer to specify:

* The type of data container that they need for this use case (OLTP/OLAP/Object based)

* Where the data is needed

* how much data latency is desirable

* Whether the data needs to be milestoned or not

* How long data should be retained for and so on.

* Regulatory retention requirements and how that data can be stored.

The ecosystem will then use this information to choose a DataPlatform to host the data pipeline for the Workspace.

Consumers may also specify they want to use multiple of these for a single application. A Workspace has multiple DatasetGroups. A DatasetGroup has one or more DatasetSinks. EAch DatasetSink describes a dataset which the consumer needs for their application. Each DatasetGroup can specify its own WorkspacePlatformConfig. A Dataset can be specified multiple times within a single Workspace, once per DatasetGroup. This is allowed so consumer may require a live, low latency version of a dataset as well as a much high latency version of the same dataset which is fully milestoned.

## Asset

The asset is the data container that will host the data for the consumer. The ecosystem will deliver the data from the producers to this container where the consumer will be able to query it. There are a variety of asset types and they will consistently change. Examples of asset types are:

* Lake House (object store + Delta capable columnar files + Data catalog)

* Traditional OLTP SQL Databases

* Traditional OLAP SQL Data warehouses

Assets are typically located with an InstructureLocation which is owned by an InfrastructureVendor. Datasets can only be stored on an Asset if the GovernanceZone that polices the dataset allows it. Some GovernanceZones may not allow cloud vendors or may not allow on site vendors.

## DatasetGroups

A DatasetGroup allow a consumer to specify a group of datasets and the consumer can specify how they want the ecosystem to present the data. The consumer can specify the data latency requirements in terms of SECONDS or MINUTES of even HOURS may be acceptable.

The consumer can also specify if they require just the current live records of each dataset or if they require milestoned or temporal versions of the datasets which include both the live and previous versions of every record in the dataset.

The Ecosystem interprets these requirements and selects an appropriate data platform from the set that are available. These can be constrained by the GovernanceZones that own data in the Dataset.

### DatasetSinks

A datasetgroup has one or more datasetsink references. This basically states the consumer would like to use a specific dataset in a Workspace using the data pipeline selected by its DatasetGroup. A dataset can be used in multiple datasetgroups and different data pipelines may be used to serve it to the Asset for a single Workspace. A datasetsink also specifies the depreciation policy the consumer is ready to allow for this dataset. This allows a consumer to say it does not want to use deprecated data at all or it will allow the use of it temporarily while alternative sources for that data are determined. This works together with the data producers ability to mark datasets as deprecated.
