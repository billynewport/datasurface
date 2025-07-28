# Design of YellowDataPlatform

## Overview

YellowDataPlatform is an implementtion of the DataPlatform class in Datasurface. It uses a postgres database for all storage. It is completely driven by the metadata stored in the datasurface model. The datasurface model will provide an intention graph to the YellowDataPlatform. This intention graph describes the consumers whose Datasetgroups are assigned to the YellowDataPlatform. Each Datasetgroup has a list of DatasetSinks which describe the datastore and dataset names they want to query. The Datasetgroup is owned by a Workspace. The Workspace is assigned to a DataContainer indicating which database the consumer wishes to use to query the data. If we following the intention graph backwards then we can see exportnodes linking the datasetsinks to the datacontainer for the Workspace. We can also see ingestion nodes linking the export nodes to the source database containing the datasets required by the consumer.

The DataPlatform should then validate that it supports the original source dataContainer and capture mechanism that that container supports. It should also support the export datacontainer the Workspace is connected to. It should validate that it supports the schemas used for every dataset in the pipeline. For the datasets that are supported, it will need to determine a consistent mapping of identifiers (table names, column names, datatypes) for that schema on the specific datacontainers used to store staging data, the merge tables and the views for the Workspaces.

The YellowDataPlatform focuses on a single DataContainer, a postgres database for capturing the data and the Workspaces must use the DataContainer used to capture the data. A special DataContainer, a logical DataContainer will be used for the Workspace. This will just say the DataContainer is provided by the DataPlatform and not explicitly set by the consumer.

The YellowDataPlatform will support CDC specifications on the source DataContainer for kafka topics. It will read those messages and write them a batch at a time to the corresponding staging table for each dataset. It will also use a side table for metadata to track where it is in this process. It will use Kafka confluence to write messages from the kafka topics to the postgres staging table for each dataset.

A job will be scheduled to run periodically and merge any outstanding batches from the staging tables to the merge tables. This jobs will also make sure the schemas of those tables and all the consumers views are up to date with the schemas in the model on DataSurface before running the merge code. A security job will run to create the tables and views and assign ACL's to them for allowing consumers to query the datasets in their Workspace.

The Kafka CDC class in DataSurface will need to specify a mapping. The YellowDataPlatform will support a set of dataset to topic mappings. This will be reflected in the KafkaIngestion class in DataSurface. The data owner will need to specify this mapping between their datasets and the kafka topics that the events are published on. The YellowDataPlatform will use the KafkaIngestion class to configure the Confluence sink for writing kafka topics to postgres tables.

## Schema evolution

DataSurface doesn't allow non backwards compatible schema changes. All schema changes in a DataSurface model are always backwards compatible. If backwards changes are really required then make a different Datastore and ingest to that after shutting down the previous one. There are consumers who were using those schemas and such breaking changes will break them also. A producer should keep publishing the old schema and, in a perfect world, the new non compatible schema so existing and new consumers can switch over to it in a timely manner, remember, different consumers may have resources to do such a conversion on different time scales. Breaking an existing schema has costs and Datasurface has a protocol for handling deprecating a dataset/datastore which must be followed.

When the job runs, it will check if the schemas have changed since the last run. This state should be kept in a metadata table in postgres. If the schema has changed then it may be necessary to stop the kafka connector, change schemas and then restart the connector. Schemas may need to be changed in the kafka schema repository and in the postgres staging/merging and consumer view objects. Once schemas are changed then we update the metadata table and then restart the connector and run the merge job.

So, the scheduled job in Airflow will run independently of the kafka connector. The kafka connector is responsible for copying events from kafka to postgres staging tables. The airflow job runs code to check schema changes and then to execute the MERGE from unprocessed staging records to the MERGE tables.

## Security

Security isn't really a DataPlatform problem, the DataPlatform problem is simply to ingest, transform and export data based on what consumers want. Different companies will have different security requirements and these have nothing to do with the DataPlatform. A seperate Datasurface subsystem will have multiple implementations which customers can choose from or they can implement their own. These implementations are responsible for handling ACLs on the views ONLY. The DataPlatform will simply handle owning the staging/merge and views. The security subsystem will run as a role which has permissions to update the security settings on the Workspace views.

## Merge

The merge job will run and periodically merging the staging records that are unprocessed in to the merge table. The staging records might have a IUD attribute indicating insert/update/delete. The merge job will be able to keep the merge records as a live view of active records with this attribute. If there is no IUD attribute then it have update records but records will never be deleted.

If a merge fails due to data issues then the job will need to fail and flag an error to the dev ops team why the job failed. The dev ops team may then need to work with the data owners to fix the data issues upstream. The stream will be stalled until this is fixed
