# Design of YellowDataPlatform

## Overview of DataSurface

Datasurface is a model driven data platform which insulates data producers, transformers and consumers from the underlying technology stacks used to process data pipelines. DataSurface is to data pipelines what AWS S3 is to storage media. Data pipeline technology advances quickly and datasurface allows customers to easily move their data applications from stack to stack with ease as needed.

Datasurface uses a central github repository which has the official version of the datasurface model. The datasurface model is built as a python DSL stored in a repository with a primary file named eco.py in the main branch. This is locked down and controlled by a central team. This team is responsible for maintaining data platforms such as Yellow, provisioning new GovernanceZones and top level policies. They are also responsible to assigned a set of DataPlatforms to each consumer. Consumers specify what they want in terms of requirements and the central team assigns a suitable DataPlatform to fulfill that needed. Multiple DataPlatforms can be assigned when the consumer wants to try a new DataPlatform and compare with their existing one easily.

GovernanceZones have their own repository and the people who can change that repository can change governance policies for that zone as well as create new Teams.

Teams can also have their own repository and they can change team level policies as importantly create new Data producers, transformers and consumers.

A data producer is a collection of named schemas called Datasets grouped in to a Datastore. A single database with some tables might be a Datastore with some datasets. The Datastore is annotated with information indicating how to get the data from the underlying container holding the information, like a database. There is no coding, how to get the data just needs to be expressed using the DSL.

A consumer is a Workspace object in the DSL. This is where the consumer selects from the datasets available from any data producer in the model the subset they need for their application. The consumer also specifies how they want the data. For example, live records only or milestoned historical records. How much data latency they are willing to accept. The type of data container they need for the Workspace, OLAP or OLTP, a snowflake database or an iceberg lakehouse. The central team looks at this and decides which available DataPlatform(s) suit that consumer best.

A transformer is a consumer which takes their data and produces derivative data from it. They might mask sensitive data. They might combine data from multiple sources. They may aggregate data or turn it in to PDF blobs in an output table. The output Datastore of a transformer is available for any consumer to use directly the same as data which comes from a data producer. Thus, a transformer is both a consumer and a data producer. DataTransformer for Yellow are written in python and execute on kubernetes against the Yellow postgres database to read and write their data.

All changes from the github repository are managed through CI/CD pipeline of github. A repo owner clones the main central model. Then they make changes to the model which they are allowed to and then they create a pull request for the central team. If the central team approves the changes then the changes are in the live model.

Automatic model consistency and validation checkers are integrated as git action handlers. These handler ensure the incoming model is valid, consistent, backwards compatible and no policies are violated by the changes from the whole model point of view. You cannot make changes to the model which violate these checks.

All model changes are stored and versioned in the github repository. GitHub authentication and authorization controls who can make changes to a repository. The central team controls the central/core model. The various governance zone teams each have their own repository where those changes can be made. Each team has their own repository where team level changes can be made. This allows a SOCs compliant system because of industry standard versioning, approval flows, authorization and authentication checks. Datasurface's action handler act as gate keepers to ensure the model stays valid, self consistent and compliant with the policies.

Lets make this a bit more concrete:

* Ecosystem model uses owner/yellow_prod branch main
* GZ US uses owner/yellow_prod branch gz_us
* Team FL uses owner/yellow_prod branch team_fl

Changes to repos should always be make using pull requests. For example, to change the Ecosystem model, i.e. add a GZ or DataPlatform, the 

### Schema evolution

DataSurface doesn't allow non backwards compatible schema changes. All schema changes in a DataSurface model are always backwards compatible. If backwards changes are really required then make a different Datastore and ingest to that after shutting down the previous one. There are consumers who were using those schemas and such breaking changes will break them also. A producer should keep publishing the old schema and, in a perfect world, the new non compatible schema so existing and new consumers can switch over to it in a timely manner, remember, different consumers may have resources to do such a conversion on different time scales. Breaking an existing schema has costs and Datasurface has a protocol for handling deprecating a dataset/datastore which must be followed.

### Making the model real.

Once the central team approves a set of changes then the infrastructure, the DataPlatforms assigned to consumers make changes within seconds/minutes to make the intentions in the model real. As soon as the model changes in the central repository, tools execute automatically to make the data instruction match whats expressed in the model in the repository. Intention as Code becomes reality.Add a new consumer then you will see the new Workspace provisioned along with the data pipelines feeding it data from the data producers. This allows a massive productivity boost for both data producers and consumers. They are simply out of the data pipeline business. It's just magic plumbing with datasurface. It's the same as people who use networking don't worry about the underlying network hardware or architecture, they just use the network. Datasurface does this for sharing data. As with networks evolving to newer/better technologies, Datasurface users don't worry about this either, they just see a better/cheaper cost for meeting their consumption needs.

## Overview of YellowDataPlatform

YellowDataPlatform is an implementation of the DataPlatform class in Datasurface. It uses a postgres database for all storage. It is completely driven by the metadata stored in the datasurface model. The datasurface model will provide an intention graph to the YellowDataPlatform. This intention graph describes the consumers whose Datasetgroups are assigned to the YellowDataPlatform. Each Datasetgroup has a list of DatasetSinks which describe the datastore and dataset names they want to query. The Datasetgroup is owned by a Workspace. The Workspace is assigned to a DataContainer indicating which database the consumer wishes to use to query the data. If we following the intention graph backwards then we can see exportnodes linking the datasetsinks to the datacontainer for the Workspace. We can also see ingestion nodes linking the export nodes to the source database containing the datasets required by the consumer.

The DataPlatform then validates that it supports the original source dataContainer and capture mechanism that that container supports. It should also support the export datacontainer the Workspace is connected to. It should validate that it supports the schemas used for every dataset in the pipeline. For the datasets that are supported, it will need to determine a consistent mapping of identifiers (table names, column names, datatypes) for that schema on the specific datacontainers used to store staging data, the merge tables and the views for the Workspaces.

The YellowDataPlatform focuses on a single DataContainer, a postgres database for capturing the data and the Workspaces must use the DataContainer used to capture the data.


## Scaling it up

YellowDataPlatform is designed to scale well in small and medium sized deployments. Postgres is a capable database and can scale well on a single node, especially with todays hardware. Kubernetes provides a scalable compute platform for running jobs. Airflow can scale in our case to thousands of DAGs.

### Ecosystem model scaling limits

An Ecosystem is represented in a single repository as a python DSL. Git repositories are very scalable and can handle a lot of concurrent users will proven tools for editing content in the repository. Laying out the model is different modules is key to reducing the number of conflicts and thus the usability. Please see the guidelines on laying out the ecosystem model as python. Yellow uses git clone caching and repository caching in general to reduce the load on the repository even with a lot of concurrent jobs.

### Postgres scaling limits

Yellow will create 2 tables for each Dataset in ingested in the system. A staging table where incoming changes are buffered and a merge table where the final records for consumption are stored. The merge table is either a live simple table or a milestoned table. A milestoned table uses significantly more storage space as every version of every record is kept in the system.

Each Workspace creates one or two views per DatasetSink that it contains. One view shows the live records whether the merge tables are live or milestoned, it always has only the latest records for the dataset. Milestoned datasets have an additional view which show the full history of the dataset. Use the live view unless you need to use temporal SQL queries against the full dataset.

There are 3 workloads against the database.

* Ingesting/merging data for all the datasets.
* DataTransformers generating derivative data, they read and then write the derivative data to an output table per output dataset.

Ingesting and datatransformers are the only read/write workloads against the database.

Consumers will run their queries against their Workspace views. This load, while a read only workload, is likely to be very significant if there are a lot of consumers especially with temporal queries.

Despite this, I think people will be surprised at how well this system scales compared with a spark or columnar style database. The consumer workload especially will run better against a true database than against a columnar style database, especially modern ones using technologies such as iceberg/delta lake or similar optimizations for fast writes.

### Traditional database vs columnar style databases

I have direct experience running ingestion/merge on top of parquet files on HDFS. We used Spark and Flink to implement the merge function at different points in time, finishing on flink.

Columnar formats have evolved and the current (2025) SOTA is something like Apache iceberg or Delta lake. A columnar dataset is normally stored in one or more parquet file. If you just want to add records then you can put the appended records in a new parquet file and add it to the folder for the dataset. However, updating or deleting a record is trickier. Before Iceberg, you wrote a copy of the dataset to a new file and omitted deleted records or updated the records in the new file. This is called merge on write (MoW). Make a copy an modify it as you do that. Then use the new file instead of the old one. You can heavily optimize MoW using bloom filters and partitioning techniques to minimize the number of records in the MERGE loop versus pass through unchanged records. Most records, especially in milestoned tables, are unchanged and thus copied as is to the output file. We would want the copying on unchanged records to be as efficient as possible or avoided (using partitioning) if possible.

Iceberg changed this. It still uses parquet files but adds a manifest file which has an entry to each file in the dataset for a given transaction. New transactions result in a new manifest file being added. ICeberg allowed delta tables. A delta table is just a parquet file which records also have a IUD attribute. This file would be a set of deltas to apply to the records in the previous transaction. This makes writes more efficient as you don't need to copy the entire dataset to new files for changes. Fast writes for columnar.

The problem is reads. Now, you need to read the latest transaction. Make a hash map in memory of the records already provided to the client. Read the latest parquet files, if the delta says a record was deleted then record that in the hash map with the record key. If it says record updated then if the hash map hasn't seen the record then provide to the client and add to the hash map. Once the current manifest files are read then go back to the previous manifest and do the same thing. Finally when you read a manifest containing no deltas, i.e. a snapshot, provide all records with keys not present in the hash map.

You read from the latest transaction/manifest file and go backwards to the last non-delta manifest file. If this sounds more expensive than just reading all the records from a set of parquet files then you are right. The hash map takes RAM and this is called MERGE on READ (MoR).

The problem with MoR is reading is now expensive and uses a lot of RAM. As a resulting, we must implement compaction or vacuuming. This generates a new snapshot file combining all updates/deletes and the previous snapshot in to a new baseline. So, we need to do a merge on write effectively every X batches rather than every batch in MoW as with MoR.

For datasets with relatively low updates, MoR is a good strategy but for datasets with high updates, MoW is the way to go. In a way, being able to choose MoW or MoR on a dataset by dataset basis is a good thing.

A data lake type situation has the following characteristics. Data is ingested. Then the data ingested is read by hundreds or thousands of consumers. So, lets look at MoW. On ingestion the hard work is done once, when the data is ingested. Readers just read full parquet files directly, no interpretation needed. For MoR, the writes are cheap but reads are expensive.

So, whats the best strategy? For me it's MoW all day long. The cost of writes being high is a one time cost, the cheap reads make it worth it. This means, for me, iceberg/delta lake isn't a huge advance.

Using MoR for datasets with low ingestion rates makes sense but these are also cheap ro do MoW on, there aren't many updates, right? We are using MoW for all high ingestion datasets any way so, for me, it's kind of whats the point? I may as well simplify and just use MoW everywhere.

There are 2 kinds of consumer workloads. Analytics and reporting. Analytics typically scan the input tables, they read all the records. Reporting is typically more complex, filters, joins, group bys and so on. An indexed traditional database is likely to be much better for reporting. Parquet was designed for applications which sweep through all the records as efficiently as possible. This is a different workload. It's not reporting. The costs on doing expensive reporting operations on a parquet/iceberg table are likely to be much higher than on an indexed database.

There is also the partitioning problem. Partitioning is a major strategy for optimized parquet/iceberg style tables. It allows you to avoid reading records in different partitions than you need completely. They are a poor mans index. However, who are we partitioning for? Fast ingestion or what the consumer needs which may be different. For example, ingesting transactions might be fast if we partition by transaction date. We can efficiently add new transactions to the dataset. Suppose the consumer needs records organized by account? The partitoning strategy doesn't help at all. Every consumer query needs to read every partition anyway to get all the records for an account. This is hard with a parquet/iceberg based table. It's much easier with a traditional database, we can just use an index.

The problem gets worse in a warehouse/lakehouse situation. Do you think you have just one consumer using that transaction data? Do you think they all have the same grouping requirements? Physical partitioning makes this difficult if not impossible. If you partition by consumer needs then which consumer gets to decide the partitioning strategy? You can only have one partitioning strategy per table. In my experience, this doesn't happy a lot but when it happens you can expect some very expensive compute requirements from consumers needing a different partitioning strategy from the one you chose for fast ingestion.

So, I hope this adds some color on why I think a traditional database is a better fit for reporting use cases.

### Airflow scaling limits

Airflow has a DAG per ingestion stream. There is an ingestion stream for each multi-dataset datastore. Single dataset datastores have a single ingestion stream per dataset. Thus, Yellow scales better on airflow using multi-dataset datastores. This are most common in my experience so this is a good thing.

Airflow has 2 DAGs per datatransformer. One for the transformer itself and another for ingestion the output of the transformer which is always multi-dataset.

The DAG jobs are optimized not to hog airflow when possible.

Yellow currently shares the airflow and merge database on the same postgres instance. This can easily be split, they are not dependent on each other.

Airflow can likely support thousands of DAGs in our scenario. YellowDataPlatform should be able to support thousands of ingestion streams with airflow.

### Kafka connection ingestion

The YellowDataPlatform supports CDC specifications on the source DataContainer for kafka topics. It reads those messages and write them a batch at a time to the corresponding staging table for each dataset. It uses a side table for metadata to track where it is in this process. It uses Kafka connect to write messages from the kafka topics to the postgres staging table for each dataset.

The Kafka CDC class in DataSurface will need to specify a mapping. The YellowDataPlatform will support a set of dataset to topic mappings. These are be reflected in the KafkaIngestion class in DataSurface. The data owner specifies this mapping between their datasets and the kafka topics that the events are published on. The YellowDataPlatform uses the KafkaIngestion class to configure the Confluence sink for writing kafka topics to postgres tables.

### Direct SQL ingestion

It also supports direct SQL ingestion through either a select every record style or select the changes since last run style.

## Ingestion/Merging data

The ingestion job is scheduled to run periodically and merge any outstanding batches from the staging tables to the merge tables. This jobs also make sure the schemas of those tables and all the consumers views are up to date with the schemas in the model on DataSurface before running the merge code. A security job runs to create the tables and views and assign ACL's to them for allowing consumers to query the datasets in their Workspace.

When the job runs, it will check if the schemas have changed since the last run. This state should be kept in a metadata table in postgres. If the schema has changed then it may be necessary to stop the kafka connector, change schemas and then restart the connector. Schemas may need to be changed in the kafka schema repository and in the postgres staging/merging and consumer view objects. Once schemas are changed then we update the metadata table and then restart the connector and run the merge job.

So, the scheduled job in Airflow will run independently of the kafka connector. The kafka connector is responsible for copying events from kafka to postgres staging tables. The airflow job runs code to check schema changes and then to execute the MERGE from unprocessed staging records to the MERGE tables.

### Hash columns

During this ingestion process, additional columns are added to its schema including hash's for all key columns and a hash for every column. These hashes serve to make later operations more efficient. Rather than testing every column in a key, we just compare the hashes from the 2 records. Similarly, when compared if a record has been updated, we just compare the all column hash for both records rather than every column. These hash columns are calculated using MD5 when the staging data is copied in to the staging table. 

### Snapshot merge

This merge type reads every record from a source table and makes a merge table equal to it, i.e. the merge table is a snapshot of the source table at the last ingestion time. When data is ingested, it is typically copied in to the staging table or staged. The merge table is what consumers query data from through one or more views. There is always a live view. This is just live records. This can point directly at the merge table for LIVE merges. It is a view filtering on batch_out = maxint on forensic merge tables.

## Consumer Workspace views

Each consumer Workspace gets it's own specifically named views for the dataset of interest. Each dataset in the Workspace has a workspace named view similar to "{workspace}_{store_name}_{dataset_name}_live". Consumers are not allowed to read the tables directly or other consumers views unless specifically entitled to do so. This allows Yellow to move consumers from one database to another without breaking them for scaling reasons.

The view is created by the merge job and is updated by the merge job. The merge job also creates the ACLs for the view.

## Security

Security isn't really a DataPlatform problem, the DataPlatform problem is simply to ingest, transform and export data based on what consumers want. Different companies will have different security requirements and these have nothing to do with the DataPlatform. A seperate Datasurface subsystem will have multiple implementations which customers can choose from or they can implement their own. These implementations are responsible for handling ACLs on the views ONLY. The DataPlatform will simply handle owning the staging/merge and views. The security subsystem will run as a role which has permissions to update the security settings on the Workspace views.

## Merge

There are 2 styles of merge supported. Live and milestoned. Live means consumers only need the latest live ingested records from producers. Milestoned means every version of the record is kept in the system so consumers can do historical queries as well as live queries. Effectively milestoned mode means there is a linked list of records for each record key. The linked list shows when record versions were created, updated, deleted and if necessary reinserted.

## How model changes are applied to the running system

When the model changes in the live repo, the handlerModelMerge service must be called. This will clone the repo and then call the handleModelMerge. This will load the model, validates it again and then make the changes. For the YellowDataPlatform, this works by finding all ingestion streams and datatransformers in the model and then creating a record per stream of transformer in database tables in the merge engine. These are the AirflowDAG and transformerDAG tables which are naming after the DataPlatform name. Each instance of the YellowDataPlatform has its own pair of tables.

When the ring 1 bootstrap is run, this generates the a single infrastructure DAG file. This is copied to the airflow DAGs folder. This DAG uses 3 tables populated by the handle model merge service. These tables are the AirflowDAG, transformerDAG and factory_dag tables. The factory_dag table is each YellowDataPlatform instance in the model. The infrastructure DAG dynamically creates 2 factory DAGs per record or per DataPlatform instance. One for creating the ingestion DAGs and another for creating the transformer DAGs. The AirflowDAG and transformerDAG tables are used to create the actual DAGs for the streams and transformers.

Thus, the infrastructure DAG is a hierarchy of model driven DAGs needed to run the YellowDataPlatform.

1. Model level DAG, the infrastructure DAG
2. DataPlatform instance level DAGs, the platform ingestion factory DAG and the platform transformer factory DAG
3. Ingestion DAGs, dt Ingestion DAGs and transformer DAGs.

Level 2 is dynamically created by the infrastructure DAG using records from the factory_dag table.
Level 3 is dynamically created by the platform DAGs in level 2 using records from the AirflowDAG and transformerDAG tables.

All these tables are populated by the handle model merge service.

Thus, if the model changes, then nothing changes until the handle model merge service is run. Datasurface customers can choose to just do updates once a day or every few minutes, it's up to them or on the use case. A development system can run very frequently or even be triggered when git commits are detected on the live model repo. Production systems can have no checks and balances.

