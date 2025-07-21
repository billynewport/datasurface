# MVP for DataTransformers in YellowDataPlatform

Time to get DataTransformers working in YellowDataPlatform. A DataTransformer is code which executes against a Workspace and produces an output which is then ingested in a new Datastore. For example, we could make a DataTransformer which takes a customer table with PII and removes the PII producing a new Datastore containing the customer data without PII through masking for example. There are many examples we could use for producing derived data.

* Changing the schema of input data.
* Joining data with other data, materializing a view.
* Masking data.
* Aggregating data.
* etc.

Everytime the source customer data changes, we run the DataTransformer and ingest the changes to the output Datastore. The code for the DataTransformer will be packaged in a docker container and run in a job pod. The job pod should be called when any of the datasets on the Workspace change or ingest another batch. Indeed, a version of a DataTransformer could be a materializing view which refreshes on a timer. Thats a viable alternative to coding one in python or Spark and one what DataSurface will support. However, materialized views only produce a single view/dataset as output so it's a question of supporting both approaches.

The DataTransformer code is packaged by its owner in its own GitHub repository. They would have a normal test cycle around this before releasing a new version. The Ecosystem model is also versioned. A DataTransformer in the model will reference a specific version of the code. The output Datastore and associated datasets/schemas are also versioned together with the DataTransformer code in the model. When the DataTransformer owner adds a DataTransformer then they need to define the Workspace/DSG, the DataTransformer and the output Datastore as one commit really.

Some points to consider. The customer data transformer code runs in a managed environment whose maintenance and code levels are controlled centrally. It's important that no platform code (or as little as possible) is present in the customer managed code/artifacts. This is essential as customer data transformer release cycles will not align with the platform release cycles and holding up a platform release is not acceptable. The platform must also ensure that customer data transformer code is run securely and that the code is not able to access any other data in the system. The platform must also ensure that the code once working, stays working after platform updates.

## Quick introduction to DataTransformers

Ingesting data is one thing but systems also need to be able to produce derivative data based on data already in the system. DataTransformers are the way to do this. They define a process which takes a Workspace with its list of datasets and produces derivative data stored in a new DataStore. The code for this transformation is packaged as a CodeArtifact in the model. YellowDataPlatform will use github repositories to package this code and run it in a kubernetes job pod when it's needed. Any credentials and the Workspace DataContainer connection info will be provided to the DataTransformer code as environment variables.

## The Ecosystem Model and the Pipeline Graph

The source of truth for the system is the Ecosystem model. This is transformed in to an intention graph which describes the system as a whole in a form which is then provided to DataPlatforms who turn the graph in to infrastructure to realize that intention using the technology stack implemented in the DataPlatform. For example, YellowDataPlatform realizes the intention graph using Kubernetes, Airflow and a merge database engine, currently Postgres.

An Ecosystem can be converted in to a PlatformPipelineGraph per DataPlatform instance. This provides the ingestion graph assigned to each DataPlatform. Each graph has all the data pipeline stages to support the subset of Workspace/DSG pairs assigned as best serviced by that DataPlatform instance. Datastores and DataTransformers will be duplicated across these graphs. This is because one consumer Workspace might request a forensic milestoning approach. Another consumer with the same data might request a live approach. This should result in each consumer being assigned to different DataPlatforms because of their different requirements. This means the data will be duplicated and ingested seperately across the different DataPlatforms. This is fine because DataSurface policies and rules are still applied to the data regardless of where it's stored/ingested.

The ingestion graph (PlatformPipelineGraph) should be all the information a DataPlatform needs to render the processing pipeline graph. This would include provisioning Workspace views, provisioning dataContainer tables. Exporting data to dataContainer tables. Ingesting data from datastores, executing data transformers. We always build the graph starting on the right hand side, the consumer side which is typically a Workspace. We work left-wards towards data producers using the datasets used in DatasetGroups in the Workspace. Any datasets used by a Workspace need to have exports to the Workspace for those datasets. All datasets exported must also have been ingested. So we add an ingestion step. If a dataset is produced by a DataTransformer then we need to have the ingestion triggered by the execution of the DataTransformer. The DataTransformer is triggered itself by exports to the Workspace which owns it. Thus the right hand side of the graph should all be Exports to workspaces. The left hand side should be all ingestion steps. Every ingestion should have a right hand side node which are exports to Workspaces. Exports will have a trigger for each dataset used by a DataTransformer. The trigger will be a join on all the exports to a single Workspace and will always trigger a DataTransformer node. The Datatransformer node will have an ingestion node for its output Datastore and then that Datastore should be exported to the Workspaces which use that Datastore. The priority of each node should be the highest priority of the Workspaces which depend on it.

## DataTransformer versioning and rollback

The DataTransformer code is packaged by its owner in its own GitHub repository. They would have a normal test cycle around this before releasing a new version. The Ecosystem model is also versioned. A DataTransformer in the model will reference a specific version of the code. The output Datastore and associated datasets/schemas are also versioned together with the DataTransformer code in the model. When the DataTransformer owner adds a DataTransformer then they need to define the Workspace/DSG, the DataTransformer and the output Datastore as one commit really.

The first time a DataTransformer is added to the model, the normal Datasurface action handlers will check for normal issues and it will be committed to the main branch.

When the DataTransformer is updated, the new repository version will be specified along with any output Datastore schema changes. The output schema changes MUST be backwards compatible with the previous version OR the action handlers will reject the commit because of an uncompatible change. Dataset's can be added or modified in a backwards compatible way only.

Truly rolling back a breaking schema change is difficult. This is because it's not just about the code, it's about the data previously ingested and used by consumers, different teams who may have sent reports out based on the old version. That data is now "published" and may need to be retained for audit/regulatory reasons. The data can't be deleted until the retention period is over. This means, the only realistic way to do a breaking change is to create a new DataTransformer and deprecate the old one. Existing consumers need to migrate over to the new output Datastore. This is usually done following this process:

1. Consumers likely have a deprecation policy of never.
2. When the DataTransformer is marked as deprecated, the git action handlers will fail the commit indicating which consumers are not allowing this (NEVER).
3. The DataTransformer owner needs to talk with the consumers and get them to change their Workspaces to allow deprecation.
4. Now, the DataTransformer owner can mark the original DataTransformer as deprecated and the commit is now possible as consumers are now allowing it.
5. The new DataTransformer should be made available.
6. Consumers should now be flagged by reports as using a deprecated Datastore in their production Workspaces. This pressure will force them to migrate to the new output Datastore.
7. Once all consumers have migrated, the old DataTransformer can be marked as demised but cannot be deleted. The data still needs to be retained for audit/regulatory reasons.

This may seem like a lot of work but being fined a billion dollars for mistakes isn't fun either. Some organizations can specify policies which relax the situation but this implementation encourages collaboration and communication between the DataTransformer owner and the consumers. It's important for data producers to know that once they publish data and other people use it then they are responsible for it for as long as those consumers use it (including retention periods). This awareness is key so teams cannot say they didn't realize their data was being used for critical reporting.

## How this works in YellowDataPlatform

YellowDataPlatform, for now, uses the merge store database for everything, all staging and merge tables, workspace views and DataTransformer output tables. Workspaces must use the data container DataPlatformManagedDataContainer. This is a special data container which means the Workspace is placed on a DataContainer by the DataPlatform and not the consumer. YellowDataPlatform will always use the merge data container for these Workspaces.

YellowDataPlatform dynamically generates ingestion DAGs for each ingestion stream assigned to it. These all end up in the merge tables of its assigned merge data container. Yellow currently only uses the ingestion nodes in the graph. Every Workspace in the model assigned to the DataPlatform will also be present in the graph. We will see ExportNodes from every ingestion node contained in the Workspace (all DatasetSinks in the DSGs for the Workspace). The DataContainer specified on the export nodes will be the Workspace DataContainer or DataPlatformManagedDataContainer. Workspaces with a DataTransformer will cause every export node to it to have a TriggerNode followed by a DataTransformerNode followed by an ingestion node for its output Datastore. If the output datastore is also used by another Workspace then the output Datastore ingestion node will be followed by an Export node to consuming Workspaces and so on.

So, we need to find the DataTransformer nodes in the graph. The left hand side nodes are all the ingestion nodes it depends on. The right hand side node should be a single ingestion node for the output datastore. The output datastore will have export nodes following it because to be in the graph, it must have consumers and the Datatransformer node and associated export nodes would also be absent if noone used it. Remember, if noone is using it then Datasurface doesn't ingest it.

The ingestion DAG for the output datastore is already taken care of, it will be in the storesToIngest list for the ingestion graph. DataTransformers output DataStores must have a cmd which is a DataTransformerOutput. This is a special cmd which is used to indicate that the datastore is an output of a DataTransformer. This is now lint checked by DataTransformer in the DSL. Users dont specify how to ingest the output datastore of a DataTransformer, it's done automatically by the DataPlatform.

## The DataTransformerJob

This is a new job type which is used to execute the code in the DataTransformer within the YellowDataPlatform. It runs within the container image of standard datasurface container image. The job runs using a datatransformer instance specific credential, not any system credentials. This also prevents the datatransformer from accessing any other data in the system. The credential it uses to access the DataContainer, the merge engine in this case, should also be supplied. This also needs to be specific to the DataTransformer instance. The YellowDataPlatform would need to generate these credentials as needed, part of handleModelMerge and then provide them to the job.

I felt uncomfortable using a customer container image to run this code. We have no control over the version of datasurface used in the container. This was a maintenance problem for me in the past when clients created spark jobs in their own jars with the libraries packed in. We ended up trying to shim in fixes to those libraries as it was hard to get all clients to recompile their spark job jars and update them. The same thing may happen here.

So, rather than using a container image in the CodeArtifact, we will make a new CodeArtifact which specifies a github repo containing the code. There will be an expectation of a transformer.py file containing a standard function (executeTransformer) which will be called by the DataPlatform to run the code. The data platform will provide the standard container image to run the job in. The main code will start a transaction against the merge engine, truncate the output tables and then within that transaction, call the transformer.py function passing the connection. The datatransformer is expected to write the output to the dt_ output tables using that connection. The transaction is then committed in the main function. When the job completes, the ingestion job for the output tables should be triggered. This is always a multi-dataset ingestion job.

The data transformer should also be provided with a dict[str,str] which maps from store#dataset name to the table name for every datasetsink in the Workspace. The output datastore and datasets is also present in the dict but prefixed with 'dt_' to avoid collisions with the input tables.

## How will a DataTransformer work?

The DataTransformer runs against all the datasets in a Workspace. It creates its outputs by writing them to tables in the Workspace DataContainer. The system then ingests the output tables into a the DataTransformers output Datastore as normal. The DataTransformer can use its output Datastore as an input to itself, just specify it in the Workspace. The tables to hold the output have a special name in the DataContainer. The table names produce the normal way but have a prefix of 'dt_'. This is enough to make then unique as only one DataTransformer can produce a specific output Datastore. The tables could be truncated before each run to ensure they are empty and must always be multi-dataset stores.

The security module should make the dt_ tables writeable by the DataTransformer credentials and dev ops members. Note, ideally, a devops team member should only be able to access these tables or any aspect of the system using a traceable temporary credential whose assignment is approved by a senior devops member. Nobody should have default access to the system. These approvals should be based on a ticket being opened for a task which requires access to the system. All access including terminal logs, commands executed, keystrokes if needed should be logged. All actions performed by the temporary credential should be logged and reviewed quickly by a senior devops member.

## Factory DAG modifications

The factory DAG needs to create the extra DAGs for the DataTransformers. For each DataTransformer it will need:

* A task sensor monitoring all the datasets on each DataTransformers Workspace using an OR operator.
* A DAG to execute the DataTransformer code in a job pod triggered by the task sensor.
* A task which runs after the DataTransformer code completes to trigger a normal ingestion job for the output Datastore tables.
