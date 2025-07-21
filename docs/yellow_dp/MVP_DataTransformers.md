# MVP for DataTransformers in YellowDataPlatform

Time to get DataTransformers working in YellowDataPlatform. A DataTransformer is code which executes against a Workspace and produces an output which is then ingested in a new Datastore. For example, we could make a DataTransformer which takes a customer table with PII and removes the PII producing a new Datastore containing the customer data without PII through masking for example. Everytime the source customer data changes, we run the DataTransformer and ingest the changes to the output Datastore. The code for the DataTransformer will be packaged in a docker container and run in a job pod. The job pod should be called when any of the datasets on the Workspace change or ingest another batch.

## Quick introduction to DataTransformers

Ingesting data is one thing but systems also need to be able to produce derivative data based on data already in the system. DataTransformers are the way to do this. They define a process which takes a Workspace with its list of datasets and produces derivative data stored in a new DataStore. The code for this transformation is packaged as a CodeArtifact in the model. YellowDataPlatform will use docker container images to package this code and run it in a kubernetes job pod when it's needed. Any credentials and the Workspace DataContaienr connection info will be provided to the DataTransformer code as environment variables.

## The Ecosystem Model and the Pipeline Graph

The source of truth for the system is the Ecosystem model. This is transformed in to an intention graph which describes the system as a whole in a form which is then provided to DataPlatforms who turn the graph in to infrastructure to realize that intention using the technology stack implemented in the DataPlatform. For example, YellowDataPlatform realizes the intention graph using Kubernetes, Airflow and a merge database engine, currently Postgres.

The graph for around DataTransformers looks like this. This should be all the information a DataPlatform needs to render the processing pipeline graph. This would include
    provisioning Workspace views, provisioning dataContainer tables. Exporting data to dataContainer tables. Ingesting data from datastores,
    executing data transformers. We always build the graph starting on the right hand side, the consumer side which is typically a
    Workspace. We work left-wards towards data producers using the datasets used in DatasetGroups in the Workspace. Any datasets
    used by a Workspace need to have exports to the Workspace for those datasets. All datasets exported must also have been
    ingested. So we add an ingestion step. If a dataset is produced by a DataTransformer then we need to have the ingestion
    triggered by the execution of the DataTransformer. The DataTransformer is triggered itself by exports to the Workspace which
    owns it.
    Thus the right hand side of the graph should all be Exports to Workspaces. The left hand side should be
    all ingestion steps. Every ingestion should have a right hand side node which are exports to Workspaces.
    Exports will have a trigger for each dataset used by a DataTransformer. The trigger will be a join on all the exports to
    a single Workspace and will always trigger a DataTransformer node. The Datatransformer node will have an ingestion
    node for its output Datastore and then that Datastore should be exported to the Workspaces which use that Datastore.

    The priority of each node should be the highest priority of the Workspaces which depend on it.


```mermaid


## How will a DataTransformer work?

The DataTransformer runs against all the datasets in a Workspace. It creates its outputs by writing them to tables in the Workspace DataContainer. The system then ingests the output tables into a the DataTransformers output Datastore as normal. The DataTransformer can use its output Datastore as an input to itself, just specify it in the Workspace. The tables to hold the output have a special name in the DataContainer. The table names produce the normal way but have a prefix of 'dt_'. This is enough to make then unique as only one DataTransformer can produce a specific output Datastore. The tables could be truncated before each run to ensure they are empty and must always be multi-dataset stores.

The security module should make the dt_ tables writeable by the DataTransformer credentials and dev ops members. Note, ideally, a devops team member should only be able to access these tables or any aspect of the system using a traceable temporary credential whose assignment is approved by a senior devops member. Nobody should have default access to the system. These approvals should be based on a ticket being opened for a task which requires access to the system. All access including terminal logs, commands executed, keystrokes if needed should be logged. All actions performed by the temporary credential should be logged and reviewed quickly by a senios devops member.

## Factory DAG modifications

The factory DAG needs to create the extra DAGs for the DataTransformers. For each DataTransformer it will need:

* A task sensor monitoring all the datasets on each DataTransformers Workspace using an OR operator.
* A DAG to execute the DataTransformer code in a job pod triggered by the task sensor.
* A task which runs after the DataTransformer code completes to trigger a normal ingestion job for the output Datastore tables.
