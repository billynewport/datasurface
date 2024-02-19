# Introduction to Dataplatforms

The main ecosystem model has the current data requirements as its metadata. It has metadata describing producers, consumers, data transformations and data platforms. Data platforms are used to handle clients data transport requests. The model will select a specific platform to service a specific Workspace. Thus, a platform will be handling data store ingestions, exports to workspaces, triggers for and the execution of data transformer computations and all the job scheduling to make the data physically move.

A data platform has two jobs.

## Materialize an intention graph in to a physical system to fulfill the intentions of the graph

The work that a data platform has to do in terms of the data pipelines is provided as a graph of nodes from the Ecosystem. The DataPlatform must then take that graph and then generate a system which can fulfill the intentions expressed in that graph. The graph consists of nodes depicting the following actions:

* Ingest data from a Data producer
* Materialize or Export Data in an asset for use by a Workspace
* Evaluate a trigger a data transformer job
* Execute a data transformer

The following represents a small intention graph. The Dataplatform has a single asset or database for all data pipeline steps to use, "Test Azure SQL". A consumer wants 3 datasets, 2 come from the NW_Data store, 1 comes from the Masked_NW_Data store. These are exported to the Workspace asset in steps 6,7 and 8. The 2 original datasets are ingested in step 1 (all 3 datasets: [products, suppliers and customers] are ingested together). Step 2 exports the original sensitive customer dataset to the datatransformer workspace "MaskCustomersWorkSpace". Then the datatransformer is executed in step 4 after being triggered in 3. The execution of the datatransformer causes the newly processed masked customer records to be ingested to the ecosystem in 5. The masked customer records are then exported to the consumer asset in 6.

This can be done using streaming or batch or forensic batch. The graph is just an intention graph.

```text
1. IngestionMultiNode/Ingest/Azure Platform/NW_Data
2.  -> (  ExportNode/Export/Azure Platform/Test Azure SQL/NW_Data/customers
3.  -> (    TriggerNode/TriggerMaskCustomersWorkSpace
4.  -> (      DataTransformerNode/DataTransfomer/MaskCustomersWorkSpace
5.  -> (        IngestionMultiNode/Ingest/Azure Platform/Masked_NW_Data
6.  -> (          ExportNode/Export/Azure Platform/Test Azure SQL/Masked_NW_Data/customers)))),
7.       ExportNode/Export/Azure Platform/Test Azure SQL/NW_Data/products,
8.       ExportNode/Export/Azure Platform/Test Azure SQL/NW_Data/suppliers)
```

A data platforms job is to take the latest set of changes and then render them in to "reality". For example, an Amazon AWS DataPlatform will take its graph and possibly convert it in to AWS Glue workflows.

When the ecosystem changes then the intention graph will also change. The Dataplatform must take the new version of its intention graph and render it on top of the existing system setup on the last iteration. The new render should minimize disruption and be as seamless as possible for the users. If the new graph simply represents that a consumer added a new Dataset to a Workspace then ideally, just create the table/views, entitle them, populate the table and then keep it up to date. If that dataset wasn't previously being ingested on this data platform then ingest it also. If the data platform is using Terraform on top of its physical infrastructure then terraform may be able to implement the changes in the new intention graph based on the old render.

Dataplatforms should be able to render large environments. Maybe 10k datastores, 100k datasets, hundreds of data containers for consumers to query and extract value from the data. Most cloud solutions are incapable of handling such large graphs. The Dataplatform will need a strategy to break up the total graph in to manageable chunks and linking them together in such a way that the infrastructure used by the DataPlatform can deal with it. This is an advantage of DataSurface over using vendor tools directly. This chunking isnt a problem users should need to deal with. Plus, the complexities of how these chunks running on different cloud vendors need to interact or be optimized similarly isn't something users should need to deal with. This is the job of the DataPlatform.

These environments also run thousands to millions of jobs a day to execute the pipeline. These jobs also have to be updated when a new metadata push arrives. It's recommended that platforms can shard themselves to make the catch up renders faster. Data can also be arriving slowly or rapidly.

However, no matter how many dataplatforms use data from a particular store, they need to coordinate and pick a single platform to run data ingestion jobs. The ingestion load on any particular data store should always be 1 ingestion. Thus, platforms may have primary/exclusive ingestion status for a subset of datastores that they are managing. Other data platform will then connect to the ingestion storage of the primary platform when they need the data.

Platform instances should be stored in the Ecosystem and provisioned when data platforms move to a new pipeline render.

A data platform is a mechanism to handle the follow chores:

* Manage a job scheduler to coordinate the follow
* Ingest data from a data store and persistently store in a data container Once, data is ingested then the data platform is also responsible to incrementally propogate data changes for a store to downstream consumer Workspaces.
* Track the retention requirements of Workspaces and make sure that the data can be reproduced for the required amount of time. This could be as much as 5 years so it's unlikely that a simple file format such as Apache Iceberg or Deltalake are sufficient. The data platform will likely need to store the data in a milestoned format to do this. File formats such as Iceberg or DeltaLake are not designed to handle long term retention required. They will typically try to compact/vacuum/conflate the deltas in the files to produce a new snapshot and once the snapshot is created then the deltas are deleted and thus the ingestion pull by pull changes are lost.
* Allow multiplatform fabrics to cooperate to connect data producers to data consumers. There may be a platform handling AWS whilst another platform handles AZURE and so on. The broker is responsible to define a protocol enabling data to flow between platforms.
* Data platforms will have their own operational state. This state might consist of its place ingestion a database using CDC. This state will likely by kept in an OLTP database.
* Dataplatforms are typically rendering using a specific branch from the github ecosystem. When the github ecosystem accepts new changes then the Dataplatform will need to advance to the next branch and then figure out what is the delta between the current rendering and the new proposed one. A Dataplatform will typically use generated IaC scripts to render the metadata described in the Ecosystem. The IaC should be completely generated from the Ecosystem metadata. Data platforsm do not need to advanced instantly when the main Ecosystem is updated. Changes may be delayed till long weekends for example. Development environments with a dataplatform need to advance much faster as developers are waiting for their changes to be pushed rapidly.
