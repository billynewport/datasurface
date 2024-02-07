# Introduction to Dataplatforms

The main catalog has the current data requirements as its metadata. It has metadata describing producers, consumers, data transformations and data platforms. Data platforms are used to handle clients data transport requests. The catalog will select a specific platform to service a specific Workspace. Thus, a platform will likely be handling data store ingestions, exports to workspaces, all the job scheduling to make the data physically move. A data platforms job is to take the latest set of changes and then render them in to "reality". When the catalog metadata changes then the data platform needs to adjust to correctly render the new requirements as changes on the existing IaC render without breaking anything or the clients being aware this is happening. Please note, rendering a catalog image can be very expensive and/or time consuming. For example, a client just created a Workspace with 500GB of data present in the datasets being used by the Workspace client. Preparing the database used by the Workspace may takes hours or even days depending on the data volumes. Once the current metadata is rendered then the platform is now using that version. The catalog metadata will change as clients create more data producers and client workspaces.

The platform must then try to iterate on the current deployment and take in to account the latest changes. Normally, a platform must finish the current render ASAP and then repeat for the latest changes. However, operational constraints may prevent this from happening at a fast rate.

Dataplatforms should be able to render large environments. Maybe 10k datastores, 100k datasets, hundreds of data containers for consumers to query and extract value from the data. These environments also run thousands to millions of jobs a day to execute the pipeline. These jobs also have to be updated when a new metadata push arrives. It's recommended that platforms can shard themselves to make the catch up renders faster. Data can also be arriving slowly or rapidly.

However, no matter how many dataplatforms use data from a particular store, they need to coordinate and pick a single platform to run data ingestion jobs. The ingestion load on any particular data store should always be 1 ingestion. Thus, platforms may have primary/exclusive ingestion status for a subset of datastores that they are managing. Other data platform will then connect to the ingestion storage of the primary platform when they need the data.

Platform instances should be stored in the catalog and provisioned when data platforms move to a new pipeline render.

A data platform is a mechanism to handle the follow chores:

* Manage a job scheduler to coordinate the follow

* Ingest data from a data store and persistently store in a data container Once, data is ingested then the data platform is also responsible to incrementally propogate data changes for a store to downstream consumer Workspaces.

* Track the retention requirements of Workspaces and make sure that the data can be reproduced for the required amount of time. This could be as much as 5 years so it's unlikely that a simple file format such as Apache Iceberg or Deltalake are sufficient. The data platform will likely need to store the data in a milestoned format to do this. File formats such as Iceberg or DeltaLake are not designed to handle long term retention required. They will typically try to compact/vacuum/conflate the deltas in the files to produce a new snapshot and once the snapshot is created then the deltas are deleted and thus the ingestion pull by pull changes are lost.

* Allow multiplatform fabrics to cooperate to connect data producers to data consumers. There may be a platform handling AWS whilst another platform handles AZURE and so on. The broker is responsible to define a protocol enabling data to flow between platforms.

* Data platforms will have their own operational state. This state might consist of its place ingestion a database using CDC. This state will likely by kept in an OLTP database.

* Dataplatforms are typically rendering using a specific branch from the github catalog. When teh github catalog accepts new changes then the Dataplatform will need to advance to the next branch and then figure out what is the delta between the current rendering and the new proposed one. A Dataplatform will typically use generated IaC scripts to render the metadata described in the catalog. The IaC should be completely generated from the catalog metadata. Data platforsm do not need to advanced instantly when the main catalog is updated. Changes may be delayed till long weekends for example. Development environments with a dataplatform need to advance much faster as developers are waiting for their changes to be pushed rapidly.
