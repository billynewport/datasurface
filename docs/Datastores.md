# Datastores and datasets

A data producer should define a Datastore representing the data that they wish to contribute to the ecosystem. The datastore is a named group of named datasets. Each dataset can be thought of an a collection of records with a common schema. The datastore also has an associated CaptureMetaData. This describes how a data platform can ingest data from this datastore. The data platform will use this information to create a data pipeline to ingest the data from the datastore and distribute it as needed to workspaces that the data platform is supplying data to.

The creation of a datastore and its associated metadata does not cause any data movement to occur. It is simply a declaration of the data that is available for use by the ecosystem. If a consumer references a dataset in a datastore from a DatasetGroup in a Workspace then the data platforms associated with the Datasetgrioups within that workspace will create data pipelines to move the data from the datastore to the workspace. The ecosystem will try to limit the number of data pipelines ingesting data from a datastore to one. This is to minimize the load on the producer from the ecosystem pipelines using their data.

A data producer with a number of datastores may choose to define their own Team with its own github repository to manage these datastores. A data producer who wishes to see the data should also create a Workspace referencing all the datasets/datastores. The producer can then use the Workspace together with their favorite data query tool to observe the data.

## Schema contract to Consumers

The schema the data producer exposes to the ecosystem through the datasets is the public facing schema or contract the producer agrees to honor to the consumers of the data. The producer may choose to create a Workspace with a data transformer connected to the original datastore which produces the public facing version of the data. The producer can then simply not allow any other Workspaces to reference the original datastore. Consumers can be sent to the data transformer output Datastore with the schemas the producer is willing to support. If the private datastore schema changes then these changes can be masked using the data transformer.

## Deprecation of data protocols

A producer can specify a deprecation status on a dataset or datastore. This is used to indicate that the data is either:

* NOT_DEPRECATED; Available for use

* DEPRECATED; Will shortly be removed or modified in a non backwards compatible fashion

A Workspace owner can specify that a DatasetSink can:

* Accept that the dataset can be deprecated by the producer

* Not allow datasets to be deprecated. The producers needs to talk with the Workspace consumer to see if the dataset can be deprecated.

The protocol is that a producer can first mark a dataset as deprecated and then later when noone is using it, remove or modify the dataset schema in a non backwards compatible fashion. First, the data producer will try to mark the dataset as deprecated. The pull request to the main repository can succeed in which case the change was successful. However, this is only possible if there are no Workspaces using the dataset OR the Workspaces that are using the dataset have acknoledged that the DatasetSink referencing the dataset can tolerate deprecated datasets. The protocol is, first the data producer tries to mark the dataset as deprecated. If this fails to merge then the data producer can look at the errors in the pull request and easily identify the Workspaces refusing to use deprecated datasets. The producer must then have a conversation out of band with the Workspace owners to see if the dataset can be deprecated. The Workspace owners may use an alternative dataset. The Workspace owners may also agree to the deprecation and then mark the datasetsink as allowing deprecation. This then allows the producer to deprecate the dataset. The Workspace owner then figures out a schedule for removing the dependency on the deprecated datasets. The Workspace owner changing the datasetsink is an acknowledgement that the conversation has taken place and that the Workspace owner is aware the dataset is deprecated and has a plan to remove the dependency on the dataset.

