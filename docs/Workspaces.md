# Workspaces

A consumer of data of an ecosystem must define a Workspace within a Team within a GovernanceZone. Users can choose to have Teams for managing just workspaces or Teams which manage Datastores and Workspaces, its a choice.

A consumer describes the non functional requirements of the workspace using a WorkspacePlatformConfig. This allows the consumer to specify how much data latency is desirable. Whether the data needs to be milestoned or not. How long data should be retained for and so on. The ecosystem will then use this information to choose a DataPlatform to host the data pipeline for the Workspace.

Consumers may also specify they want to use multiple of these for a single application. A Workspace has multiple DatasetGroups. A DatasetGroup has one or more DatasetSinks. EAch DatasetSink describes a dataset which the consumer needs for their application. Each DatasetGroup can specify its own WorkspacePlatformConfig. A Dataset can be specified multiple times within a single Workspace, once per DatasetGroup. This is allowed so consumer may require a live, low latency version of a dataset as well as a much high latency version of the same dataset which is fully milestoned.

