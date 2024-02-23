# Getting Started

## Setup the main github repository and its workflows

The main github repository is the source of truth for the data ecosystem. It contains the model of the data ecosystem. The model is used to create and maintain data pipelines. The model is also used to provide governance and control over the data within the enterprise. We need to install the github action handler to make sure the central model stays self consistent, backwards compatible and consistent with the governance policies of the enterprise.

[See this document for an overview of how DataSurface uses Github](HowGitHubIsUsed.md)

## Define the top level ecosystem, infrastructurevendors, dataplaforms and goverance zones

The ecosystem is the top of the model. It defines the infrastructure vendors that the ecosystem can work with. Each vendor also defines the locations they provide to host data and/or compute. The ecosystem also defines the data platforms that can be used to fulfill the data movement requirements of consumers of data within the ecosystem.

Next, governance zones can be declared by adding a GovernanceZoneDeclaration for each governance zone. This defines the name of the zone as well as the github repository/branch which is used for authoring the governance zone. Only the main ecosystem repository can be used to declare a zone. The governance zone cannot be deleted until the governance zone github repo removes references to the governance zone.

For more information on different aspects of the system see the following documents:

* [Ecosystem](Ecosystem.md)
* [Governance Zones](GovernanceZone.md)
* [Dataplatforms](DataPlatform.md)
* [Teams](Teams.md)
* [How github is used by Datasurface](HowGitHubIsUsed.md)
* [Datastores](Datastores.md)
* [Workspaces](Workspaces.md)
* [Data Transformers](DataTransformer.md)
* [Data Containers](DataContainers.md)
* [Data Classification System](DataClassification.md)
