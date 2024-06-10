# Getting Started

## GitHub

This project is not dependent on GitHub. It can be used with any CI/CD capable repository. GitHub is simply the first CI/CD plugin that has been written. There will be more, GitLib is next.

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
* [Data Types and Schemas](DataTypes.md)
* [Data Transformers](DataTransformer.md)
* [Data Containers](DataContainers.md)
* [Data Classification System](DataClassification.md)
* [REST APIs to access the models](RESTAPI.md)

## Commands to make a set of changes

You can find a tutorial on getting started [here](Tutorial.md)

## DataSurface Performance

Here, we try to identify the performance aspects of DataSurface. We will consider the performance of the DataSurface model validation, the CI/CD speeds, the DataPlatform performance and the DataPlatform monitoring. For more information see [Performance](Performance.md)
