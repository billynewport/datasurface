[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE-OF-CONDUCT.md) 

> :warning: **Work in progress**: This project is being implemented in phases and has not reached a fully functional state at this time. Please see the [roadmap](ROADMAP.md) for details on when features should drop and the road map.

# Data surface, a data ecosystem broker

The goal of this project are:

* introduce a new concept, the ecosystem broker which arbitrates between the data actors and the catalog of available data platform products to meet the data actors needs
* is to raise the abstration level for the actors using data in an enterprise
* to turn data pipelines in to a commodity product which allows an ecosystem to continuously choose the best product for a consumer use case over time automatically
* data pipelines become a second order technology which data actors do not need to be concerned with. Previously, data pipelines were a first order technology which data actors worked directly with.
* introduce a new concept, the ecosystem broker which arbitrates between the data actors and the catalog of available data platform products to meet the data actors needs
* making data pipelines a second order technology is a game changer for both the data actors in an enterprise as well as for the enterprise in that data pipelines can be eliminated from the technical debt that must be carried
* better govern and control the data ecosystem of a firm as all data can be easily moved around a firm using the ecosystem approach using a declarative framework.

Enterprise developers should be performing only the following tasks:

* Producing new data
* Consuming existing data to extract value
* Producing derivative value added data from existing data

Nobody should be writing data pipeline code. The data ecosystem should be able to infer the data pipelines from the above metadata describing the data ecosystem. This metadata is a model of the data ecosystem. The model is stored in a git repository and is the official version of truth for the data ecosystem. The model is used to create and maintain data pipelines. The model is also used to provide governance and control over the data within the enterprise.

This model has contributions from ecosystem managers, data governors, data producers, data consumers, data transformers and data platforms. Together, a new element, the Ecosystem broker, interprets the intentions of these actors and creates the data pipelines needed to support the intentions in the model as well as makes sure these pipelines do not violate the governance policies of the enterprise.

This model is expressed using a Python DSL and is stored in a github repository which serves as the official version of truth for the model. Authorization for modifying different elements of the model is federated. Each region of the model can be assigned to be modifiable using only pull requests from a specific github repository.

Datasurface uses github in a novel way, at least to our knowledge. You can read more about the [approach here](docs/HowGitHubIsUsed.md).

The main repository uses github action handler to make sure the central model stays self consistent, backwards compatible and consistent with the governance policies of the enterprise.

DataSurface, the broker, then arbitrates between the desires of the consumers of the data ecosystem and the available DataPlatforms. DataSurface selects a DataPlatform for each consumer. DataSurface then generates the data flow graph for each Dataplatform in use. This graph includes all consumers assigned to that DataPlatform as well as the data pipelines from those consumers back through any data transformers all the way to Data Producers. [See the DataPlatform section for more information](docs/DataPlatform.md)

The DataPlatforms then make that graph a reality by creating the infrastructure to support that graph. There will be an initial set of DataPlatforms provided with DataSurface and others can be added over time as technology progresses. Consumers may be reassigned to a newer/improved DataPlatform over time and this should be transparent to the consumer. This means less technical debt as "pipelines" are automatically upgraded over time by DataSurface to use Dataplatforms using latest technology. The choice of the technology is not something producers or consumers should be concerned with, they just indicate what they need. Everything else, well it's just plumbing under the surface.

Please see the [Getting started document for more information](docs/GettingStarted.md).
