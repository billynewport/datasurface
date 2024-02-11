# Introduction

The goal is this project is to eliminate data pipelines as a manual task within an enterprise as well as provide better governance and control over the data within the enterprise because the movement of data is driven through automation by metadata. It uses an enterprise model describing the data ecosystem of the enterprise. This model has contributions from ecosystem managers, data governors, data producers, data consumers, data transformers and data platforms. Together, a new element, the Ecosystem broker, interprets the intentions of these actors and creates the data pipelines needed to support the intentions in the model as well as makes sure these pipelines do not violate the governance policies of the enterprise.

This model is expressed using a Python DSL and is stored in a github repository which serves as the official version of truth for the model. The model is decomposed in to regions which are under the control of different teams, each team with its own repository. Each team has a repository which is a fork of the main repository and this team repository is the only place from which changes to that teams region can be made and accept by the main repository.

A team can clone the main repository and then make changes to the model in their team repository. A pull request can then be made against the main repository. The main repository has a set of handlers which enforce the governance policies of the enterprise. The handlers are responsible for ensuring that the model is consistent and that changes to the model are consistent with the governance policies of the enterprise.

This allows the data ecosystem to leverage github authorization, authentication and workflow systems. Action handlers are used to vet incoming pull requests from the actor repositories to the primary github repository. These action handlers perform the following actions before allowing a pull request to succeed:

* Ensures the objects changed in a pull request can be changed from the github repository sourcing the pull request.

* Ensures the model described in the pull request is self consistent and contains no errors.

* Ensures the incoming model is backwards compatible with the existing model.

* Ensures the incoming model is consistent with the governance policies of the enterprise.

Once all of these are satisified then the action handler will allow the pull request to succeed. The pull request is then merged into the main repository and the model is updated. The model can then produce data pipeline graphs for each data platform.

Data platforms can then use the model to create and maintain data pipelines. Data platforms will usually translate the data pipeline graph described by the model in to a set of pipelines which are created by translating the model pipeline in to IaC artifacts which are provisioned on the target infrastructure platforms.

It also leverages github to maintain a detailed log of every change and who made it. This is important for governance and compliance as well as operational reasons.

The following actors are involved in the model:

## Data Ecosystem team

This team owns the overall ecosystem model. This model contains the following metadata:

* List of data platforms available to the enterprise

* List of infrastructure vendors and locations available to the enterprise

* List of governance zones and the teams and repositories that own them

## Data Governance Zone teams

Each data governance zone has an associated Github repository. This repository contains the following metadata:

* A filtered list of data platforms allowed for data producers and consumers within this zone

* A filtered list of data classifications allowed within this zone.

* A filtered list of infrastructure vendors and locations allowed within this zone

* A set of storage policies that govern the data at rest within this zone

* A set of teams that are allowed to operate within this zone

## Teams

Each governance zone has a number of named teams. Each team is responsible for a subset of the data ecosystem's producers, consumers and transformers.

* Data Producers defined a Datastore and its associated Datasets. They also define how data can be ingested from the datastore. This ingestion metadata is used by data platforms when they need data from a specific datastore for a consumer.

* Data consumers define Workspaces which describe the subset of the ecosystem data they need to perform their function. A consumer defines this using a set of DatasetGroups. A Datasetgroup allows a consumer to specify how the data is required and what data is required for that purpose. For example, a datasetgroup can define that 4 datasets are required with very low latency. A different datasetgroup for that consumer can define the same 4 datasets are needed with fully milestoned data. The consumer would therefore have access to two different versions of the same data. One, near time with very up to date data and the other which may only have fully corrected/reconciled data once a day. The ecosystem will choose 2 different data platforms to service these datasetgroups. The consumer doesn't care. It simply expresses it's intentions and the data ecosystem does its best to satisfy those intentions. The data platforms it chooses can change over time as new platforms become available or existing platforms are decommissioned.

The project is designed to be a new type of enterprise data catalog. It uses a new evolution of a data catalog which describes the intent of data producers, data consumers, data transformers and data platforms. A broker mediates between these 4 actors. The broker is responsible for ensuring that the data is stored in a manner that is consistent with the governance policies of the enterprise. The broker provides descriptions of pipeline graphs to the various data platforms, instructing them what data they need to ingest, move and materialize in different data containers for consumers.

It's designed to allow the following actors within an enterprise to describe their intent and as a result, it's capable of automatically creating and maintaining data pipelines.

* Data producers

A data producer is a team which owns data within an enterprise and they want to share the data with the enterprise.

* Data Consumers

Data consumers are teams that want to use data exposed by data producers. The consumers create a Workspace into which they create links or DatasetSinks to the data from producers that they require. They can specify the format they want the data in and the platform will ensure the data is available in that format.

* Data Transformers

Data transformers are teams that want to use data from the enterprise to create derivative data. They can create a Workspace and then create links or DatasetSinks to the data from producers that they require. They can specify the format they want the data in and the platform will ensure the data is available in that format. They also provide code which the platform will execute within an environment to produce the derivative data. The derivative data is then stored in a new datastore which can be used by other consumers.

* Data Platforms

A Data platform 
# Ecosystem

This is meant to describe all data producers and consumers within an enterprise. It is organized into a collection of Governance Zones. The metadata describing the entire ecosystem is stored in a single git repository. This repository is the official version of the ecosystem surface. Changes to this repository are tightly controlled using GitHub handlers and pull requests.

An ecosystem is responsible for:

- A set of infrastructure vendors and locations that can be used within the ecosystem
- A set of DataPlatforms that are available to service consumers needs
- A set of approved GovernanceZones

## Governance Zones

A governance zone is responsible for:

- Filters which infrastructure vendors can be used within the zone
- Filters what types of data can be used within this zone (privacy data)
- Filters which data platforms are allowed to be used within the zone
- A collection of teams under its control
- A set of storage policies governing data produced by teams in the zone
- A set of teams which can be used within this zone

Each GovernanceZone is associated with its own GitHub repository. The ecosystem repository will only accept pull requests which change objects in a GovernanceZone from the GovernanceZones GitHub repository.

Teams must be first declared in a GovernanceZone before the Team can define them.

### Data platform filters

This allows a GZ to restrict the set of dataplatforms that can be used by Workspaces. This could be used to have a sensitive data GZ and a non sensitive data GZ. GZs in different juristictions may also limit the vendors to legally compatible vendors.

### Storage Policies

These are defined by a zone and are used to police data at rest. Before data owned by a zone can be stored in a data container, the data container must pass all policies defined by the zone owning the data. Policies can change over time and existing data containers can be revetted which could result in data being removed/moved to a compatible container.

### Teams

A team is a collection of people responsible for managing the subset of the surface represented by their producers and consumers. A team makes changes using a team-specific git repository. Changes to Team metadata are only allowed in this repository. The changes are propagated to the primary git repository using pull requests.

The Team manages a set of Data Producers and Data Consumers.

### Data Producers

These are the actors that own data. The data is stored in some kind of data container. A producer can describe how to ingest the data from that container. The platform can then use that ingestion description to get the data from the producer without the producer having to write code or manage the pipeline. The description ideally uses CDC (change data capture) but can also use SQL definitions OR FTP servers and so on. Legacy systems are important and the system can describe those also for ingestion.

A Datastore is a collection of datasets with a common ingestion description. An example would be a set of tables in a database that are logically grouped and should be ingested as a group. The datastore has:

- A set of named datasets. Each dataset has a schema. The schema includes privacy information.
- Capture metadata describing how to ingest this datastore. Where is it, how can we get it, what credential should be used to ingest it, etc.
- A set of policies that govern the data in the datastore. These policies are defined by the zone owning the data.

### Data Consumers

Data consumers consume data exposed in the ecosystem using a Workspace. A Workspace is a collection of datasets of interest to the consumer. The consumer specifies metadata about the workspace indicating how they want to consume the data. For example, a particular style of data engine (OLAP, Lakehouse with Spark containers, OLTP database, etc). The ecosystem's job is to make sure the data is available in the workspace in the format requested by the consumer. How the ecosystem does this is up to the ecosystem. The consumer just stated their requirements. The producers of that data also specify their ingestion metadata and the ecosystem can join the dots.

The consumer can then use the data in the workspace to do their job. A data consumer can also produce derivative data from the workspace. They would then be both consumers and producers of data. If they produce data then we call them a Data Refiner to distinguish them from a pure consumer.

### Data Refiners

A data refiner is a consumer that produces derivative data from the Workspace and stores it in a new Datastore in the ecosystem. This new datastore can then be used by other consumers.

You can imagine a multi-layer system in this manner. Datastore A is first ingested by the platform. Refiner A then produces Datastore B. A consumer then uses data from B. Refiner A could run on an OLAP database, the Consumer could run on a Lakehouse.

## Data Platforms

A data platform is a system which can interpret metadata from the surface and implement the data pipelines inferred from the metadata. An ecosystem will have multiple data platforms available with different characteristics. The ecosystem will choose the best platform for each Workspace given the requirements of the consumer and the capabilities of the platform. The ecosystem can change the platform over time if more suitable platforms become available.

## Data Containers

These are anywhere data can be stored at rest. The storage of data at rest is governed by the storage policies of each dataset. The data container can be a database, a file system, a cloud blob store, etc. The data container is responsible for enforcing the storage policies of the data it contains.

## How we ensure the model stays consistent

See [PULL](xref:docs/PullRequests.adoc) for more information.
