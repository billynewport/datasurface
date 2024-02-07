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
