# Data surface Data Catalog

Data surface uses a unique data catalog implementation. Data catalogs are used to describe data sources and their schemas in a database. There are controls implemented around the data catalog so that provenance can be established for the artifacts stored in the catalog:

## Table of Contents

- [Data sources](#data-sources)
- [Controls for provenance](#controls-for-provenance)
- [Controlling changes of metadata in Data surface](#controlling-changes-of-metadata-in-data-surface)
- [What is an Ecosystem?](#what-is-an-ecosystem)
- [What is a Governance Zone?](#what-is-a-governance-zone)
- [What is a Team?](#what-is-a-team)
- [Datastores, datasets and capture metadata](#datastores-datasets-and-capture-metadata)
- [Consuming data, Workspaces and dataplatforms](#consuming-data-workspaces-reader-security-and-dataplatforms)
- [Defining an ecosystem](#defining-an-ecosystem)
- [Defining a Governance Zone](#defining-a-governance-zone)
- [How changes are merged in to the main repository](#how-changes-are-merged-in-to-the-main-repository)
- [How deprecation works](#how-deprecation-works)
- [VSCode extension](#vscode-extension)

## Data sources

- Datasets and their schemas
- Data surface allows data consumers to describe their inputs, engine requirements and regulatory requirements.

## Controls for provenance

Catalogs typically need to define roles, workflows and approval processes. Data surface uses GitHub for these purposes rather than needing a proprietary implementation or such as implementation that can be bridged to workflow or authorization frameworks in use by the enterprise.

## Controlling changes of metadata in Data surface

This section describes how a use can use GitHub to define the major artifacts needed in an implementation. Data surface defines top level artifacts whose changes can be managed by use of GitHub.

- Ecosystem
- Governance Zones
- Teams

## What is an Ecosystem?

An Ecosystem is intended to represent the entirety of a firm's data assets. This includes all geographical/regulatory locations, all data platforms, data center hosting providers, cloud providers and internal providers. The ecosystem is defined in a GitHub repository. The CI/CD in place around who can make changes and who reviews those changes controls the changes to the ecosystem. An Ecosystem is divided into Governance Zones.

An Ecosystem is associated with a specific repository to manage changes. The repository must contain a file called `eco.py` which defines the ecosystem. The `eco.py` module must be at the root level and must have a method called `def createEcosystem() -> Ecosystem:`. This method must return the primary Ecosystem instance. All objects within that ecosystem should also be filled out on that instance.

Each Governance zone must be authorized by the ecosystem owner before it can be defined.

### What is a Governance Zone?

Each governance zone is responsible for implementing a set of policies over internal and cloud infrastructure providers and data platforms. All data stores and moved within this zone is governed by the zone policies. Firms might have a US zone, a UK zone, a European Zone and a Chinese zone for example. Data sourced in each zone would be managed by teams local to that one. The zone can define storage policies which control how and where data is stored and where it can be moved.

A Governance zone is associated with a specific repository to manage changes.

Each team must be authorized by the governance zone owner before it can be defined.

#### What is a Team?

A team is responsible for an exclusive set of data and consumers of that data. Data is represented as Datastores and consumers are represented as Workspaces.

A team is associated with a specific repository to manage changes.

##### Datastores, datasets and capture metadata

Data is represented as Datastores which are collections of Datasets. A dataset can be thought of as a single collection of homogeneous data with a common schema describing all records in that dataset. A datastore is a collection of these datasets which are ingested together into the ecosystem. A single physical database might have several data stores defined on it. Each datastore having datasets corresponding to tables within the database. Each data store is pointed at the same database but may use different credentials for reading the data and use different schedules or approaches for ingesting the data into the ecosystem.

##### Consuming data, Workspaces, reader security and dataplatforms

- Workspace reader security [Security.md](Security.md)
- DataPlatforms [DataPlatform.md](DataPlatform.md)

## Defining an ecosystem

An ecosystem consists of a collection of governance zones. The ecosystem is defined in the main branch of a GitHub repository. The CI/CD in place around who can make changes and who reviews those changes controls the changes to the ecosystem.

## Defining a Governance Zone

The ecosystem team can allow a new governance zone to be defined. They can add a governance zone declaration to a specific ecosystem. This defines the name of the governance zone as well as the repository which can be used to control changes to all objects for that governance zone. It's important to realize that the way these changes are controlled is not at the file level but instead it's at the model level. Data surface only requires that the ecosystem must be defined in a file called `eco.py`. It does not define any other file names. Users can use any files they choose.

The `eco.py` module must be at the root level and must have a method called `def createEcosystem() -> Ecosystem:`. This method must return the primary Ecosystem instance. All objects within that ecosystem should also be filled out on that instance. The ecosystem branch checks the model for validity. It checks:

- That all objects are defined correctly.
- That all references between objects are valid.
- The whole model is consistent.

The main branch defines the steady state integrated model of the system into which changes are pushed using other repositories with authorization to change various parts of the model/ecosystem such as governance zones and teams. Other git repositories can be defined on branches or even separate repositories. Ownership of a subset of the model is assigned to these branches or repositories. When changes are pulled into the main branch, handlers make sure that the changes result in a consistent model AND that any model changes were made from a git branch or repository that is authorized to make those changes. The system doesn't care which files were modified by which repository. It only cares that the model is consistent and that the model objects were only modified by an authorized repository. Thus, a repository can refactor how the ecosystem is constructed from the `eco.py` file to a different file structure. The ecosystem branch will only check that the model is consistent and that the objects are only modified by authorized repositories.

## How changes are merged in to the main repository

An action handler associated with the main repository will perform the following checks before allowing changes to be merged into the main branch:

- The proposed model is self-consistent and passes all validation tests. These tests include tests checking that object names are correct (ANSI SQL identifiers), that all references between objects are valid and that all objects are defined correctly.
- The proposed model is backwards compatible with the current model. Producers cannot drop columns on tables, drop columns, add non-nullable columns or change column types in ways that break backwards compatibility with the current model. Data sets used in Workspaces cannot be deprecated if the Workspaces using them do not allow deprecated datasets.
- Comparing the main model with the proposed model can generate a set of changes. The handler checks that all these changes are allowed in the main model for the proposed repository. For example, if a repository is only allowed to change a specific set of tables, the handler will check that the proposed changes only affect those tables.

If the handler determines that all of these constraints are satisfied then the changes can be merged into the main repository. If not, the changes are rejected.

## How deprecation works

A producer or a workspace can create a repository with a copy of the main repository. They can edit a data store, dataset or workspace to be marked as deprecated. When this change is committed and pushed to the main repository, the main repository will check that the deprecation is allowed. If it is, the deprecation will be allowed. If not, the deprecation will be rejected. It can be rejected because there are Workspaces that have DataSinks using the newly deprecated datasets.

Each DataSink has an attribute indicating whether the Workspace owner is prepared to allow deprecated datasets to be used. Thus, initially, it's likely a producer's attempt to deprecate a Dataset will fail. The producer is informed which workspaces are causing the problem. The producer can then reach out to the workspace owners and discuss with them the situation. Once a solution is agreed then the workspace owner can mark the impacted DataSink objects in the Workspace as allowing deprecated datasets. Once this is committed and merged in the main branch, the producer can then push a change which deprecates the dataset. Marking a dataset as deprecated also protects the producer from teams adding new Workspaces which should be marked as not allowing deprecated datasets. The owners of these new Workspaces would then in a similar manner reach out to the datastore owner and ask for advice on how to proceed.

Thus, deprecation is typically a 2-step process. First, the producer attempts to push a deprecation change. This will typically be rejected. The producer then reaches out to the list of Workspace owners who are impacted by the deprecation. Once they have agreed on a solution, the workspace owners mark their DataSink objects as allowing deprecated datasets. The producer then pushes the deprecation change again. This time it will be accepted.
It's not necessary for a producer to commit and push to discover these issues. The handler validation can be executed in developer IDEs or on the command line and they will see the same errors/warnings.

Once an object that allowed deprecations is present then if the Workspace/StoragePolicy/Datastore/Dataset becomes deprecated then ALL future merges will include a warning indicating that deprecated Datasets/Workspaces or Storage Policies are being used but this will not prevent merges.

## VSCode extension

There will be a vscode extension which makes checking the model in the workspace is compatible with the current live model in the model repository. It will allow the latest branch to be recloned and then allow the current model in the workspace to be checked against this remote model. Any warnings and errors will be flagged against the DSL definitions in the workspace. Developers will use this to precheck the changes locally before pushing them to the main branch. The implementation of this extension is described in [VsCodeExtension.md](VsCodeExtension.md).
