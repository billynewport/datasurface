# Glossary of Terms

## Data Platform

A system or service that is selected to service the data pipeline requirements of a number of DatasetGroups defined in a set of Workspaces, such as data store ingestions, exports to workspaces, triggers for data transformer computations, and job scheduling for data movement. [Read more](DataPlatform.md)

## Workspace

A specific environment for a consumer where desired data is handled and made available for the consumer to query. [Read more](Workspaces.md)

## DatasetGroup

This defines a single need for a consumer. A Workspace contains one or more of these. Each datasetgroup defines a set of requirements on how the consumer wants the data. This typically includes non functional requirements such as data latency, retention requirements, and so on. [Read more](Workspaces.md)

## DatasetSink

A DatasetGroup has one or more DatasetSinks. Each sink is a reference to a single dataset owned by a data producer that the consumer requires for these use case. A DatasetGroup can contain many DatasetSinks.

## Ecosystem

The overall model that includes metadata describing producers, consumers, data transformations, and data platforms. Changes in the ecosystem are reflected in changes to the intention graph. [Read more](Ecosystem.md)

## Intention Graph

A graph of nodes from the Ecosystem that represents the data pipelines. There is one of these graphs per data platform selected by Datasurface based on the Workspace requirements. The data platform takes this graph and generates a system to fulfill the intentions expressed in that graph.

## DataSurface

This is the broker which sits between the Ecosystem model and the DataPlatforms. It selects a specific platform to service a specific Workspace. [Read more](Ecosystem.md)

## Data Producer

An entity that generates data that is available for consumption. The data may be ingested by data platforms selected.

## Data Transformer

A process or job that transforms data in some way. This could be a computation, a data cleaning task, a data aggregation task, etc.

## Data Store Ingestion

The process of taking data from a data producer and storing it in a data platform.

## Data Export

The process of taking data from a data platform and making it available for use in a Workspace.

## Trigger

A condition or event that causes a data transformer job to be executed.
