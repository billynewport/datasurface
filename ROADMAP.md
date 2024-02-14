# Data surface ROAD MAP

## Phase 1: GitHub model working

This is the basic model, the github action handlers, model validations, model authorization checks, backwards compatibility. This is currently 90% there as of mid Feb 2024.

## Phase: Batch Microsoft Azure Data Platform

This will allow an ecosystem to be rendered on top of Microsoft Azure. It will allow data stored in Azure data containers to be fed to Azure hosted data containers for consumers to use. It will automatically provision tables, entitlements and feed the data consumers need. It will be batch based. Expect latencies measured in the single digit minutes. This will support for Azure SQL Server instances, Azure managed SQL server instances and Lakehouse.

## Phase: Streaming Microsoft Azure Data Platform

This will allow an ecosystem to be rendered top of Microsoft Azure. Streaming and batch consumers will be supported at this time. Consumers can have data delivered with low latency when required and in batch mode when not. Apache Beam will likely feature in this for implementing a streaming pipeline including transformations.

## Phase: Azure SnowFlake support as a data container

## Phase: Amazon AWS support

## Phase: Milestoned data support

