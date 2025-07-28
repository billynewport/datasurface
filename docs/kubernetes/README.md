# Generic Kubernetes DataPlatform Design

This document describes how the DataSurface generic Kubernetes data platform works. It's intended to be used in the following environments:

* On premise or private clouds
* Public clouds on top of vendor managed K8S clusters

## Overview

This is a fast batch data pipeline implementation. It is designed to be used in a Kubernetes environment. It is based on the following components:

* Provided CI/CD repository.
* Apache AirFlow
* Debezium
* Spark on Kubernetes
* 