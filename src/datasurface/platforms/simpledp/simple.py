"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, DataPlatformExecutor, Documentation, Ecosystem, ValidationTree, CloudVendor, DataContainer, \
    PlatformPipelineGraph, DataPlatformGraphHandler, CredentialStore, PostgresDatabase
from typing import Any, Optional
from datasurface.md import LocationKey, Credential, JSONable, KafkaServer, Datastore, KafkaIngestion


class SimplePlatformExecutor(DataPlatformExecutor):
    def __init__(self):
        super().__init__()

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass


class SimpleDataPlatformHandler(DataPlatformGraphHandler):
    """This takes the graph and then implements the data pipeline described in the graph using the technology stack
    pattern implemented by this platform. This platform supports ingesting data from Kafka confluence connectors. It
    takes the data from kafka topics and writes them to a postgres staging table. A seperate job scheduled by airflow
    then runs periodically and merges the staging data in to a MERGE table as a batch. Any workspaces can also query the
    data in the MERGE tables through Workspace specific views."""
    def __init__(self, graph: PlatformPipelineGraph) -> None:
        super().__init__(graph)

    def getInternalDataContainers(self) -> set[DataContainer]:
        """This returns any DataContainers used by the platform."""
        return set()

    def lintGraph(self, eco: Ecosystem, tree: ValidationTree):
        """This should be called execute graph. This is where the graph is validated and any issues are reported. If there are
        no issues then the graph is executed. Executed here means
        1. Terraform file which creates kafka connect connectors to ingest topics to postgres tables.
        2. Airflow DAG which has all the needed MERGE jobs. The Jobs in Airflow should be parameterized and the parameters
        would have enough information such as DataStore name, Datasets names. The Ecosystem git clone should be on the local file
        system and the path provided to the DAG. It can then get everything it needs from that.

        The MERGE job are responsible for node type reconciliation such as creating tables/views for staging, merge tables
        and Workspace views. It's also responsible to keep them up to date. This happens before the merge job is run. This seems
        like something that will be done by a DataSurface service as it's pretty standard."""

        if not isinstance(self.graph.platform, SimpleDataPlatform):
            raise ValueError("SimpleDataPlatformHandler can only be used with SimpleDataPlatform")

        # Lets make sure only kafa ingestions are used.
        for storeName in self.graph.storesToIngest:
            store: Datastore = eco.cache_getDatastoreOrThrow(storeName).datastore
            if not isinstance(store, KafkaIngestion):
                raise ValueError(f"Datastore {storeName} ingestion type is not supported by this platform")
        pass


class KafkaConnectCluster(DataContainer, JSONable):
    def __init__(self, name: str, locs: set[LocationKey], restAPIUrlString: str, kafkaServer: KafkaServer, caCert: Optional[Credential] = None) -> None:
        super().__init__(name, locs)
        self.restAPIUrlString: str = restAPIUrlString
        self.kafkaServer: KafkaServer = kafkaServer
        self.caCertificate: Optional[Credential] = caCert

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "restAPIUrlString": self.restAPIUrlString,
                "kafkaServer": self.kafkaServer.to_json()
            }
        )
        if (self.caCertificate):
            rc.update(
                {
                    "caCertificate": self.caCertificate.to_json()
                }
            )
        return rc


class SimpleDataPlatform(DataPlatform):
    """This defines the simple data platform. It can consume data from sources and write them to a postgres based merge store.
      It has the use of a postgres database for staging and merge tables as well as Workspace views"""
    def __init__(
            self,
            name: str,
            doc: Documentation,
            credentialStore: CredentialStore,
            kafkaConnectCluster: KafkaConnectCluster,
            connectCredentials: Credential,
            mergeStore: PostgresDatabase,
            postgresCredentials: Credential
            ):
        super().__init__(name, doc, SimplePlatformExecutor(), credentialStore)
        self.credStoreName: str = credentialStore.name
        self.kafkaConnectCluster: KafkaConnectCluster = kafkaConnectCluster
        self.connectCredentials: Credential = connectCredentials
        self.mergeStore: PostgresDatabase = mergeStore
        self.postgresCredentials: Credential = postgresCredentials

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "credStoreName": self.credStoreName,
                "mergeStore": self.mergeStore.to_json(),
                "connectCredentials": self.connectCredentials.to_json(),
                "postgresCredentials": self.postgresCredentials.to_json(),
                "kafkaConnectCluster": self.kafkaConnectCluster.to_json()
            }
        )
        return rc

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return False

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        """This should validate the platform and its associated parts but it cannot validate the usage of the DataPlatform
        as the graph must be generated for that to happen. The lintGraph method on the SimpleDataPlatformHandler does
        that as well as generating the terraform, airflow and other artifacts."""
        super().lint(eco, tree)
        self.kafkaConnectCluster.lint(eco, tree)
        self.mergeStore.lint(eco, tree)
        self.postgresCredentials.lint(eco, tree)
        self.connectCredentials.lint(eco, tree)

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        return SimpleDataPlatformHandler(graph)
