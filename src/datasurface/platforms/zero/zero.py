"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, DataPlatformExecutor, Documentation, Ecosystem, ValidationTree, CloudVendor, DataContainer, \
    ValidationProblem, ProblemSeverity, PlatformPipelineGraph, DataPlatformGraphHandler, Credential, CredentialStore, PostgresDatabase, KafkaServer


class ZeroPlatformExecutor(DataPlatformExecutor):
    def __init__(self):
        super().__init__()

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass


class ZeroDataPlatformHandler(DataPlatformGraphHandler):
    def __init__(self, graph: PlatformPipelineGraph) -> None:
        super().__init__(graph)

    def getInternalDataContainers(self) -> set[DataContainer]:
        return set()

    def lintGraph(self, eco: Ecosystem, tree: ValidationTree):
        pass


class ZeroDataPlatform(DataPlatform):
    """This defines the zero data platform. It can consume data from sources and write them to a postgres based merge store. It has the use of a kafka connect server as well as the postgres"""
    def __init__(
            self,
            name: str,
            doc: Documentation,
            credentialStore: CredentialStore,
            kafkaServer: KafkaServer,
            mergeStore: PostgresDatabase):
        super().__init__(name, doc, ZeroPlatformExecutor(), credentialStore)
        self.credStoreName: str = credentialStore.name
        self.kafkaServer: KafkaServer = kafkaServer
        self.mergeStore: PostgresDatabase = mergeStore

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        pass

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        return ZeroDataPlatformHandler(graph)
