"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, DataPlatformExecutor, Documentation, Ecosystem, ValidationTree, CloudVendor, DataContainer, \
    PlatformPipelineGraph, DataPlatformGraphHandler, CredentialStore, PostgresDatabase
from typing import Any


class SimplePlatformExecutor(DataPlatformExecutor):
    def __init__(self):
        super().__init__()

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass


class SimpleDataPlatformHandler(DataPlatformGraphHandler):
    def __init__(self, graph: PlatformPipelineGraph) -> None:
        super().__init__(graph)

    def getInternalDataContainers(self) -> set[DataContainer]:
        return set()

    def lintGraph(self, eco: Ecosystem, tree: ValidationTree):
        pass


class SimpleDataPlatform(DataPlatform):
    """This defines the simple data platform. It can consume data from sources and write them to a postgres based merge store.
      It has the use of a postgres database for staging and merge tables as well as Workspace views"""
    def __init__(
            self,
            name: str,
            doc: Documentation,
            credentialStore: CredentialStore,
            mergeStore: PostgresDatabase):
        super().__init__(name, doc, SimplePlatformExecutor(), credentialStore)
        self.credStoreName: str = credentialStore.name
        self.mergeStore: PostgresDatabase = mergeStore

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "credStoreName": self.credStoreName,
                "mergeStore": self.mergeStore.to_json()
            }
        )
        return rc

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return False

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        return SimpleDataPlatformHandler(graph)
