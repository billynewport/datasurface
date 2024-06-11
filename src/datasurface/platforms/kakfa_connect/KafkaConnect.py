"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.Documentation import Documentation
from datasurface.md.Governance import CloudVendor, DataContainer, DataPlatform, DataPlatformExecutor, Ecosystem, \
    IaCDataPlatformRenderer, IaCDataPlatformRendererShim, PlatformPipelineGraph
from datasurface.md.Lint import ValidationTree


class KafkaConnectPlatform(DataPlatform):
    def __init__(self, name: str, doc: Documentation, executor: DataPlatformExecutor):
        super().__init__(name, doc, executor)

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        rc: set[CloudVendor] = set()
        rc.add(CloudVendor.PRIVATE)
        return rc

    def __hash__(self) -> int:
        return hash(self.name)

    def _str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, KafkaConnectPlatform)

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return dc.areLocationsOwnedByTheseVendors(eco, {CloudVendor.PRIVATE})

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)

    def getInternalDataContainers(self) -> set[DataContainer]:
        # TODO: Implement this method
        return set()

    def createIaCRender(self, graph: PlatformPipelineGraph) -> IaCDataPlatformRenderer:
        return IaCDataPlatformRendererShim(self.executor, graph)
