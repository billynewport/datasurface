"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Documentation
from datasurface.md import CloudVendor, DataContainer, DataPlatform, DataPlatformExecutor, Ecosystem, IaCDataPlatformRenderer, \
    IaCDataPlatformRendererShim, PlatformPipelineGraph
from datasurface.md import ValidationTree


class K8SGenericDataPlatform(DataPlatform):
    """
    This is a generic Kubernetes data platform. It provides the basic kubernetes data platform functionality. It is used
    by the implementations for the cloud vendors and private clouds.
    """
    def __init__(self, name: str, doc: Documentation, executor: DataPlatformExecutor, supportedVendors: set[CloudVendor]):
        super().__init__(name, doc, executor)
        self.supportedVendors = supportedVendors

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return self.supportedVendors

    def _str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, K8SGenericDataPlatform)

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return dc.areLocationsOwnedByTheseVendors(eco, self.supportedVendors)

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)

    def getInternalDataContainers(self) -> set[DataContainer]:
        # TODO: Implement this method
        return set()

    def createIaCRender(self, graph: PlatformPipelineGraph) -> IaCDataPlatformRenderer:
        return IaCDataPlatformRendererShim(self.executor, graph)


class K8SPrivateDataPlatform(K8SGenericDataPlatform):
    def __init__(self, doc: Documentation, executor: DataPlatformExecutor):
        super().__init__("K8SPrivate", doc, executor, {CloudVendor.PRIVATE})

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, K8SPrivateDataPlatform)

    def getInternalDataContainers(self) -> set[DataContainer]:
        # TODO: Implement this method
        return set()
