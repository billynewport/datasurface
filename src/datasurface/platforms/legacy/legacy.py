"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, Ecosystem, \
    DataPlatformChooser, DataContainer, Documentation, DataPlatformExecutor, ValidationTree, \
    CloudVendor, PlatformPipelineGraph, DataPlatformGraphHandler, AttributeNotSet, ObjectWrongType, ProblemSeverity

from datasurface.md import PlainTextDocumentation

from typing import Optional


class LegacyDataPlatformExecutor(DataPlatformExecutor):
    """This is a no-op DataPlatformExecutor. It's intent is to specify that the data flows are already realized and externally managed"""

    def __init__(self) -> None:
        super().__init__()

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass

    def __str__(self) -> str:
        return "LegacyDataPlatformExecutor()"


class LegacyDataPlatformHandler(DataPlatformGraphHandler):
    """This receives the pipeline graph for DSGs assigned to a legacy data platform. This class is important because
    it can extra parameters from the DSG. Each Workspace in the DSG needs to have a legacyplatformchooser so that they
    can specify the datacontainers used by the legacy data platform. This allows the governance code to run against
    these pipelines"""

    def __init__(self, graph: PlatformPipelineGraph) -> None:
        super().__init__(graph)
        self.internalContainers: set[DataContainer] = set()

    def calculateInternalContainers(self) -> None:
        self.internalContainers.clear()
        for dsgSet in self.graph.dataContainerConsumers.values():
            for (ws, dsg) in dsgSet:
                if dsg.platformMD is not None:
                    if isinstance(dsg.platformMD, LegacyDatPlatformChooser):
                        self.internalContainers.update(dsg.platformMD.containers)

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        # Lets check every DSG in the graph uses a LegacyDataPlatformChooser
        # and gather a list of all DataContainers used by the DSGs.

        # There is no need to also add in producer datacontainers and they are already exposed in the
        # graph.
        for dsgSet in self.graph.dataContainerConsumers.values():
            for (ws, dsg) in dsgSet:
                if dsg.platformMD is None:
                    tree.addRaw(AttributeNotSet(f"DSG {ws.name, dsg.name} needs a LegacyDataPlatformChooser"))
                else:
                    if not isinstance(dsg.platformMD, LegacyDatPlatformChooser):
                        tree.addRaw(ObjectWrongType(dsg.platformMD, LegacyDataPlatform, ProblemSeverity.ERROR))

    def __str__(self) -> str:
        return "LegacyDataPlatformHandler()"

    def getInternalDataContainers(self) -> set[DataContainer]:
        if len(self.internalContainers) == 0:
            self.calculateInternalContainers()
        return self.internalContainers

    def lintGraph(self, eco: Ecosystem, tree: ValidationTree):
        """This is preexisting infra, it all exists already so it's by definition compatible with this platform"""
        pass


class LegacyDataPlatform(DataPlatform):
    """This is a no-op DataPlatform. It's intent is to specify that the data flows are already realized and externally managed
    by existing systems. However, DataSurface will still track the data flows and manage governance for the data."""
    def __init__(self, name: str, doc: Documentation) -> None:
        super().__init__(name, doc, LegacyDataPlatformExecutor())

    def __str__(self) -> str:
        return f"LegacyDataPlatform({self.name})"

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return False

    def lint(self, eco: Ecosystem, tree: ValidationTree) -> None:
        pass

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        return LegacyDataPlatformHandler(graph)


class LegacyDatPlatformChooser(DataPlatformChooser):
    """This chooses the legacy DataPlatform and allows the specifics of the particular
    data pipeline to be specified such as DataContainers used"""
    def __init__(self, name: str, doc: Documentation, containers: set[DataContainer]) -> None:
        super().__init__()
        self.name: str = name
        self.containers: set[DataContainer] = containers
        self.doc: Documentation = doc

    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        return LegacyDataPlatform(self.name, PlainTextDocumentation("Legacy Data Platform"))

    def __str__(self) -> str:
        return "LegacyDatPlatformChooser()"
