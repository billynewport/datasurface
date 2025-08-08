"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, Ecosystem, Workspace, \
    DataPlatformChooser, DataContainer, Documentation, DataPlatformExecutor, ValidationTree, \
    CloudVendor, PlatformPipelineGraph, DataPlatformGraphHandler, AttributeNotSet, ObjectWrongType, ProblemSeverity, \
    CodeExecutionEnvironment, CodeArtifact, LocationKey, CredentialStore, SchemaProjector, PlatformServicesProvider
from datasurface.md.credential import NoopCredentialStore

from typing import Optional, Any


class LegacyDataPlatformExecutor(DataPlatformExecutor):
    """This is a no-op DataPlatformExecutor. It's intent is to specify that the data flows are already realized and externally managed"""

    def __init__(self) -> None:
        super().__init__()

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
        }

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
        """This iterates through all the DSGs mapped to this platform and gathers all the DataContainers used by them"""
        self.internalContainers.clear()
        for dsgSet in self.graph.dataContainerConsumers.values():
            for (_, dsg) in dsgSet:  # ws is not used
                if dsg.platformMD is not None:
                    if isinstance(dsg.platformMD, LegacyDataPlatformChooser):
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
                    if not isinstance(dsg.platformMD, LegacyDataPlatformChooser):
                        tree.addRaw(ObjectWrongType(dsg.platformMD, LegacyDataPlatform, ProblemSeverity.ERROR))

    def __str__(self) -> str:
        return "LegacyDataPlatformHandler()"

    def getInternalDataContainers(self) -> set[DataContainer]:
        if len(self.internalContainers) == 0:
            self.calculateInternalContainers()
        return self.internalContainers

    def lintGraph(self, eco: Ecosystem, credStore: 'CredentialStore', tree: ValidationTree):
        """This is preexisting infra, it all exists already so it's by definition compatible with this platform"""
        pass

    def renderGraph(self, credStore: 'CredentialStore', issueTree: ValidationTree) -> dict[str, str]:
        """This is preexisting infra, it all exists already so it's by definition compatible with this platform"""
        return {}


class LegacyPlatformServiceProvider(PlatformServicesProvider):
    """This is a no-op PlatformServiceProvider. It's intent is to specify that the data flows are already realized and externally managed
    by existing systems. However, DataSurface will still track the data flows and manage governance for the data."""
    def __init__(self, name: str, locs: set[LocationKey], dataPlatforms: list[DataPlatform]) -> None:
        super().__init__(name, locs, NoopCredentialStore(), dataPlatforms)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        pass

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def generateBootstrapArtifacts(self, eco: 'Ecosystem', ringLevel: int) -> dict[str, str]:
        raise NotImplementedError("generateBootstrapArtifacts not implemented")

    def mergeHandler(self, eco: 'Ecosystem'):
        pass

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LegacyPlatformServiceProvider):
            return super().__eq__(other)
        else:
            return False


class LegacyDataPlatform(DataPlatform[LegacyPlatformServiceProvider]):
    """This is a no-op DataPlatform. It's intent is to specify that the data flows are already realized and externally managed
    by existing systems. However, DataSurface will still track the data flows and manage governance for the data."""
    def __init__(self, name: str, doc: Documentation) -> None:
        super().__init__(name, doc, LegacyDataPlatformExecutor())

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LegacyDataPlatform):
            return super().__eq__(other)
        else:
            return False

    def __str__(self) -> str:
        return f"LegacyDataPlatform({self.name})"

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def getCredentialStore(self) -> CredentialStore:
        return NoopCredentialStore()

    def isWorkspaceDataContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        # For a Legacy platform, we assume if its configured then its supported. This data platform
        # doesn't implement anything, it just exposes existing data flows to the governance code.
        return True

    def lint(self, eco: Ecosystem, tree: ValidationTree) -> None:
        pass

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        return LegacyDataPlatformHandler(graph)

    def generateBootstrapArtifacts(self, eco: Ecosystem, ringLevel: int) -> dict[str, str]:
        """This generates the bootstrap artifacts for all the data platforms in the ecosystem. It will create a folder for each data platform, call the
        platform and then create a file named after the key and write the value to the file."""
        return {}

    def createSchemaProjector(self, eco: Ecosystem) -> SchemaProjector:
        """This returns a schema projector which can be used to project the dataset schema to a schema compatible with the container"""
        raise NotImplementedError("createSchemaProjector not implemented")

    def lintWorkspace(self, eco: Ecosystem, tree: ValidationTree, ws: 'Workspace', dsgName: str):
        # Allow all basically.
        pass

    def resetBatchState(self, eco: Ecosystem, storeName: str, datasetName: Optional[str] = None) -> str:
        """This resets the batch state for a datastore"""
        raise NotImplementedError("resetBatchState not implemented")

    def setPSP(self, psp: LegacyPlatformServiceProvider) -> None:
        super().setPSP(psp)


class LegacyDataPlatformChooser(DataPlatformChooser):
    """This chooses the legacy DataPlatform and allows the specifics of the particular
    data pipeline to be specified such as DataContainers used"""
    def __init__(self, dataPlatformName: str, doc: Documentation, containers: set[DataContainer]) -> None:
        super().__init__()
        self.dataPlatformName: str = dataPlatformName
        """This should match a LegacyDataPlatform defined in the Ecosystem"""
        self.containers: set[DataContainer] = containers
        self.doc: Documentation = doc

    def chooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        return eco.getDataPlatformOrThrow(self.dataPlatformName)

    def __str__(self) -> str:
        return "LegacyDatPlatformChooser()"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"dataPlatformName": self.dataPlatformName, "containers": [dc.to_json() for dc in self.containers]})
        return rc

    def __hash__(self) -> int:
        return hash((self.dataPlatformName, frozenset(self.containers)))


def gatherDataContainerLocationsIntoSet(containers: set[DataContainer]) -> set[LocationKey]:
    """This is a helper function to gather all the locations from a set of DataContainers"""
    locations: set[LocationKey] = set()
    for dc in containers:
        locations.update(dc.locations)
    return locations


class LegacyDataTransformer(CodeExecutionEnvironment):
    """This is an existing DataTransformer which is unmanaged by DataSurface."""
    def __init__(self, name: str, doc: Documentation, containers: set[DataContainer]) -> None:
        super().__init__(name, gatherDataContainerLocationsIntoSet(containers))
        self.name: str = name
        """The name of the DataTransformer"""
        self.doc: Documentation = doc
        """The documentation for the DataTransformer"""

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "name": self.name,
            }
        )
        if self.doc:
            rc.update(
                {
                    "doc": self.doc.to_json()
                }
            )
        return rc

    def isCodeArtifactSupported(self, eco: Ecosystem, ca: CodeArtifact) -> bool:
        """This is a no-op DataTransformer, it's always supported"""
        return True

    def lint(self, eco: Ecosystem, tree: ValidationTree) -> None:
        pass
