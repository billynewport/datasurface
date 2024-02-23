
from typing import Optional, Type, cast
from datasurface.md.Governance import DataContainer, DataPlatform, DataTransformerOutput, DatasetGroup, DatasetSink, Datastore
from datasurface.md.Governance import Ecosystem, IngestionConsistencyType, StepTrigger, Workspace


class PipelineNode:
    """This is a named node in the pipeline graph. It stores node common information and which nodes this node depends on and those that depend on this node"""
    def __init__(self, name: str, platform: DataPlatform):
        self.name: str = name
        self.platform: DataPlatform = platform
        # This node depends on this set of nodes
        self.leftHandNodes: dict[str, PipelineNode] = dict()
        # This set of nodes depend on this node
        self.rightHandNodes: dict[str, PipelineNode] = dict()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}/{self.name}"

    def __eq__(self, o: object) -> bool:
        return isinstance(o, PipelineNode) and self.name == o.name and self.leftHandNodes == o.leftHandNodes and self.rightHandNodes == o.rightHandNodes

    def addRightHandNode(self, rhNode: 'PipelineNode'):
        """This records a node that depends on this node"""
        self.rightHandNodes[str(rhNode)] = rhNode
        rhNode.leftHandNodes[str(self)] = self


class ExportNode(PipelineNode):
    def __init__(self, platform: DataPlatform, dataContainer: DataContainer, storeName: str, datasetName: str):
        super().__init__(f"Export/{platform.name}/{dataContainer.name}/{storeName}/{datasetName}", platform)
        self.dataContainer: DataContainer = dataContainer
        self.storeName: str = storeName
        self.datasetName: str = datasetName

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ExportNode) and self.dataContainer == o.dataContainer and \
            self.storeName == o.storeName and self.datasetName == o.datasetName


class IngestionNode(PipelineNode):
    def __init__(self, name: str, platform: DataPlatform, storeName: str, captureTrigger: Optional[StepTrigger]):
        super().__init__(name, platform)
        self.storeName: str = storeName
        self.captureTrigger: Optional[StepTrigger] = captureTrigger

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, IngestionNode) and self.storeName == o.storeName and self.captureTrigger == o.captureTrigger


class IngestionMultiNode(IngestionNode):
    def __init__(self, platform: DataPlatform, storeName: str, captureTrigger: Optional[StepTrigger]):
        super().__init__(f"Ingest/{platform.name}/{storeName}", platform, storeName, captureTrigger)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, IngestionMultiNode)


class IngestionSingleNode(IngestionNode):
    def __init__(self, platform: DataPlatform, storeName: str, dataset: str, captureTrigger: Optional[StepTrigger]):
        super().__init__(f"Ingest/{platform.name}/{storeName}/{dataset}", platform, storeName, captureTrigger)
        self.dataset: str = dataset

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, IngestionSingleNode) and self.dataset == o.dataset


class TriggerNode(PipelineNode):
    def __init__(self, w: Workspace, platform: DataPlatform):
        super().__init__(f"Trigger{w.name}", platform)
        self.workspace: Workspace = w

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, TriggerNode) and self.workspace == o.workspace


class DataTransformerNode(PipelineNode):
    def __init__(self, ws: Workspace, platform: DataPlatform):
        super().__init__(f"DataTransfomer/{ws.name}", platform)
        self.workspace: Workspace = ws

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, DataTransformerNode) and self.workspace == o.workspace


class DSGRootNode:
    def __init__(self, w: Workspace, dsg: DatasetGroup):
        self.workspace: Workspace = w
        self.dsg: DatasetGroup = dsg

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, o: object) -> bool:
        return isinstance(o, DSGRootNode) and self.workspace == o.workspace and self.dsg == o.dsg

    def __str__(self) -> str:
        return f"{self.workspace.name}/{self.dsg.name}"


class PlatformPipelineGraph:
    """This should be all the information a DataPlatform needs to render the processing pipeline graph. This would include
    provisioning Workspace views, provisioning dataContainer tables. Exporting data to dataContainer tables. Ingesting data from datastores,
    executing data transformers"""
    def __init__(self, eco: Ecosystem, platform: DataPlatform):
        self.platform: DataPlatform = platform
        self.eco: Ecosystem = eco
        self.workspaces: dict[str, Workspace] = dict()
        # All DSGs per Platform
        self.roots: set[DSGRootNode] = set()
        # All Datasets to be exported per asset per platform
        # Views need to be aliased on top providing the Workspace/DSG named object for consumers to query
        self.dataContainerExports: dict[DataContainer, set[DatasetSink]] = dict()

        # These are all the datastores used in the pipelinegraph for this platform. Note, this may be
        # a subset of the datastores in total in the ecosystem
        self.storesToIngest: set[str] = set()

        # This is the set of ALL nodes in this platforms pipeline graph
        self.nodes: dict[str, PipelineNode] = dict()

    def generateGraph(self):
        """This generates the pipeline graph for the platform. This is a directed graph with nodes representing
        ingestion, export, trigger, and data transformation operations"""
        self.dataContainerExports = dict()

        # Split DSGs by Asset hosting Workspaces
        for dsg in self.roots:
            if dsg.workspace.dataContainer:
                dataContainer: DataContainer = dsg.workspace.dataContainer
                if self.dataContainerExports.get(dataContainer) is None:
                    self.dataContainerExports[dataContainer] = set()
                sinks: set[DatasetSink] = self.dataContainerExports[dataContainer]
                for sink in dsg.dsg.sinks.values():
                    sinks.add(sink)
        # Now collect stores to ingest per platform
        self.storesToIngest = set()
        for sinkset in self.dataContainerExports.values():
            for sink in sinkset:
                self.storesToIngest.add(sink.storeName)

        # Make ingestion steps for every store used by platform
        for store in self.storesToIngest:
            self.createIngestionStep(store)

        # Now build pipeline graph backwards from workspaces used by platform and stores used by platform
        for dataContainer in self.dataContainerExports.keys():
            exports = self.dataContainerExports[dataContainer]
            for export in exports:
                exportStep: PipelineNode = ExportNode(self.platform, dataContainer, export.storeName, export.datasetName)
                # If export doesn't already exist then create and add to ingestion job
                if (self.nodes.get(str(exportStep)) is None):
                    self.nodes[str(exportStep)] = exportStep
                    self.addExportToPriorIngestion(exportStep)

    def findExistingOrCreateStep(self, step: PipelineNode) -> PipelineNode:
        """This finds an existing step or adds it to the set of steps in the graph"""
        if self.nodes.get(str(step)) is None:
            self.nodes[str(step)] = step
        else:
            step = self.nodes[str(step)]
        return step

    def createIngestionStep(self, storeName: str):
        """This creates a step to ingest data for a datastore. This results in either a single step for a multi-dataset store
        or one step per dataset in the single dataset stores"""
        store: Datastore = self.eco.cache_getDatastoreOrThrow(storeName).datastore

        if store.cmd:
            if (store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET):
                for datasetName in store.datasets.keys():
                    self.findExistingOrCreateStep(IngestionSingleNode(self.platform, storeName, datasetName, store.cmd.stepTrigger))
            else:  # MULTI_DATASET
                self.findExistingOrCreateStep(IngestionMultiNode(self.platform, storeName, store.cmd.stepTrigger))
        else:
            raise Exception(f"Store {storeName} cmd is None")

    def addExportToPriorIngestion(self, exportStep: ExportNode):
        """This makes sure the ingestion steps for a the datasets in an export step exist"""
        assert (self.nodes.get(str(exportStep)) is not None)
        """Work backwards from export step. The normal chain is INGEST -> EXPORT. In the case of exporting a store from
        a transformer then it is INGEST -> EXPORT -> TRIGGER -> TRANSFORM -> INGEST -> EXPORT"""
        store: Datastore = self.eco.cache_getDatastoreOrThrow(exportStep.storeName).datastore
        if (store.cmd):
            ingestionStep: Optional[PipelineNode] = None
            # Create a step for a single or multi dataset ingestion
            if (store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET):
                ingestionStep = IngestionSingleNode(exportStep.platform, exportStep.storeName, exportStep.datasetName, store.cmd.stepTrigger)
            else:  # MULTI_DATASET
                ingestionStep = IngestionMultiNode(exportStep.platform, exportStep.storeName, store.cmd.stepTrigger)
            ingestionStep = self.findExistingOrCreateStep(ingestionStep)

            ingestionStep.addRightHandNode(exportStep)
            # If this store is a transformer then we need to create the transformer job
            if isinstance(store.cmd, DataTransformerOutput):
                dt: DataTransformerOutput = store.cmd
                w: Workspace = self.eco.cache_getWorkspaceOrThrow(dt.workSpaceName).workspace
                if w.dataContainer:
                    # Find/Create Trigger, this is a join on all incoming exports needed for the transformer
                    dtStep: DataTransformerNode = cast(DataTransformerNode, self.findExistingOrCreateStep(DataTransformerNode(w, self.platform)))
                    triggerStep: TriggerNode = cast(TriggerNode, self.findExistingOrCreateStep(TriggerNode(w, self.platform)))
                    # Add ingestion for transfomer
                    dtIngestStep: PipelineNode = self.findExistingOrCreateStep(IngestionMultiNode(self.platform, exportStep.storeName, None))
                    dtStep.addRightHandNode(dtIngestStep)
                    # Ingesting Transformer causes Export
                    dtIngestStep.addRightHandNode(exportStep)
                    # Trigger calls Transformer Step
                    triggerStep.addRightHandNode(dtStep)
                    # Add Exports to call trigger
                    for dsgR in self.roots:
                        if dsgR.workspace == w:
                            for sink in dsgR.dsg.sinks.values():
                                dsrExportStep: ExportNode = cast(ExportNode, self.findExistingOrCreateStep(
                                        ExportNode(self.platform, w.dataContainer, sink.storeName, sink.datasetName)))
                                self.addExportToPriorIngestion(dsrExportStep)
                                # Add Trigger for DT after export
                                dsrExportStep.addRightHandNode(triggerStep)

    def getLeftSideOfGraph(self) -> set[PipelineNode]:
        """This returns ingestions which don't depend on anything else, the left end of a pipeline"""
        rc: set[PipelineNode] = set()
        for step in self.nodes.values():
            if len(step.leftHandNodes) == 0:
                rc.add(step)
        return rc

    def getRightSideOfGraph(self) -> set[PipelineNode]:
        """This returns steps which does have other steps depending on them, the right end of a pipeline"""
        rc: set[PipelineNode] = set()
        for step in self.nodes.values():
            if len(step.rightHandNodes) == 0:
                rc.add(step)
        return rc

    def checkNextStepsForStepType(self, filterStep: Type[PipelineNode], targetStep: Type[PipelineNode]) -> bool:
        """This finds steps of a certain type and then checks that ALL follow on steps from it are a certain type"""
        for s in self.nodes:
            if isinstance(s, filterStep):
                for nextS in s.rightHandNodes:
                    if not isinstance(nextS, targetStep):
                        return False
        return True

    def graphToText(self) -> str:
        """This returns a string representation of the pipeline graph from left to right"""
        left_side = self.getLeftSideOfGraph()

        # Create a dictionary to keep track of the nodes we've visited
        visited = {node: False for node in self.nodes.values()}

        def dfs(node: PipelineNode, indent: str = '') -> str:
            """Depth-first search to traverse the graph and build the string representation"""
            if visited[node]:
                return str(node)
            visited[node] = True
            result = indent + str(node) + '\n'
            if node.rightHandNodes:
                result += ' -> ('
                result += ', '.join(dfs(n, indent + '  ') for n in node.rightHandNodes.values() if not visited[n])
                result += ')'
            return result

        # Start the traversal from each node on the left side
        graph_strs = [dfs(node) for node in left_side]

        return '\n'.join(graph_strs)


class EcosystemPipelineGraph:
    """This is the total graph for an Ecosystem. It's a list of graphs keyed by DataPlatforms in use. One graph per DataPlatform"""
    def __init__(self, eco: Ecosystem):
        self.eco: Ecosystem = eco

        # Store for each DP, the set of DSGRootNodes
        self.roots: dict[DataPlatform, PlatformPipelineGraph] = dict()

        # Scan workspaces/dsg pairs, split by DataPlatform
        for w in eco.workSpaceCache.values():
            for dsg in w.workspace.dsgs.values():
                if dsg.platformMD:
                    p: Optional[DataPlatform] = dsg.platformMD.choooseDataPlatform(self.eco)
                    if p:
                        root: DSGRootNode = DSGRootNode(w.workspace, dsg)
                        if self.roots.get(p) is None:
                            self.roots[p] = PlatformPipelineGraph(eco, p)
                        self.roots[p].roots.add(root)
                        # Collect Workspaces using the platform
                        if (self.roots[p].workspaces.get(w.workspace.name) is None):
                            self.roots[p].workspaces[w.workspace.name] = w.workspace

        # Now track DSGs per dataContainer
        # For each platform what DSGs need to be exported to a given dataContainer
        for platform in self.roots.keys():
            pinfo = self.roots[platform]
            pinfo.generateGraph()
