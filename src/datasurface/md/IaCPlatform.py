from abc import ABC, abstractmethod
import os
import tempfile
from typing import Callable, Optional, Type
from datasurface.md import Documentation
from datasurface.md.Documentation import Documentable
from datasurface.md.Governance import DataContainer, DataPlatformExecutor, DatastoreCacheEntry, Ecosystem
from datasurface.md.Lint import ValidationProblem, ValidationTree
from datasurface.md.PipelineGraph import DataTransformerNode, ExportNode, IngestionMultiNode, IngestionSingleNode, PipelineNode, \
    PlatformPipelineGraph, TriggerNode


class IaCFragmentManager(ABC, Documentable):
    def __init__(self, name: str, doc: Documentation):
        Documentable.__init__(self, doc)
        ABC.__init__(self)
        self.name: str = name

    @abstractmethod
    def preRender(self):
        """This is called before the rendering of the fragments. It can be used to set up the fragment manager."""
        pass

    @abstractmethod
    def postRender(self):
        """This is called after the rendering of the fragments. It can be used to clean up the fragment manager."""
        pass

    @abstractmethod
    def addFragment(self, node: PipelineNode, fragment: str):
        """Add a fragment to the fragment manager"""
        pass


class CombineToStringFragmentManager(IaCFragmentManager):
    def __init__(self, name: str, doc: Documentation):
        super().__init__(name, doc)
        # This is a dictionary of dictionaries. The first key is the group, the second key is the name, and the value is the fragment
        self.fragments: dict[Type[PipelineNode], dict[PipelineNode, str]] = {}

    def addFragment(self, node: PipelineNode, fragment: str):
        nodeType: Type[PipelineNode] = node.__class__
        if nodeType not in self.fragments:
            self.fragments[nodeType] = {}
        self.fragments[nodeType][node] = fragment

    def __str__(self) -> str:
        rc: str = ""
        for group in self.fragments:
            rc += f"Group: {group}\n"
            for name in self.fragments[group]:
                rc += f"  {name}: {self.fragments[group][name]}\n"
        return rc


def defaultPipelineNodeFileName(node: PipelineNode) -> str:
    """Calculate simple file names for pipeline nodes. These are used with an file extension for the particular
    IaC provider, e.g. .tf for Terraform or .yaml for Kubernetes. The file name is unique for each node."""
    if (isinstance(node, IngestionMultiNode)):
        return f"{node.__class__.__name__}_{node.storeName}"
    elif (isinstance(node, IngestionSingleNode)):
        return f"{node.__class__.__name__}_{node.storeName}_{node.datasetName}"
    elif (isinstance(node, ExportNode)):
        return f"{node.__class__.__name__}_{node.storeName}_{node.datasetName}"
    elif (isinstance(node, TriggerNode)):
        return f"{node.__class__.__name__}_{node.name}_{node.workspace.name}"
    elif (isinstance(node, DataTransformerNode)):
        return f"{node.__class__.__name__}_{node.workspace.name}_{node.name}"
    raise Exception(f"Unknown node type {node}")


class FileBasedFragmentManager(IaCFragmentManager):
    """This is a file based fragment manager. It writes the fragments to a temporary directory. The fragments are stored in files with the name of the node.
    The name of the node is determined by the fnGetFileNameForNode function. This function should return a unique name for each node.
    The fragments are stored in the rootDir directory."""
    def __init__(self, name: str, doc: Documentation, fnGetFileNameForNode: Callable[[PipelineNode], str]):
        super().__init__(name, doc)
        self.rootDir: str = tempfile.mkdtemp()
        self.fnGetFileNameForNode: Callable[[PipelineNode], str] = fnGetFileNameForNode

    def addFragment(self, node: PipelineNode, fragment: str):
        name: str = self.fnGetFileNameForNode(node)
        with open(f"{self.rootDir}/{name}", "w") as file:
            file.write(fragment)

    def addStaticFile(self, folder: str, name: str, fragment: str):
        """Add a static file to the fragment manager. This is useful for adding files like
        provider.tf or variables.tf which are not associated with a particular node."""
        # Create the folder if it does not exist
        full_folder_path: str = f"{self.rootDir}/{folder}" if len(folder) > 0 else self.rootDir
        if not os.path.exists(full_folder_path):
            os.makedirs(full_folder_path)
        with open(f"{full_folder_path}/{name}", "w") as file:
            file.write(fragment)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, rootDir={self.rootDir})"

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FileBasedFragmentManager) and self.rootDir == __value.rootDir

    def __hash__(self) -> int:
        return hash(self.name)


class IaCDataPlatformRenderer(ABC):
    """This is intended to be a base class for IaC style DataPlatforms which render the intention graph
    to an IaC format. The various nodes in the graph are rendered as seperate files in a temporary folder
    which remains after the graph is rendered. The folder can then be committed to a CI/CD repository where
    it can be used by a platform like Terraform to effect the changes in the graph."""
    def __init__(self, executor: DataPlatformExecutor, graph: PlatformPipelineGraph):
        super().__init__()
        self.executor: DataPlatformExecutor = executor
        self.graph: PlatformPipelineGraph = graph

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, IaCDataPlatformRenderer) and self.executor == __value.executor and \
            self.graph == __value.graph

    def renderIaC(self, fragments: IaCFragmentManager) -> IaCFragmentManager:
        fragments.preRender()
        """This renders the IaC for the given graph"""
        for node in self.graph.nodes.values():
            if isinstance(node, IngestionSingleNode):
                fragments.addFragment(node, self.renderIngestionSingle(node))
            elif isinstance(node, IngestionMultiNode):
                fragments.addFragment(node, self.renderIngestionMulti(node))
            elif isinstance(node, ExportNode):
                fragments.addFragment(node, self.renderExport(node))
            elif isinstance(node, TriggerNode):
                fragments.addFragment(node, self.renderTrigger(node))
            elif isinstance(node, DataTransformerNode):
                fragments.addFragment(node, self.renderDataTransformer(node))
            else:
                raise Exception(f"Unknown node type {node.__class__.__name__}")
        fragments.postRender()
        return fragments

    @abstractmethod
    def renderIngestionSingle(self, ingestNode: IngestionSingleNode) -> str:
        pass

    @abstractmethod
    def renderIngestionMulti(self, ingestNode: IngestionMultiNode) -> str:
        pass

    @abstractmethod
    def renderExport(self, exportNode: ExportNode) -> str:
        pass

    @abstractmethod
    def renderTrigger(self, triggerNode: TriggerNode) -> str:
        pass

    @abstractmethod
    def renderDataTransformer(self, dtNode: DataTransformerNode) -> str:
        pass

    @abstractmethod
    def lintIngestionSingleNode(self, eco: Ecosystem, node: IngestionSingleNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintIngestionMultiNode(self, eco: Ecosystem, node: IngestionMultiNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintExportNode(self, eco: Ecosystem, node: ExportNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintTriggerNode(self, eco: Ecosystem, node: TriggerNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintDataTransformerNode(self, eco: Ecosystem, node: DataTransformerNode, tree: ValidationTree) -> None:
        pass

    def lintGraph(self, eco: Ecosystem, tree: ValidationTree):
        """Check that the platform can handle every node in the pipeline graph. Nodes may fail because there is no supported
        connector for an ingestion or export node or because there are missing parameters or because a certain type of
        trigger or data transformer is not supported. It may also fail because an infrastructure vendor or datacontainer is not supported"""
        for node in self.graph.nodes.values():
            if (isinstance(node, IngestionSingleNode)):
                self.lintIngestionSingleNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, IngestionMultiNode)):
                self.lintIngestionMultiNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, ExportNode)):
                self.lintExportNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, TriggerNode)):
                self.lintTriggerNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, DataTransformerNode)):
                self.lintDataTransformerNode(eco, node, tree.addSubTree(node))

    def getDataContainerForDatastore(self, storeName: str) -> Optional[DataContainer]:
        """Get the data container for a given datastore"""
        storeEntry: DatastoreCacheEntry = self.graph.eco.cache_getDatastoreOrThrow(storeName)
        if (storeEntry.datastore.cmd is not None):
            return storeEntry.datastore.cmd.dataContainer
        else:
            return None

    def isDataContainerSupported(self, dc: DataContainer, allowedContainers: set[Type[DataContainer]]) -> bool:
        """Check if a data container is supported"""
        return dc.__class__ in allowedContainers


class UnsupportedDataContainer(ValidationProblem):
    def __init__(self, dc: DataContainer):
        super().__init__(f"DataContainer {dc} is not supported")

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, UnsupportedDataContainer)

    def __hash__(self) -> int:
        return hash(self.description)
