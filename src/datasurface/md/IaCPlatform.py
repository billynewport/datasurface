from abc import ABC, abstractmethod
from typing import Type
from datasurface.md import Documentation
from datasurface.md.Documentation import Documentable
from datasurface.md.Governance import DataPlatform, Ecosystem
from datasurface.md.Lint import ValidationTree
from datasurface.md.PipelineGraph import DataTransformerNode, ExportNode, IngestionMultiNode, IngestionSingleNode, PipelineNode, \
    PlatformPipelineGraph, TriggerNode


class IaCFragmentManager(ABC, Documentable):
    def __init__(self, name: str, doc: Documentation):
        Documentable.__init__(self, doc)
        ABC.__init__(self)
        self.name: str = name

    @abstractmethod
    def addFragment(self, nodeType: Type[PipelineNode], name: str, fragment: str):
        pass


class CombineToStringFragmentManager(IaCFragmentManager):
    def __init__(self, name: str, doc: Documentation):
        super().__init__(name, doc)
        # This is a dictionary of dictionaries. The first key is the group, the second key is the name, and the value is the fragment
        self.fragments: dict[Type[PipelineNode], dict[str, str]] = {}

    def addFragment(self, nodeType: Type[PipelineNode], name: str, fragment: str):
        if nodeType not in self.fragments:
            self.fragments[nodeType] = {}
        self.fragments[nodeType][name] = fragment

    def __str__(self) -> str:
        rc: str = ""
        for group in self.fragments:
            rc += f"Group: {group}\n"
            for name in self.fragments[group]:
                rc += f"  {name}: {self.fragments[group][name]}\n"
        return rc


class IaCDataPlatform(DataPlatform):
    """This is intended to be a base class for IaC style DataPlatforms"""
    def __init__(self, name: str, doc: Documentation):
        super().__init__(name, doc)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, IaCDataPlatform)

    def renderIaC(self, fragments: IaCFragmentManager, graph: PlatformPipelineGraph) -> IaCFragmentManager:
        """This renders the IaC for the given graph"""
        for node in graph.nodes:
            if isinstance(node, IngestionSingleNode):
                fragments.addFragment(node.__class__, str(node), self.renderIngestionSingle(node))
            elif isinstance(node, IngestionMultiNode):
                fragments.addFragment(node.__class__, str(node), self.renderIngestionMulti(node))
            elif isinstance(node, ExportNode):
                fragments.addFragment(node.__class__, str(node), self.renderExport(node))
            elif isinstance(node, TriggerNode):
                fragments.addFragment(node.__class__, str(node), self.renderTrigger(node))
            elif isinstance(node, DataTransformerNode):
                fragments.addFragment(node.__class__, str(node), self.renderDataTransformer(node))
            else:
                raise Exception(f"Unknown node type {node}")
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

    def lintGraph(self, eco: Ecosystem, graph: PlatformPipelineGraph, tree: ValidationTree):
        """Check that the platform can handle every node in the pipeline graph. Nodes may fail because there is no supported
        connector for an ingestion or export node or because there are missing parameters or because a certain type of
        trigger or data transformer is not supported. It may also fail because an infrastructure vendor or datacontainer is not supported"""
        for node in graph.nodes.values():
            if (isinstance(node, IngestionSingleNode)):
                self.lintIngestionSingleNode(eco, node, tree.createChild(node))
            elif (isinstance(node, IngestionMultiNode)):
                self.lintIngestionMultiNode(eco, node, tree.createChild(node))
            elif (isinstance(node, ExportNode)):
                self.lintExportNode(eco, node, tree.createChild(node))
            elif (isinstance(node, TriggerNode)):
                self.lintTriggerNode(eco, node, tree.createChild(node))
            elif (isinstance(node, DataTransformerNode)):
                self.lintDataTransformerNode(eco, node, tree.createChild(node))
