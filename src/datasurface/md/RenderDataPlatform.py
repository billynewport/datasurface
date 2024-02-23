from abc import abstractmethod
from datasurface.md import Documentation
from datasurface.md.Governance import DataPlatform
from datasurface.md.PipelineGraph import DataTransformerNode, ExportNode, IngestionMultiNode, IngestionSingleNode, PlatformPipelineGraph, TriggerNode


class IaCDataPlatform(DataPlatform):
    """This is intended to be a base class for IaC style DataPlatforms"""
    def __init__(self, name: str, doc: Documentation):
        super().__init__(name, doc)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, IaCDataPlatform)

    def renderIaC(self, graph: PlatformPipelineGraph) -> str:
        """This renders the IaC for the given graph"""
        rc: str = ""
        for node in graph.nodes:
            if isinstance(node, IngestionSingleNode):
                rc += self.renderIngestionSingle(node)
            elif isinstance(node, IngestionMultiNode):
                rc += self.renderIngestionMulti(node)
            elif isinstance(node, ExportNode):
                rc += self.renderExport(node)
            elif isinstance(node, TriggerNode):
                rc += self.renderTrigger(node)
            elif isinstance(node, DataTransformerNode):
                rc += self.renderDataTransformer(node)
            else:
                raise Exception(f"Unknown node type {node}")
        return rc

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
