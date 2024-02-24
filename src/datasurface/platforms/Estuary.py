from typing import Type
from jinja2 import Environment, PackageLoader, select_autoescape
from datasurface.md import Documentation
from datasurface.md import Ecosystem
from datasurface.md import ValidationTree
from datasurface.md.Governance import CloudVendor, DataContainer
from datasurface.md.PipelineGraph import DataTransformerNode, ExportNode, IngestionMultiNode, IngestionSingleNode, PipelineNode, TriggerNode
from datasurface.md.IaCPlatform import IaCDataPlatform, IaCFragmentManager


class EstuaryFragmentManager(IaCFragmentManager):
    def __init__(self, name: str, yamlFileName: str, doc: Documentation):
        super().__init__(name, doc)
        self.yamlFileName: str = yamlFileName
        self.captures: dict[str, str] = {}
        self.collections: dict[str, str] = {}
        self.materializations: dict[str, str] = {}

        self.jinjaEnv : Environment = Environment(
            loader=PackageLoader('datasurface.platforms.estuary', 'templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def addFragment(self, nodeType: Type[PipelineNode], name: str, fragment: str):
        if(nodeType is IngestionSingleNode):


class EstuaryPlatform(IaCDataPlatform):
    def __init__(self, name: str, doc: Documentation):
        super().__init__(name, doc)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, EstuaryPlatform)

    def lintIngestionSingleNode(self, eco: Ecosystem, node: IngestionSingleNode, tree: ValidationTree) -> None:
        tree.addProblem(f"Estuary does not support IngestionSingleNode nodes {node}")

    def lintIngestionMultiNode(self, eco: Ecosystem, node: IngestionMultiNode, tree: ValidationTree) -> None:
        pass

    def lintExportNode(self, eco: Ecosystem, node: ExportNode, tree: ValidationTree) -> None:
        pass

    def lintTriggerNode(self, eco: Ecosystem, node: TriggerNode, tree: ValidationTree) -> None:
        pass

    def lintDataTransformerNode(self, eco: Ecosystem, node: DataTransformerNode, tree: ValidationTree) -> None:
        pass

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return dc.isUsingVendorsOnly(eco, {CloudVendor.AWS, CloudVendor.AZURE, CloudVendor.GCP})

