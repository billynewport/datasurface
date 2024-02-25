from typing import Any
from jinja2 import Environment, PackageLoader, Template, select_autoescape
from datasurface.md import Documentation
from datasurface.md import Ecosystem
from datasurface.md import ValidationTree
from datasurface.md.Governance import CloudVendor, DataContainer
from datasurface.md.PipelineGraph import DataTransformerNode, ExportNode, IngestionMultiNode, IngestionSingleNode, TriggerNode
from datasurface.md.IaCPlatform import IaCDataPlatform


class EstuaryPlatform(IaCDataPlatform):
    def __init__(self, name: str, doc: Documentation, streamStore: DataContainer):
        super().__init__(name, doc)
        self.streamStore: DataContainer = streamStore
        self.env = Environment(
            loader=PackageLoader('datasurface.platforms.estuary', 'templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, EstuaryPlatform)

    def renderIngestionSingle(self, ingestNode: IngestionSingleNode) -> str:
        template: Template = self.env.get_template('captureSingle.yaml', None)

        data: dict[str, Any] = {}
        data["datastore"] = store
        data["getDatasets"] = getDatasets
        data["getColumns"] = getColumns
        data["convertColumnAttributesToString"] = convertColumnAttributesToString
        code: str = template.render(data)

        return code
        

    def renderIngestionMulti(self, ingestNode: IngestionMultiNode) -> str:
        pass

    def renderExport(self, exportNode: ExportNode) -> str:
        pass

    def renderTrigger(self, triggerNode: TriggerNode) -> str:
        pass

    def renderDataTransformer(self, dtNode: DataTransformerNode) -> str:
        pass

    def lintIngestionSingleNode(self, eco: Ecosystem, node: IngestionSingleNode, tree: ValidationTree) -> None:
        pass

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

    def getInternalDataContainers(self) -> set[DataContainer]:
        return {self.streamStore}
