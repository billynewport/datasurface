"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Ecosystem, Workspace
)
from typing import Dict
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.md import DataPlatform
# Removed import to avoid circular dependency - will import in function

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class DataTransformerContext:
    """This class is used to map dataset names to table names for the workspace."""
    def __init__(self, eco: Ecosystem, workspace: Workspace, dp: DataPlatform) -> None:
        self._eco: Ecosystem = eco
        self._workspace: Workspace = workspace
        self._dataPlatform: DataPlatform = dp
        self._input_dataset_to_table_mapping: Dict[str, str] = {}
        self._output_dataset_to_table_mapping: Dict[str, str] = {}

    def _getInputKey(self, dsg: str, storeName: str, datasetName: str) -> str:
        return f"{dsg}#{storeName}#{datasetName}"

    def _getOutputKey(self, storeName: str, datasetName: str) -> str:
        return f"output#{storeName}#{datasetName}"

    def getInputTableNameForDataset(self, dsg: str, storeName: str, datasetName: str) -> str:
        return self._input_dataset_to_table_mapping.get(self._getInputKey(dsg, storeName, datasetName), "")

    def getOutputTableNameForDataset(self, datasetName: str) -> str:
        assert self._workspace.dataTransformer is not None
        tableName: str = self._output_dataset_to_table_mapping.get(self._getOutputKey(self._workspace.dataTransformer.outputDatastore.name, datasetName), "")
        if tableName == "":
            raise ValueError(f"Output table name not found for dataset {datasetName}")
        return tableName

    def getEcosystem(self) -> Ecosystem:
        return self._eco

    def getPlatform(self) -> DataPlatform:
        return self._dataPlatform

    def getWorkspace(self) -> Workspace:
        return self._workspace
