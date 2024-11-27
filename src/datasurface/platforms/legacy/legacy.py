"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.Governance import DataPlatform, Ecosystem, \
    DataPlatformChooser, DataContainer, Documentation, DataPlatformExecutor, ValidationTree

from typing import Optional


class LegacyDatPlatformChooser(DataPlatformChooser):
    """This chooses the legacy DataPlatform and allows the specifics of the particular
    data pipeline to be specified such as DataContainers used"""
    def __init__(self, name: str, doc: Documentation, containers: set[DataContainer]) -> None:
        super().__init__()
        self.name: str = "LegacyDataPlatformChooser"
        self.containers: set[DataContainer] = set()
        self.doc: Documentation = doc

    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        return LegacyDataPlatform(self.name, Documentation("Legacy Data Platform"))

    def __str__(self) -> str:
        return "LegacyDatPlatformChooser()"


class LegacyDataPlatformExecutor(DataPlatformExecutor):
    """This is a no-op DataPlatformExecutor. It's intent is to specify that the data flows are already realized and externally managed"""

    def __init__(self) -> None:
        super().__init__()

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass

    def __str__(self) -> str:
        return "LegacyDataPlatformExecutor()"


class LegacyDataPlatform(DataPlatform):
    """This is a no-op DataPlatform. It's intent is to specify that the data flows are already realized and externally managed
    by existing systems. However, DataSurface will still track the data flows and manage governance for the data."""
    def __init__(self, name: str, doc: Documentation) -> None:
        super().__init__(name, doc, LegacyDataPlatformExecutor())

    def __str__(self) -> str:
        return f"LegacyDataPlatform({self.name})"
