"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Ecosystem, ValidationTree
from typing import Optional
import sys
import copy
import importlib
from types import ModuleType
import os


def loadEcosystemFromEcoModule(path: str) -> tuple[Optional[Ecosystem], Optional[ValidationTree]]:
    """This tries to bootstrap an Ecosystem from the file eco.py on a specific path"""
    origSystemPath: list[str] = copy.deepcopy(sys.path)
    try:
        sys.path.append(path)

        # Remove the module from sys.modules to force a reload
        if 'eco' in sys.modules:
            del sys.modules['eco']

        try:
            module: ModuleType = importlib.import_module("eco")
            function = getattr(module, "createEcosystem")
        except ModuleNotFoundError:
            # Should only happen on inital setup of a repository
            return None, None

        # This should now point to createEcosystem() -> Ecosystem in the eco.py file on the path specified
        eco: Ecosystem = function()
        # Flesh out

        tree: ValidationTree = eco.lintAndHydrateCaches()

        # Now hydrate the dsg platform mappings
        eco.hydrateDSGDataPlatformMappings(os.path.join(path, "dsg_platform_mapping.json"), tree)
        eco.hydratePrimaryIngestionPlatforms(os.path.join(path, "primary_ingestion_platforms.json"), tree)
        eco.createGraph()

        return eco, tree
    finally:
        sys.path = origSystemPath
