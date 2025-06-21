"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md import Ecosystem, DataPlatform
from datasurface.md import ValidationTree
from typing import Optional
import os


def generatePlatformBootstrap(modelFolderName: str, basePlatformDir: str, *platformNames: str) -> None:
    """This will generate the platform bootstrap files for a given platform using the model defined in
    the model folder, the model must be called 'eco.py'. The files will be generated in the {basePlatformDir}/{platformName} directory."""

    # Load the model
    eco: Optional[Ecosystem]
    ecoTree: Optional[ValidationTree]
    eco, ecoTree = loadEcosystemFromEcoModule(modelFolderName)
    if eco is None or (ecoTree is not None and ecoTree.hasErrors()):
        if ecoTree is not None:
            ecoTree.printTree()
        raise Exception(f"Failed to load ecosystem from {modelFolderName}")

    # Generate the platform bootstrap files
    assert eco is not None

    # Find the platform
    for platformName in platformNames:
        dp: DataPlatform = eco.getDataPlatformOrThrow(platformName)
        # Generate the bootstrap files
        bootstrapArtifacts: dict[str, str] = dp.generateBootstrapArtifacts()
        # Create the platform directory if it doesn't exist
        os.makedirs(os.path.join(basePlatformDir, platformName), exist_ok=True)
        for name, content in bootstrapArtifacts.items():
            with open(os.path.join(basePlatformDir, platformName, name), "w") as f:
                f.write(content)
