"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.platforms.yellow.jobs import JobUtilities, JobStatus
from datasurface.md import Ecosystem, CredentialStore, Workspace, WorkspaceCacheEntry, Datastore
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
from sqlalchemy import Engine
from datasurface.platforms.yellow.jobs import createEngine
from datasurface.md.codeartifact import PythonRepoCodeArtifact
from typing import cast
from datasurface.cmd.platform import cloneGitRepository
import os
import sys
import copy
import importlib
from types import ModuleType
from sqlalchemy.engine import Connection


class DataTransformerJob(JobUtilities):
    """This is a job which runs a DataTransformer."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform, workspaceName: str, workingFolder: str) -> None:
        super().__init__(eco, credStore, dp)
        self.workspaceName: str = workspaceName
        self.workingFolder: str = workingFolder

    def executeTransformer(self, codeDir: str, connection: Connection) -> None:
        # Try to load and execute the transformer code
        origSystemPath: list[str] = copy.deepcopy(sys.path)
        try:
            sys.path.append(codeDir)

            transformerModuleName: str = "transformer"

            # Remove the module from sys.modules to force a reload
            if transformerModuleName in sys.modules:
                del sys.modules[transformerModuleName]

            try:
                module: ModuleType = importlib.import_module(transformerModuleName)
                function = getattr(module, "createTransformer")

                
            except ModuleNotFoundError:
                # Should only happen on inital setup of a repository
                return None, None
        finally:
            sys.path = origSystemPath

    def run(self) -> JobStatus:

        # Now, get a connection to the merge database
        systemMergeUser, systemMergePassword = self.credStore.getAsUserPassword(self.dp.postgresCredential)
        systemMergeEngine: Engine = createEngine(self.dp.mergeStore, systemMergeUser, systemMergePassword)

        # Need to create the dt tables for the output datastore if they don't exist
        wce: WorkspaceCacheEntry = self.eco.cache_getWorkspaceOrThrow(self.workspaceName)
        w: Workspace = wce.workspace
        assert w.dataTransformer is not None
        assert w.dataTransformer.outputDatastore is not None
        code: PythonRepoCodeArtifact = cast(PythonRepoCodeArtifact, w.dataTransformer.code)

        # git clone the code artifact in to the code folder in the working folder
        clone_dir: str = os.path.join(self.workingFolder, "code")
        cloneGitRepository(code.repo, clone_dir)

        # Execute the transformer code

        # Get the output datastore
        outputDatastore: Datastore = w.dataTransformer.outputDatastore

        with systemMergeEngine.begin() as connection:
            self.executeTransformer(clone_dir, connection)
            # Get the state from the batch metrics table

        return JobStatus.DONE
