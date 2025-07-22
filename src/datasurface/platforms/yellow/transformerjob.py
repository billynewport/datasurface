"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.sqlalchemyutils import createOrUpdateTable
from datasurface.platforms.yellow.jobs import JobUtilities, JobStatus
from datasurface.md import Ecosystem, CredentialStore, Workspace, WorkspaceCacheEntry, Datastore, Dataset
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
from sqlalchemy import Engine, text
from datasurface.platforms.yellow.jobs import createEngine
from datasurface.md.codeartifact import PythonRepoCodeArtifact
from typing import cast, Dict, Any, Optional, Callable
from datasurface.cmd.platform import cloneGitRepository
import os
import sys
import copy
import importlib
from types import ModuleType
from sqlalchemy.engine import Connection
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
import sqlalchemy
from sqlalchemy import Table


class DataTransformerJob(JobUtilities):
    """This is a job which runs a DataTransformer."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform, workspaceName: str, workingFolder: str) -> None:
        super().__init__(eco, credStore, dp)
        self.workspaceName: str = workspaceName
        self.workingFolder: str = workingFolder

    def _buildDatasetMapping(self, workspace: Workspace, outputDatastore: Datastore) -> Dict[str, str]:
        """Build a mapping from store#dataset names to table names for the workspace."""
        dataset_mapping: Dict[str, str] = {}

        # Add input datasets from the workspace
        for dsg in workspace.dsgs.values():
            for ds in dsg.sinks.values():
                store_dataset_key = f"{ds.storeName}#{ds.datasetName}"
                store: Datastore = self.eco.cache_getDatastoreOrThrow(ds.storeName).datastore
                dataset: Dataset = store.datasets[ds.datasetName]
                # Table names in merge database follow the pattern: store_dataset
                table_name = self.getRawMergeTableNameForDataset(store, dataset)
                dataset_mapping[store_dataset_key] = table_name

        # Add output datasets with dt_ prefix
        for dataset in outputDatastore.datasets.values():
            # Need the prefix incase the output datastore is also in the inputs on the workspace
            store_dataset_key = f"dt_{outputDatastore.name}#{dataset.name}"
            table_name = self.getDataTransformerOutputTableNameForDatasetForIngestionOnly(outputDatastore, dataset)
            dataset_mapping[store_dataset_key] = table_name

        return dataset_mapping

    def _truncateOutputTables(self, connection: Connection, outputDatastore: Datastore) -> None:
        """Truncate all output tables for the DataTransformer."""
        for dataset in outputDatastore.datasets.values():
            table_name = self.getDataTransformerOutputTableNameForDatasetForIngestionOnly(outputDatastore, dataset)
            try:
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
                print(f"Truncated output table: {table_name}")
            except Exception as e:
                print(f"Could not truncate table {table_name}: {e}")

    def executeTransformer(self, codeDir: str, connection: Connection, workspace: Workspace, outputDatastore: Datastore) -> Optional[Any]:
        """Load and execute the transformer code."""
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
                executeTransformer: Callable = getattr(module, "executeTransformer")

                # Build the dataset mapping
                dataset_mapping = self._buildDatasetMapping(workspace, outputDatastore)

                # Call the transformer function
                result = executeTransformer(connection, dataset_mapping)

                print(f"DataTransformer executed successfully for workspace: {self.workspaceName}")
                return result

            except ModuleNotFoundError:
                # Should only happen on initial setup of a repository
                print(f"Transformer module not found in {codeDir}")
                return None
            except AttributeError as e:
                print(f"createTransformer function not found in transformer module: {e}")
                return None
            except Exception as e:
                print(f"Error executing transformer: {e}")
                raise
        finally:
            sys.path = origSystemPath

    def run(self) -> JobStatus:
        try:
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

            # Get the output datastore
            outputDatastore: Datastore = w.dataTransformer.outputDatastore

            # Create or update the output tables
            for dataset in outputDatastore.datasets.values():
                store: Datastore = self.eco.cache_getDatastoreOrThrow(outputDatastore.name).datastore
                t: Table = datasetToSQLAlchemyTable(
                    dataset, self.getDataTransformerOutputTableNameForDatasetForIngestionOnly(store, dataset), sqlalchemy.MetaData())
                createOrUpdateTable(systemMergeEngine, t)

            # Execute the transformer in a transaction
            with systemMergeEngine.begin() as connection:
                # Truncate output tables before running transformer
                self._truncateOutputTables(connection, outputDatastore)

                # Execute the transformer code
                result = self.executeTransformer(clone_dir, connection, w, outputDatastore)

                if result is None:
                    print("DataTransformer returned no result")

                print(f"DataTransformer job completed for workspace: {self.workspaceName}")

            # At this point the data is in the output tables ready for the ingestion job to grab it as a snapshot
            return JobStatus.DONE

        except Exception as e:
            print(f"DataTransformer job failed: {e}")
            return JobStatus.ERROR
