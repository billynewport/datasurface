"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.sqlalchemyutils import createOrUpdateTable
from datasurface.platforms.yellow.jobs import JobUtilities, JobStatus
from datasurface.md import Ecosystem, CredentialStore, Workspace, WorkspaceCacheEntry, Datastore, Dataset, DataPlatform
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
from sqlalchemy import Engine, text
from datasurface.platforms.yellow.jobs import createEngine
from datasurface.md.codeartifact import PythonRepoCodeArtifact
from typing import cast, Dict, Any, Optional, Callable
from datasurface.cmd.platform import cloneGitRepository, getLatestModelAtTimestampedFolder
import os
import sys
import copy
import importlib
from types import ModuleType
from sqlalchemy.engine import Connection
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
import sqlalchemy
from sqlalchemy import Table
import argparse
from datasurface.md.repo import GitHubRepository
from datasurface.md.lint import ValidationTree


class DataTransformerContext:
    """This class is used to map dataset names to table names for the workspace."""
    def __init__(self, eco: Ecosystem, workspace: Workspace, dp: DataPlatform) -> None:
        self._eco: Ecosystem = eco
        self._workspace: Workspace = workspace
        self._dataPlatform: DataPlatform = dp
        self._input_dataset_to_table_mapping: Dict[str, str] = {}
        self._output_dataset_to_table_mapping: Dict[str, str] = {}

    def getInputTableNameForDataset(self, storeName: str, datasetName: str) -> str:
        return self._input_dataset_to_table_mapping.get(f"{storeName}#{datasetName}", "")

    def getOutputTableNameForDataset(self, storeName: str, datasetName: str) -> str:
        return self._output_dataset_to_table_mapping.get(f"{storeName}#{datasetName}", "")

    def getEcosystem(self) -> Ecosystem:
        return self._eco

    def getPlatform(self) -> DataPlatform:
        return self._dataPlatform

    def getWorkspace(self) -> Workspace:
        return self._workspace


class InternalDataTransformerContext(DataTransformerContext):
    """This class is used to map dataset names to table names for the workspace."""
    def __init__(self, eco: Ecosystem, workspace: Workspace, dp: YellowDataPlatform) -> None:
        super().__init__(eco, workspace, dp)

    def addInputDataset(self, storeName: str, datasetName: str, table_name: str) -> None:
        self._input_dataset_to_table_mapping[f"{storeName}#{datasetName}"] = table_name

    def addOutputDataset(self, storeName: str, datasetName: str, table_name: str) -> None:
        self._output_dataset_to_table_mapping[f"{storeName}#{datasetName}"] = table_name


class DataTransformerJob(JobUtilities):
    """This is a job which runs a DataTransformer."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform, workspaceName: str, workingFolder: str) -> None:
        super().__init__(eco, credStore, dp)
        self.workspaceName: str = workspaceName
        self.workingFolder: str = workingFolder

    def _buildDatasetMapping(self, workspace: Workspace, outputDatastore: Datastore) -> DataTransformerContext:
        """Build a mapping from store#dataset names to table names for the workspace."""
        dataset_mapping: InternalDataTransformerContext = InternalDataTransformerContext(self.eco, workspace, self.dp)

        # Add input datasets from the workspace
        for dsg in workspace.dsgs.values():
            for ds in dsg.sinks.values():
                store: Datastore = self.eco.cache_getDatastoreOrThrow(ds.storeName).datastore
                dataset: Dataset = store.datasets[ds.datasetName]
                # Table names in merge database follow the pattern: store_dataset
                table_name = self.getRawMergeTableNameForDataset(store, dataset)
                dataset_mapping.addInputDataset(store.name, dataset.name, table_name)

        # Add output datasets with dt_ prefix
        for dataset in outputDatastore.datasets.values():
            # Need the prefix incase the output datastore is also in the inputs on the workspace
            table_name = self.getDataTransformerOutputTableNameForDatasetForIngestionOnly(outputDatastore, dataset)
            dataset_mapping.addOutputDataset(outputDatastore.name, dataset.name, table_name)

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
                print(f"Calling transformer function: {executeTransformer}")
                result = executeTransformer(connection, dataset_mapping)
                print(f"Transformer function result: {result}")
                print(f"DataTransformer executed successfully for workspace: {self.workspaceName}")
                return result

            except ModuleNotFoundError:
                # Should only happen on initial setup of a repository
                print(f"Transformer module not found in {codeDir}")
                raise
            except AttributeError as e:
                print(f"createTransformer function not found in transformer module: {e}")
                raise
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
            finalCodeFolder: str = cloneGitRepository(code.repo, clone_dir)

            # Get the output datastore
            outputDatastore: Datastore = w.dataTransformer.outputDatastore

            # Create or update the output tables
            for dataset in outputDatastore.datasets.values():
                store: Datastore = self.eco.cache_getDatastoreOrThrow(outputDatastore.name).datastore
                t: Table = datasetToSQLAlchemyTable(
                    dataset, self.getDataTransformerOutputTableNameForDatasetForIngestionOnly(store, dataset), sqlalchemy.MetaData())
                createOrUpdateTable(systemMergeEngine, t)

            # Execute the transformer in a transaction
            print("Starting Transaction for job")
            with systemMergeEngine.begin() as connection:
                # Truncate output tables before running transformer
                self._truncateOutputTables(connection, outputDatastore)

                # Execute the transformer code
                result = self.executeTransformer(finalCodeFolder, connection, w, outputDatastore)

                if result is None:
                    print("DataTransformer returned no result")

                print(f"DataTransformer job completed for workspace: {self.workspaceName}")

            # At this point the data is in the output tables ready for the ingestion job to grab it as a snapshot
            return JobStatus.DONE

        except Exception as e:
            print(f"DataTransformer job failed: {e}")
            return JobStatus.ERROR


def main():
    """Main entry point for the DataTransformerJob when run as a command-line tool."""
    parser = argparse.ArgumentParser(description='Run DataTransformerJob for a specific workspace')
    parser.add_argument('--platform-name', required=True, help='Name of the platform')
    parser.add_argument('--workspace-name', required=True, help='Name of the workspace')
    parser.add_argument('--operation', default='run-datatransformer', help='Operation to perform')
    parser.add_argument('--working-folder', default='/tmp/datatransformer', help='Working folder for temporary files')
    parser.add_argument('--git-repo-path', required=True, help='Path to the git repository')
    parser.add_argument('--git-repo-owner', required=True, help='GitHub repository owner (e.g., billynewport)')
    parser.add_argument('--git-repo-name', required=True, help='GitHub repository name (e.g., mvpmodel)')
    parser.add_argument('--git-repo-branch', required=True, help='GitHub repository branch (e.g., main)')

    args = parser.parse_args()

    # Ensure the working directory exists
    os.makedirs(args.working_folder, exist_ok=True)

    # Clone the git repository if the directory is empty
    if not os.path.exists(args.git_repo_path) or not os.listdir(args.git_repo_path):
        git_token = os.environ.get('git_TOKEN')
        if not git_token:
            print("ERROR: git_TOKEN environment variable not found")
            return -1

    # Ensure the directory exists
    os.makedirs(args.git_repo_path, exist_ok=True)

    eco: Optional[Ecosystem] = None
    tree: Optional[ValidationTree] = None
    eco, tree = getLatestModelAtTimestampedFolder(
        GitHubRepository(f"{args.git_repo_owner}/{args.git_repo_name}", args.git_repo_branch), args.git_repo_path, doClone=True)
    if tree is not None and tree.hasErrors():
        print("Ecosystem model has errors")
        tree.printTree()
        return -1  # ERROR
    if eco is None or tree is None:
        print("Failed to load ecosystem")
        return -1  # ERROR

    if args.operation == "run-datatransformer":
        print(f"Running {args.operation} for platform: {args.platform_name}, workspace: {args.workspace_name}")

        dp: Optional[YellowDataPlatform] = cast(YellowDataPlatform, eco.getDataPlatform(args.platform_name))
        if dp is None:
            print(f"Unknown platform: {args.platform_name}")
            return -1  # ERROR

        # Check if the workspace exists
        wce: Optional[WorkspaceCacheEntry] = eco.cache_getWorkspace(args.workspace_name)
        if wce is None:
            print(f"Unknown workspace: {args.workspace_name}")
            return -1  # ERROR

        workspace: Workspace = wce.workspace
        if workspace.dataTransformer is None:
            print(f"Workspace {args.workspace_name} has no DataTransformer")
            return -1  # ERROR

        # Create and run the DataTransformer job
        job: DataTransformerJob = DataTransformerJob(eco, dp.getCredentialStore(), dp, args.workspace_name, args.working_folder)
        jobStatus: JobStatus = job.run()

        if jobStatus == JobStatus.DONE:
            print("DataTransformer job completed successfully")
            return 0  # DONE
        elif jobStatus == JobStatus.KEEP_WORKING:
            print("DataTransformer job is still in progress")
            return 1  # KEEP_WORKING
        else:
            print("DataTransformer job failed")
            return -1  # ERROR
    else:
        print(f"Unknown operation: {args.operation}")
        return -1  # ERROR


if __name__ == "__main__":
    try:
        exit_code = main()
        print(f"DATASURFACE_RESULT_CODE={exit_code}")
    except Exception as e:
        print(f"Unhandled exception in main: {e}")
        import traceback
        traceback.print_exc()
        print("DATASURFACE_RESULT_CODE=-1")
        exit_code = -1
    # Always exit with 0 (success) - Airflow will parse the result code from logs
    sys.exit(0)
