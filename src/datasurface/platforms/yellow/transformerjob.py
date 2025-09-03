"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.sqlalchemyutils import createOrUpdateTable
from datasurface.platforms.yellow.yellow_dp import JobUtilities, JobStatus
from datasurface.md import (
    Ecosystem, CredentialStore, Workspace, WorkspaceCacheEntry, Datastore, Dataset,
    Credential
)
from datasurface.md.credential import CredentialType
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform, KubernetesEnvVarsCredentialStore
from sqlalchemy import Engine, text
from datasurface.platforms.yellow.db_utils import createEngine, createInspector
from typing import cast, Any, Optional
from datasurface.cmd.platform import getLatestModelAtTimestampedFolder
import os
import sys
from sqlalchemy.engine import Connection
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
import sqlalchemy
from sqlalchemy import Table
import argparse
from datasurface.md.repo import GitHubRepository
from datasurface.md.lint import ValidationTree
from datasurface.platforms.yellow.yellow_dp import YellowDatasetUtilities
from abc import ABC, abstractmethod
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger, set_context,
    log_operation_timing
)
from datasurface.md.model_schema import DatasurfaceTransformerType, executeModelExternalizer
from datasurface.md.codeartifact import PythonRepoCodeArtifact
from datasurface.md import DatasurfaceTransformerCodeArtifact
# Removed import to avoid circular dependency - will import in function
import copy
import importlib
from types import ModuleType
from datasurface.cmd.platform import cloneGitRepository
from typing import Callable
from datasurface.platforms.yellow.transformer_context import DataTransformerContext
# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class CodeArtifactHandler(ABC):
    def __init__(self, job: 'DataTransformerJob') -> None:
        self.job: 'DataTransformerJob' = job

    @abstractmethod
    def execute(self, connection: Connection) -> Any:
        """Execute the code artifact."""
        pass


class InternalDataTransformerContext(DataTransformerContext):
    """This class is used to map dataset names to table names for the workspace."""
    def __init__(self, eco: Ecosystem, workspace: Workspace, dp: YellowDataPlatform) -> None:
        super().__init__(eco, workspace, dp)

    def addInputDataset(self, dsg: str, storeName: str, datasetName: str, table_name: str) -> None:
        self._input_dataset_to_table_mapping[self._getInputKey(dsg, storeName, datasetName)] = table_name

    def addOutputDataset(self, storeName: str, datasetName: str, table_name: str) -> None:
        self._output_dataset_to_table_mapping[self._getOutputKey(storeName, datasetName)] = table_name


class DataTransformerJob(JobUtilities):
    """This is a job which runs a DataTransformer."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform, workspaceName: str, workingFolder: str) -> None:
        super().__init__(eco, credStore, dp)
        self.workspaceName: str = workspaceName
        self.workingFolder: str = workingFolder

        # Set logging context for this job
        set_context(workspace=workspaceName, platform=dp.name)

    def _buildDatasetMapping(self, workspace: Workspace, outputDatastore: Datastore) -> DataTransformerContext:
        """Build a mapping from store#dataset names to table names for the workspace."""
        dataset_mapping: InternalDataTransformerContext = InternalDataTransformerContext(self.eco, workspace, self.dp)

        # Add input datasets from the workspace
        for dsg in workspace.dsgs.values():
            for ds in dsg.sinks.values():
                store: Datastore = self.eco.cache_getDatastoreOrThrow(ds.storeName).datastore
                dataset: Dataset = store.datasets[ds.datasetName]
                # Use live view name instead of merge table name for DataTransformers
                # This ensures DataTransformers work with live data only
                utils = YellowDatasetUtilities(self.eco, self.credStore, self.dp, store, dataset.name)
                view_name = utils.getPhysWorkspaceLiveViewName(workspace.name, dsg.name)
                dataset_mapping.addInputDataset(dsg.name, store.name, dataset.name, view_name)

        # Add output datasets with dt_ prefix
        for dataset in outputDatastore.datasets.values():
            # Need the prefix incase the output datastore is also in the inputs on the workspace
            table_name = self.getPhysDataTransformerOutputTableNameForDatasetForIngestionOnly(outputDatastore, dataset)
            dataset_mapping.addOutputDataset(outputDatastore.name, dataset.name, table_name)

        return dataset_mapping

    def _truncateOutputTables(self, connection: Connection, outputDatastore: Datastore) -> None:
        """Truncate all output tables for the DataTransformer."""
        for dataset in outputDatastore.datasets.values():
            table_name = self.getPhysDataTransformerOutputTableNameForDatasetForIngestionOnly(outputDatastore, dataset)
            try:
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
                logger.info(f"Truncated output table: {table_name}")
            except Exception as e:
                logger.error(f"Could not truncate table {table_name}: {e}")

    def run(self, credStore: CredentialStore) -> JobStatus:
        try:
            logger.info("Starting DataTransformer job",
                        workspace_name=self.workspaceName,
                        working_folder=self.workingFolder)

            # Now, get a connection to the merge database
            with log_operation_timing(logger, "database_connection_setup"):
                systemMergeUser, systemMergePassword = self.dp.getPSP().credStore.getAsUserPassword(self.dp.getPSP().mergeRW_Credential)
                systemMergeEngine: Engine = createEngine(self.dp.getPSP().mergeStore, systemMergeUser, systemMergePassword)
                inspector = createInspector(systemMergeEngine)

            # Need to create the dt tables for the output datastore if they don't exist
            wce: WorkspaceCacheEntry = self.eco.cache_getWorkspaceOrThrow(self.workspaceName)
            w: Workspace = wce.workspace
            assert w.dataTransformer is not None
            assert w.dataTransformer.outputDatastore is not None
            # Import here to avoid circular dependency
            codeArtifactHandler: CodeArtifactHandler = createCodeArtifactHandler(self)
            # Get the output datastore
            outputDatastore: Datastore = w.dataTransformer.outputDatastore

            # Create or update the output tables
            with systemMergeEngine.begin() as connection:
                for dataset in outputDatastore.datasets.values():
                    store: Datastore = self.eco.cache_getDatastoreOrThrow(outputDatastore.name).datastore
                    t: Table = datasetToSQLAlchemyTable(
                        dataset, self.getPhysDataTransformerOutputTableNameForDatasetForIngestionOnly(store, dataset), sqlalchemy.MetaData(), systemMergeEngine)
                    createOrUpdateTable(connection, inspector, t)

            # Reset an open batch if one exists.
            with log_operation_timing(logger, "batch_creation_or_reset"):
                reset_rc: str = self.dp.resetBatchState(self.eco, outputDatastore.name, committedOk=True)
                if reset_rc != "SUCCESS":
                    logger.error("Failed to reset batch state", reason=reset_rc)
                    return JobStatus.ERROR

            # Execute the transformer in a transaction
            with log_operation_timing(logger, "transformer_execution"):
                logger.info("Starting Transaction for job")
                with systemMergeEngine.begin() as connection:
                    # Truncate output tables before running transformer
                    self._truncateOutputTables(connection, outputDatastore)

                    # Execute the transformer code
                    result = codeArtifactHandler.execute(connection)

                    if result is None:
                        logger.warning("DataTransformer returned no result")

                    logger.info("DataTransformer job completed for workspace",
                                workspace_name=self.workspaceName)

            # At this point the data is in the output tables ready for the ingestion job to grab it as a snapshot
            return JobStatus.DONE

        except Exception as e:
            logger.exception("DataTransformer job failed",
                             exc_info=e,
                             workspace_name=self.workspaceName,
                             working_folder=self.workingFolder)
            return JobStatus.ERROR


def createCodeArtifactHandler(job: 'DataTransformerJob') -> CodeArtifactHandler:
    """Create a code artifact handler for the given job."""
    w: Workspace = job.eco.cache_getWorkspaceOrThrow(job.workspaceName).workspace
    if w.dataTransformer is None:
        raise ValueError(f"Workspace {job.workspaceName} has no data transformer")
    if w.dataTransformer.code is None:
        raise ValueError(f"Workspace {job.workspaceName} has no data transformer code")

    if isinstance(w.dataTransformer.code, PythonRepoCodeArtifact):
        return PythonRepoCodeArtifactHandler(job)
    elif isinstance(w.dataTransformer.code, DatasurfaceTransformerCodeArtifact):
        return SystemCodeArtifactHandler(job)
    else:
        raise ValueError(f"Unsupported code artifact type: {type(w.dataTransformer.code)}")


class PythonRepoCodeArtifactHandler(CodeArtifactHandler):
    def __init__(self, job: 'DataTransformerJob') -> None:
        super().__init__(job)

    def callTransformer(self, codeDir: str, connection: Connection, workspace: Workspace, outputDatastore: Datastore) -> Optional[Any]:
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
                dataset_mapping = self.job._buildDatasetMapping(workspace, outputDatastore)

                # Call the transformer function
                logger.info(f"Calling transformer function: {executeTransformer}")
                result = executeTransformer(connection, dataset_mapping)
                logger.info(f"Transformer function result: {result}")
                logger.info(f"DataTransformer executed successfully for workspace: {self.job.workspaceName}")
                return result

            except ModuleNotFoundError:
                # Should only happen on initial setup of a repository
                logger.error(f"Transformer module not found in {codeDir}")
                raise
            except AttributeError as e:
                logger.error(f"createTransformer function not found in transformer module: {e}")
                raise
            except Exception as e:
                logger.error(f"Error executing transformer: {e}")
                raise
        finally:
            sys.path = origSystemPath

    def execute(self, connection: Connection) -> Any:
        """Execute the code artifact."""
        wce: WorkspaceCacheEntry = self.job.eco.cache_getWorkspaceOrThrow(self.job.workspaceName)
        w: Workspace = wce.workspace
        if w.dataTransformer is None:
            raise ValueError(f"Workspace {self.job.workspaceName} has no data transformer")
        if w.dataTransformer.outputDatastore is None:
            raise ValueError(f"Workspace {self.job.workspaceName} has no output datastore")
        if w.dataTransformer.code is not None and not isinstance(w.dataTransformer.code, PythonRepoCodeArtifact):
            raise ValueError(f"Workspace {self.job.workspaceName} has a code artifact that is not a PythonRepoCodeArtifact")
        code: PythonRepoCodeArtifact = w.dataTransformer.code

        # git clone the code artifact in to the code folder in the working folder
        clone_dir: str = os.path.join(self.job.workingFolder, "code")
        with log_operation_timing(logger, "git_clone_repository", repo=str(code.repo)):
            finalCodeFolder: str = cloneGitRepository(self.job.credStore, code.repo, clone_dir)
        return self.callTransformer(finalCodeFolder, connection, w, w.dataTransformer.outputDatastore)


class SystemCodeArtifactHandler(CodeArtifactHandler):
    def __init__(self, job: 'DataTransformerJob') -> None:
        super().__init__(job)

    def execute(self, connection: Connection) -> Any:
        """Execute the code artifact."""
        wce: WorkspaceCacheEntry = self.job.eco.cache_getWorkspaceOrThrow(self.job.workspaceName)
        w: Workspace = wce.workspace
        if w.dataTransformer is None:
            raise ValueError(f"Workspace {self.job.workspaceName} has no data transformer")
        if w.dataTransformer.outputDatastore is None:
            raise ValueError(f"Workspace {self.job.workspaceName} has no output datastore")
        if w.dataTransformer.code is not None and not isinstance(w.dataTransformer.code, DatasurfaceTransformerCodeArtifact):
            raise ValueError(f"Workspace {self.job.workspaceName} has a code artifact that is not a DatasurfaceTransformerCodeArtifact")
        code: DatasurfaceTransformerCodeArtifact = w.dataTransformer.code
        if code.type == DatasurfaceTransformerType.EXTERNALIZE_MODEL:
            executeModelExternalizer(connection, self.job._buildDatasetMapping(w, w.dataTransformer.outputDatastore))
        else:
            raise ValueError(f"Unsupported code artifact type: {code.type}")


def main():
    """Main entry point for the DataTransformerJob when run as a command-line tool."""
    parser = argparse.ArgumentParser(description='Run DataTransformerJob for a specific workspace')
    parser.add_argument('--platform-name', required=True, help='Name of the platform')
    parser.add_argument('--workspace-name', required=True, help='Name of the workspace')
    parser.add_argument('--operation', default='run-datatransformer', help='Operation to perform')
    parser.add_argument('--working-folder', default='/tmp/datatransformer', help='Working folder for temporary files')
    parser.add_argument('--git-repo-path', required=True, help='Path to the git repository or cache')
    parser.add_argument('--git-repo-owner', required=True, help='GitHub repository owner (e.g., billynewport)')
    parser.add_argument('--git-repo-name', required=True, help='GitHub repository name (e.g., mvpmodel)')
    parser.add_argument('--git-repo-branch', required=True, help='GitHub repository live branch (e.g., main)')
    parser.add_argument('--git-platform-repo-credential-name', required=True, help='GitHub credential name for accessing the model repository (e.g., git)')
    parser.add_argument('--use-git-cache', action='store_true', default=False, help='Use shared git cache for better performance (default: False)')
    parser.add_argument('--max-cache-age-minutes', type=int, default=5, help='Maximum cache age in minutes before checking remote (default: 5)')

    args = parser.parse_args()
    credStore: CredentialStore = KubernetesEnvVarsCredentialStore("Job cred store", set())

    # Ensure the working directory exists
    os.makedirs(args.working_folder, exist_ok=True)

    # Ensure the directory exists
    os.makedirs(args.git_repo_path, exist_ok=True)

    eco: Optional[Ecosystem] = None
    tree: Optional[ValidationTree] = None
    eco, tree = getLatestModelAtTimestampedFolder(
        credStore,
        GitHubRepository(
            f"{args.git_repo_owner}/{args.git_repo_name}",
            args.git_repo_branch,
            credential=Credential(args.git_platform_repo_credential_name, CredentialType.API_TOKEN)),
        args.git_repo_path,
        doClone=not args.use_git_cache,  # Only do direct clone if not using cache
        useCache=args.use_git_cache,     # Use cache by default
        maxCacheAgeMinutes=args.max_cache_age_minutes)
    if tree is not None and tree.hasErrors():
        logger.error("Ecosystem model has errors", validation_errors=tree.getErrorsAsStructuredData())
        return -1  # ERROR
    if eco is None or tree is None:
        logger.error("Failed to load ecosystem")
        return -1  # ERROR

    if args.operation == "run-datatransformer":
        logger.info(f"Running {args.operation} for platform: {args.platform_name}, workspace: {args.workspace_name}")

        dp: Optional[YellowDataPlatform] = cast(YellowDataPlatform, eco.getDataPlatform(args.platform_name))
        if dp is None:
            logger.error(f"Unknown platform: {args.platform_name}")
            return -1  # ERROR

        # Check if the workspace exists
        wce: Optional[WorkspaceCacheEntry] = eco.cache_getWorkspace(args.workspace_name)
        if wce is None:
            logger.error(f"Unknown workspace: {args.workspace_name}")
            return -1  # ERROR

        workspace: Workspace = wce.workspace
        if workspace.dataTransformer is None:
            logger.error(f"Workspace {args.workspace_name} has no DataTransformer")
            return -1  # ERROR

        # Create and run the DataTransformer job
        job: DataTransformerJob = DataTransformerJob(eco, dp.getCredentialStore(), dp, args.workspace_name, args.working_folder)
        jobStatus: JobStatus = job.run(credStore)

        if jobStatus == JobStatus.DONE:
            logger.info("DataTransformer job completed successfully")
            return 0  # DONE
        elif jobStatus == JobStatus.KEEP_WORKING:
            logger.info("DataTransformer job is still in progress")
            return 1  # KEEP_WORKING
        else:
            logger.error("DataTransformer job failed")
            return -1  # ERROR
    else:
        logger.error(f"Unknown operation: {args.operation}")
        return -1  # ERROR


if __name__ == "__main__":
    try:
        exit_code = main()
        logger.info(f"DATASURFACE_RESULT_CODE={exit_code}")
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}")
        import traceback
        logger.error(traceback.format_exc())
        logger.error("DATASURFACE_RESULT_CODE=-1")
        exit_code = -1
    # Always exit with 0 (success) - Airflow will parse the result code from logs
    sys.exit(0)
