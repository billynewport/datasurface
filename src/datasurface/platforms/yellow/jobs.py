"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset, IngestionConsistencyType
)
from typing import cast, Optional
from datasurface.md.governance import DatastoreCacheEntry, EcosystemPipelineGraph, PlatformPipelineGraph, SQLIngestion
from datasurface.md.lint import ValidationTree
from datasurface.md.credential import Credential, CredentialType
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, KubernetesEnvVarsCredentialStore
)
from datasurface.cmd.platform import getLatestModelAtTimestampedFolder
import argparse
from datasurface.platforms.yellow.merge import Job
import sys
from datasurface.platforms.yellow.yellow_dp import JobStatus
from datasurface.md.repo import GitHubRepository
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger, set_context,
)
import os

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


"""
This job runs to ingest/merge new data into a dataset. The data is ingested from a source in to a staging table named
after the dataset. The data is ingested in batches and every ingested record is extended with the batch id that ingested
it as well as key and all column md5 hashs. The hashes improve performance when comparing records for equality or
looking up specific records as it allows a single column to be used rather than the entire record or complete primary
key.

The process is tracked using a metadata table, the batch_status table and a table for storing batch counters. The job ingested the
next batch of data from an ingestion stream and then merges it in to the merge table. Thus, an ingestion stream is a serial
collection of batches. Each batch ingests a chunk of data from the source and then merges it with the corresponding merge table
for the dataset. The process state is tracked in a batch_metrics record keyed to the ingestion stream and the batch id. This record
tracks that the batch has been started, is ingesting, is ready for merge or is complete. Some basic batch metrics are also
stored in the batch_metrics table.

The ingestion streams are named/keyed depending on whather the ingestion stream is for every dataset in a store or for just
one dataset in a datastore. The key is either just {storeName} or '{storeName}#{datasetName}.

The state of the ingestion has the following keys:

- datasets_to_go: A list of outstanding datasets to ingest
- current: tuple of dataset_name and offset. The offset is where ingestion of the dataset should restart.

When the last dataset is ingested, the state is set to MERGING and all datasets are merged in a single tx.

This job is designed to be run by Airflow as a KubernetesPodOperator. It returns:
- Exit code 0: "KEEP_WORKING" - The batch is still in progress, reschedule the job
- Exit code 1: "DONE" - The batch is committed or failed, stop rescheduling
"""


def main():
    """Main entry point for the SnapshotMergeJob when run as a command-line tool."""
    parser = argparse.ArgumentParser(description='Run SnapshotMergeJob for a specific ingestion stream')
    parser.add_argument('--platform-name', required=True, help='Name of the platform')
    parser.add_argument('--store-name', help='Name of the datastore')
    parser.add_argument('--dataset-name', help='Name of the dataset (for single dataset ingestion)')
    parser.add_argument('--operation', default='snapshot-merge', help='Operation to perform')
    parser.add_argument('--git-repo-path', required=True, help='Path to the git repository or cache')
    parser.add_argument('--git-repo-owner', required=True, help='GitHub repository owner (e.g., billynewport)')
    parser.add_argument('--git-repo-name', required=True, help='GitHub repository name (e.g., mvpmodel)')
    parser.add_argument('--git-repo-branch', required=True, help='GitHub repository live branch (e.g., main)')
    parser.add_argument('--git-platform-repo-credential-name', required=True, help='GitHub credential name for accessing the model repository (e.g., git)')
    parser.add_argument('--use-git-cache', action='store_true', default=True, help='Use shared git cache for better performance (default: True)')
    parser.add_argument('--max-cache-age-minutes', type=int, default=5, help='Maximum cache age in minutes before checking remote (default: 5)')

    args: argparse.Namespace = parser.parse_args()

    credStore: CredentialStore = KubernetesEnvVarsCredentialStore("Job cred store", set())

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
        logger.error("Ecosystem model has errors", tree_display=tree.getErrorsAsStructuredData())
        return -1  # ERROR
    if eco is None or tree is None:
        logger.error("Failed to load ecosystem")
        return -1  # ERROR

    if args.operation == "snapshot-merge":
        # Set logging context for this job run
        set_context(platform=args.platform_name, workspace=args.store_name)

        logger.info("Starting snapshot-merge operation",
                    operation=args.operation,
                    platform_name=args.platform_name,
                    store_name=args.store_name,
                    dataset_name=args.dataset_name)

        if args.store_name is None:
            logger.error("Store name is required for snapshot-merge operation")
            return -1  # ERROR

        dp: Optional[YellowDataPlatform] = cast(YellowDataPlatform, eco.getDataPlatform(args.platform_name))
        if dp is None:
            logger.error("Unknown platform", platform_name=args.platform_name)
            return -1  # ERROR
        graph: EcosystemPipelineGraph = eco.getGraph()
        root: Optional[PlatformPipelineGraph] = graph.roots.get(dp.name)
        if root is None:
            logger.error("Unknown graph for platform", platform_name=args.platform_name)
            return -1  # ERROR
        # Is this datastore being ingested by this platform?
        if args.store_name not in root.storesToIngest:
            logger.error("Datastore is not being ingested by platform",
                         store_name=args.store_name, platform_name=args.platform_name)
            return -1  # ERROR

        storeEntry: Optional[DatastoreCacheEntry] = eco.cache_getDatastore(args.store_name)
        if storeEntry is None:
            logger.error("Unknown store", store_name=args.store_name)
            return -1  # ERROR
        store: Datastore = storeEntry.datastore
        store.cmd = dp.getEffectiveCMDForDatastore(eco, store)

        if store.cmd is None:
            logger.error("Store has no capture meta data", store_name=args.store_name)
            return -1  # ERROR
        else:
            if store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET:
                if args.dataset_name is None:
                    logger.error("Single dataset ingestion requires a dataset name")
                    return -1  # ERROR
            elif store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.MULTI_DATASET:
                if args.dataset_name is not None:
                    logger.error("Multi dataset ingestion does not require a dataset name")
                    return -1  # ERROR

        # DataTransformer output stores don't need external credentials
        if isinstance(store.cmd, SQLIngestion):
            if store.cmd.credential is None:
                logger.error("Store with SQLIngestion has no credential", store_name=args.store_name)
                return -1  # ERROR

        if args.dataset_name:
            dataset: Optional[Dataset] = store.datasets.get(args.dataset_name)
            if dataset is None:
                logger.error("Unknown dataset", dataset_name=args.dataset_name)
                return -1  # ERROR

        # Import here to avoid circular import
        from datasurface.platforms.yellow.job_factory import calculateCorrectJob
        job: Optional[Job] = calculateCorrectJob(eco, dp, store, args.dataset_name)
        if job is None:
            logger.error("Failed to calculate correct job", operation=args.operation)
            return -1  # ERROR
        jobStatus: JobStatus = job.run()
        if jobStatus == JobStatus.DONE:
            logger.info("Job completed successfully",
                        platform_name=args.platform_name,
                        store_name=args.store_name,
                        dataset_name=args.dataset_name)
            return 0  # DONE
        elif jobStatus == JobStatus.KEEP_WORKING:
            logger.info("Job is still in progress",
                        platform_name=args.platform_name,
                        store_name=args.store_name,
                        dataset_name=args.dataset_name)
            return 1  # KEEP_WORKING
        else:
            logger.error("Job failed",
                         platform_name=args.platform_name,
                         store_name=args.store_name,
                         dataset_name=args.dataset_name)
            return -1  # ERROR
    else:
        logger.error("Unknown operation", operation=args.operation)
        return -1  # ERROR


if __name__ == "__main__":
    try:
        exit_code = main()
        # This specific print must remain - Airflow parses this from logs
        print(f"DATASURFACE_RESULT_CODE={exit_code}")
    except Exception as e:
        logger.error("Unhandled exception in main", exception=str(e))
        import traceback
        traceback.print_exc()
        # This specific print must remain - Airflow parses this from logs
        print("DATASURFACE_RESULT_CODE=-1")
        exit_code = -1

    # Exit with actual result code so Airflow shows correct task status
    # -1 = ERROR (red), 0 = DONE (green), 1 = KEEP_WORKING (yellow/retry)
    if exit_code == -1:
        sys.exit(1)  # Airflow will show as failed (red)
    elif exit_code == 1:
        sys.exit(0)  # KEEP_WORKING - Airflow will retry (should show as running/yellow)
    else:  # exit_code == 0
        sys.exit(0)  # DONE - Airflow will show as success (green)
