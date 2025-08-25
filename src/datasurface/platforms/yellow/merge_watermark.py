"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset
)
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection, Result
from typing import cast, List, Optional, Any
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform,
    BatchStatus, BatchState, JobStatus
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.jobs import Job
from datasurface.platforms.yellow.merge_forensic import SnapshotMergeJobForensic
from datasurface.platforms.yellow.merge_live import SnapshotMergeJobLiveOnly
from datasurface.platforms.yellow.merge_forensic import SnapshotDeltaMode
from datasurface.md.governance import SQLWatermarkSnapshotDeltaIngestion

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class MergeWatermarkJob(Job):
    """
    This ingestion type uses a watermark column to ingest data in a source. For the first batch it will determine the high watermark column value
    across all datasets and then ingest all records with a watermark value less than the high watermark. The high watermark is then stored in the batch state.
    For subsequent batches, it will read the previous batch high watermark, calculate the new high watermark across all datasets and then ingest all
    records with a watermark >= the old watermark and < the new high watermark. The new high watermark is then stored in the batch state.
    """
    def __init__(
            self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
            store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

    def substituteStringsinSQL(self, origSQL: str, vars: dict[str, str]) -> str:
        """This method substitutes the strings in the SQL string with the values in the vars dictionary."""
        for key, value in vars.items():
            origSQL = origSQL.replace(f"{{{key}}}", value)
        return origSQL

    def getHighWatermarkForAllDatasets(self, connection: Connection, watermarkColumn: str, state: BatchState) -> Any:
        """
        This fetches the high watermark value across all dataset tables using database operations
        """
        highWatermark: Optional[Any] = None
        for datasetName in state.all_datasets:
            dataset: Dataset = self.store.datasets[datasetName]
            tableName: str = self.getPhysSourceTableName(dataset)

            # Use the new database operations method to get max watermark value
            sql: str = self.source_db_ops.get_max_watermark_value(tableName, watermarkColumn)
            result: Result = connection.execute(text(sql))

            # The watermark column type is either an integer or a timestamp
            candidateHighWatermark: Optional[Any] = result.scalar()
            if candidateHighWatermark is not None:
                if highWatermark is None:
                    highWatermark = candidateHighWatermark
                else:
                    highWatermark = max(highWatermark, candidateHighWatermark)
        return highWatermark

    HIGH_WATERMARK_KEY: str = "highWatermark"

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int) -> tuple[int, int, int]:
        """
        This will grab the high watermark across all datasets and ingest records based on watermark ranges.
        For the first batch, it ingests all records with watermark < high_watermark.
        For subsequent batches, it ingests records with watermark >= previous_high and < current_high.
        """
        currBatchState: Optional[BatchState] = None
        # Fetch restart state from batch metrics table
        cmd: SQLWatermarkSnapshotDeltaIngestion = cast(SQLWatermarkSnapshotDeltaIngestion, self.store.cmd)
        previousHighWatermark: Optional[Any] = None
        latestHighWatermark: Optional[Any] = None

        # First calculate the previous and latest high watermarks
        with mergeEngine.begin() as mergeConn:
            # Get the state from the batch metrics table
            previousBatchState: BatchState = self.getBatchState(mergeEngine, mergeConn, key, batchId-1)
            # There will be a state because of createBatch
            currBatchState = self.getBatchState(mergeEngine, mergeConn, key, batchId)
            # Check for schema changes before ingestion
            self.checkForSchemaChanges(previousBatchState)
            previousHighWatermark = previousBatchState.job_state.get(self.HIGH_WATERMARK_KEY)
            latestHighWatermark = None
            # Truncate the staging tables for each dataset in the batch state
            self.truncateStagingTables(mergeConn, currBatchState, batchId)

        # Get the latest high watermark for all datasets
        with sourceEngine.connect() as sourceConn:
            latestHighWatermark = self.getHighWatermarkForAllDatasets(sourceConn, cmd.watermarkColumn, previousBatchState)
            if latestHighWatermark is None:
                # No records to ingest - update batch state to INGESTED and return
                with mergeEngine.begin() as mergeConn:
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.INGESTED, state=currBatchState)
                return 0, 0, 0

        # Update batch state with the new high watermark
        currBatchState.job_state[self.HIGH_WATERMARK_KEY] = latestHighWatermark

        # Define the watermark SQL generator based on whether this is first batch or incremental
        if previousHighWatermark is None:
            # Snapshot: fetch all records up to the latest high watermark
            def watermark_sql_generator(sourceTableName: str, allColumns: List[str], pkColumns: List[str]) -> str:
                return self.source_db_ops.get_watermark_records_lt(
                    sourceTableName, allColumns, pkColumns, cmd.watermarkColumn, latestHighWatermark
                )
        else:
            # Incremental: fetch records in watermark range
            def watermark_sql_generator(sourceTableName: str, allColumns: List[str], pkColumns: List[str]) -> str:
                return self.source_db_ops.get_watermark_records_range(
                    sourceTableName, allColumns, pkColumns, cmd.watermarkColumn, previousHighWatermark, latestHighWatermark
                )

        recordsInserted = 0
        with sourceEngine.connect() as sourceConn:
            currBatchState.reset()
            while currBatchState.hasMoreDatasets():  # While there is a dataset to ingest
                # Get source table name, Map the dataset name if necessary
                datasetRecordsInserted, _, _ = self.baseIngestSingleDataset(
                    currBatchState, sourceConn, mergeEngine, key, batchId, watermark_sql_generator)
                recordsInserted += datasetRecordsInserted
                # All rows for this dataset have been ingested, move to next dataset.

                # Move to next dataset if any
                currBatchState.moveToNextDataset()
                # If there are more datasets then we stay in STARTED and the next dataset will be ingested
                # on the next iteration. If there are no more datasets then we set the status to INGESTED and
                # the job finishes and waits for the next trigger for MERGE to start.
                if currBatchState.hasMoreDatasets():
                    # Move to next dataset
                    logger.debug("Moving to next dataset", next_dataset=currBatchState.getCurrentDataset())
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.STARTED, state=currBatchState)
                else:
                    # No more datasets to ingest, set the state to merging
                    logger.debug("No more datasets to ingest, setting status to INGESTED")
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.INGESTED, state=currBatchState)

        return recordsInserted, 0, 0

    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        return self.executeNormalRollingBatch(sourceEngine, mergeEngine, key)


class MergeWatermarkForensic(MergeWatermarkJob):
    """This job will ingest data from a remote forensic merge table into a local forensic merge table.
    It maintains efficient incremental synchronization by:

    1. Seed Batch: On first run, pulls all live records from the remote merge table
    2. Incremental Batches: Uses remote batch milestoning to identify only changed records since last sync
    3. Remote Batch Tracking: Stores the last processed remote batch ID in the job_state dict

    The remote source table is expected to be a forensic merge table with ds_surf_batch_in and
    ds_surf_batch_out columns for milestoning. Live records have batch_out = 2147483647 (LIVE_RECORD_ID).
    """
    def __init__(
            self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
            store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, chunkSize: int = 10000) -> tuple[int, int, int]:
        """This is the same as the normal mergeStagingToMergeAndCommit but it uses the forensic merge statements."""
        return SnapshotMergeJobForensic.genericMergeStagingToMergeAndCommitForensic(self, mergeEngine, batchId, key, SnapshotDeltaMode.UPSERT, chunkSize)


class MergeWatermarkLive(MergeWatermarkJob):
    def __init__(
            self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
            store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, chunkSize: int = 10000) -> tuple[int, int, int]:
        """This is the same as the normal mergeStagingToMergeAndCommit but it uses the live merge statements."""
        return SnapshotMergeJobLiveOnly.genericMergeStagingToMergeAndCommitLive(self, mergeEngine, batchId, key, SnapshotDeltaMode.UPSERT, chunkSize)
