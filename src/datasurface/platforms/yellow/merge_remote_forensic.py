"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset
)
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datasurface.md.schema import DDLTable, DDLColumn, PrimaryKeyList
from datasurface.md.types import Integer
from typing import cast, List, Optional, Any
from sqlalchemy import Table, MetaData
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, YellowSchemaProjector,
    BatchStatus, BatchState, JobStatus
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge_remote_live import MergeRemoteJob
from datasurface.platforms.yellow.merge import NoopJobException

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class SnapshotMergeJobRemoteForensic(MergeRemoteJob):
    """This job will ingest data from a remote forensic merge table into a local forensic merge table.
    It maintains efficient incremental synchronization by:

    1. Seed Batch: On first run, pulls all live records from the remote merge table
    2. Incremental Batches: Uses remote batch milestoning to identify only changed records since last sync
    3. Remote Batch Tracking: Stores the last processed remote batch ID in the job_state dict

    The remote source table is expected to be a forensic merge table with ds_surf_batch_in and
    ds_surf_batch_out columns for milestoning. Live records have batch_out = 2147483647 (LIVE_RECORD_ID).

    The destination is also a forensic table, so we maintain full history with proper milestoning.
    """

    def __init__(
            self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
            store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str, engine: Optional[Engine] = None):
        """Override to add remote batch columns for forensic mirror staging tables."""

        # Create a modified dataset with additional columns for staging

        # Add standard staging columns
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector
        # This already includes the batch_id, all_hash, and key_hash columns
        stagingDataset: Dataset = sp.computeSchema(dataset, sp.SCHEMA_TYPE_STAGING)

        # Add remote batch columns to preserve original milestoning
        ddlSchema: DDLTable = cast(DDLTable, stagingDataset.originalSchema)
        ddlSchema.add(DDLColumn(name=f"remote_{sp.BATCH_IN_COLUMN_NAME}", data_type=Integer()))
        ddlSchema.add(DDLColumn(name=f"remote_{sp.BATCH_OUT_COLUMN_NAME}", data_type=Integer()))

        # For forensic staging tables, modify the primary key to include remote_batch_in
        # This allows multiple versions of the same record to exist in staging
        if ddlSchema.primaryKeyColumns:
            # Create new primary key with original columns plus remote_batch_in
            new_pk_columns = list(ddlSchema.primaryKeyColumns.colNames) + [f"remote_{sp.BATCH_IN_COLUMN_NAME}"]
            ddlSchema.primaryKeyColumns = PrimaryKeyList(new_pk_columns)

        t: Table = datasetToSQLAlchemyTable(stagingDataset, tableName, MetaData(), engine)
        return t

    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        """Execute one remote-forensic batch aligned to the latest committed remote batch ID.

        This method uses the remote batch ID as the local batch ID end-to-end. If the latest
        remote batch is already committed locally, it exits early.
        """
        assert self.schemaProjector is not None
        # Determine the remote batch to process
        remoteBatchId: int = self._getCurrentRemoteBatchId(sourceEngine, self.schemaProjector)

        # Fail fast if there is no committed remote batch to pull
        if remoteBatchId == 0:
            raise NoopJobException("No committed remote batches found on remote source; cannot run remote forensic sync")

        # Ingest to staging for this remote batch id (method handles early exit if already committed)
        recordsInserted, _, _ = self.ingestNextBatchToStaging(sourceEngine, mergeEngine, key, remoteBatchId)

        # Proceed to merge the staging data into the forensic merge table
        # Even if no records were inserted, we need to mark the batch as committed
        # to indicate we've processed up to this remote batch ID
        self.mergeStagingToMergeAndCommit(mergeEngine, remoteBatchId, key)
        return JobStatus.DONE

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int) -> tuple[int, int, int]:
        """Ingest data from remote forensic merge table to staging.

        First batch is a seed batch that pulls all live records.
        Subsequent batches use milestoning to get only changed records since last remote batch.
        Note: The local batchId will be updated to match the currentRemoteBatchId being processed.
        """
        # Get current remote batch ID first to determine what we're processing
        assert self.schemaProjector is not None
        currentRemoteBatchId = self._getCurrentRemoteBatchId(sourceEngine, self.schemaProjector)
        logger.info("Current remote batch ID determined", remote_batch_id=currentRemoteBatchId)

        # Use the remote batch ID as our local batch ID for this run
        localBatchId = currentRemoteBatchId

        state: Optional[BatchState] = None
        # Get job state from the last completed batch (not the current one we're about to process)
        with mergeEngine.begin() as connection:
            # Create/ensure batch record exists for the remote batch ID
            should_continue = self._createBatchForRemoteId(connection, key, localBatchId)
            if not should_continue:
                # Remote batch already committed, we're up to date
                logger.info("Job already up to date with remote batch, exiting",
                            remote_batch_id=localBatchId, key=key)
                return 0, 0, 0  # No records processed since already up to date
            # Look for the last completed batch to get job state
            # Query batch metrics table for the highest completed batch
            try:
                result = connection.execute(text(f"""
                    SELECT MAX("batch_id")
                    FROM {self.getPhysBatchMetricsTableName()}
                    WHERE key = :key AND batch_status = :batch_status
                """), {"key": key, "batch_status": BatchStatus.COMMITTED.value})
                lastCompletedBatch = result.fetchone()
                lastCompletedBatch = lastCompletedBatch[0] if lastCompletedBatch and lastCompletedBatch[0] else None
            except Exception as e:
                logger.error("Error getting last completed batch", error=e)
                # Table might not exist yet
                lastCompletedBatch = None

            if lastCompletedBatch is not None:
                # Get job state from last completed batch
                lastState = self.getBatchState(mergeEngine, connection, key, lastCompletedBatch)
                if lastState is not None:
                    state = BatchState(all_datasets=list(self.store.datasets.keys()))
                    state.job_state = lastState.job_state.copy()
                    logger.info("Retrieved job state from last completed batch",
                                last_batch_id=lastCompletedBatch,
                                last_remote_batch_id=state.job_state.get(self.remoteBatchIdKey))
                else:
                    state = BatchState(all_datasets=list(self.store.datasets.keys()))
                    logger.info("No job state found in last completed batch, starting fresh")
            else:
                # Truly initial batch state
                state = BatchState(all_datasets=list(self.store.datasets.keys()))
                logger.info("No previous batches found, starting initial batch")

        # Check for schema changes before ingestion
        self.checkForSchemaChanges(state)

        # Determine if this is a seed batch or incremental
        isSeedBatch: bool = state.job_state.get(self.isSeedBatchKey, True)
        lastRemoteBatchId: Optional[int] = state.job_state.get(self.remoteBatchIdKey, None)

        logger.info("Starting remote forensic merge ingestion",
                    local_batch_id=localBatchId,
                    remote_batch_id=currentRemoteBatchId,
                    is_seed_batch=isSeedBatch,
                    last_remote_batch_id=lastRemoteBatchId,
                    datasets_count=len(state.all_datasets))

        recordsInserted = 0
        totalRecords = 0

        with sourceEngine.connect() as sourceConn:
            while state.hasMoreDatasets():
                datasetToIngestName: str = state.getCurrentDataset()
                dataset: Dataset = self.store.datasets[datasetToIngestName]

                # Get source and staging table names
                sourceTableName: str = self.remoteYDU.getPhysMergeTableNameForDataset(dataset)
                stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)

                # Get schema information
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)
                pkColumns: List[str] = schema.primaryKeyColumns.colNames
                if not pkColumns:
                    pkColumns = [col.name for col in schema.columns.values()]
                allColumns: List[str] = [col.name for col in schema.columns.values()]

                sp: YellowSchemaProjector = self.schemaProjector

                if isSeedBatch:
                    # Seed batch: Get all live records as of current remote batch
                    recordsInserted += self._ingestSeedBatch(
                        sourceConn, mergeEngine, sourceTableName, stagingTableName,
                        allColumns, pkColumns, localBatchId, sp, currentRemoteBatchId)
                else:
                    # Incremental batch: Get changes since last remote batch
                    recordsInserted += self._ingestIncrementalBatch(
                        sourceConn, mergeEngine, sourceTableName, stagingTableName,
                        allColumns, pkColumns, localBatchId, sp, lastRemoteBatchId, currentRemoteBatchId)

                # Move to next dataset
                state.moveToNextDataset()
                if state.hasMoreDatasets():
                    logger.debug("Moving to next dataset", next_dataset=state.getCurrentDataset())
                else:
                    logger.debug("All datasets ingested, setting status to INGESTED")
                    # Update final state before marking as ingested
                    state.job_state[self.remoteBatchIdKey] = currentRemoteBatchId
                    state.job_state[self.isSeedBatchKey] = False  # Next batch will be incremental
                    self.updateBatchStatusInTx(mergeEngine, key, localBatchId, BatchStatus.INGESTED, state=state)

        return recordsInserted, 0, totalRecords

    def _createBatchForRemoteId(self, connection, key: str, remoteBatchId: int) -> bool:
        """Create a batch record using the remote batch ID instead of incrementing local counter.

        This bypasses the normal createBatchCommon logic which increments the local counter.
        Instead, we directly create a batch with the remote batch ID.

        Returns:
            bool: True if processing should continue, False if already up to date
        """
        # Check if this batch already exists
        result = connection.execute(text(f"""
            SELECT "batch_status" FROM {self.getPhysBatchMetricsTableName()}
            WHERE "key" = :key AND "batch_id" = :batch_id
        """), {"key": key, "batch_id": remoteBatchId})
        existing_batch = result.fetchone()

        if existing_batch is not None:
            batch_status = existing_batch[0]
            if batch_status == BatchStatus.COMMITTED.value:
                logger.info("Remote batch already committed, job is up to date",
                            remote_batch_id=remoteBatchId, key=key)
                return False  # Stop processing, already up to date
            else:
                logger.info("Remote batch exists but not committed, will reprocess",
                            remote_batch_id=remoteBatchId, key=key, status=batch_status)
                # Clear staging tables for reprocessing
                self._clearStagingTables(connection)
                return True  # Continue processing
        else:
            # Create initial batch state for remote processing
            if not hasattr(self.store, 'datasets') or not self.store.datasets:
                raise ValueError("Store must have datasets configured for remote merge processing")
            state = BatchState(all_datasets=list(self.store.datasets.keys()))

            # Insert a new batch record with the remote batch ID
            connection.execute(text(f"""
                INSERT INTO {self.getPhysBatchMetricsTableName()}
                ("key", "batch_id", "batch_start_time", "batch_status", "state")
                VALUES (:key, :batch_id, NOW(), :batch_status, :state)
            """), {
                "key": key,
                "batch_id": remoteBatchId,
                "batch_status": BatchStatus.STARTED.value,
                "state": state.model_dump_json()
            })

            # Update the batch counter to reflect the remote batch ID when creating the batch
            self._updateBatchCounter(connection, key, remoteBatchId)

            # Clear staging tables for new batch processing
            self._clearStagingTables(connection)

            logger.info("Created new batch record for remote batch",
                        remote_batch_id=remoteBatchId, key=key)
            return True  # Continue processing

    def _ingestSeedBatch(
            self, sourceConn, mergeEngine: Engine, sourceTableName: str, stagingTableName: str,
            allColumns: List[str], pkColumns: List[str], batchId: int, sp: YellowSchemaProjector,
            currentRemoteBatchId: int) -> int:
        """Ingest ALL records from remote table as of the current remote batch ID for mirror replication."""
        logger.info("Performing mirror seed batch ingestion",
                    source_table=sourceTableName,
                    as_of_remote_batch=currentRemoteBatchId)

        # Query for ALL records that existed as of the current remote batch ID
        # This includes both live and historical records to create a complete mirror
        # We preserve the remote hashes as-is to maintain source system consistency
        quoted_columns = [f'"{col}"' for col in allColumns]
        selectSql = f"""
        SELECT {', '.join(quoted_columns)},
            {sp.ALL_HASH_COLUMN_NAME},
            {sp.KEY_HASH_COLUMN_NAME},
            {sp.BATCH_IN_COLUMN_NAME} as remote_batch_in,
            {sp.BATCH_OUT_COLUMN_NAME} as remote_batch_out
        FROM {sourceTableName}
        WHERE {sp.BATCH_IN_COLUMN_NAME} <= {currentRemoteBatchId}
        """

        logger.debug("Executing mirror seed batch query", query=selectSql)
        result = sourceConn.execute(text(selectSql))
        rows = result.fetchall()
        column_names = result.keys()
        logger.info("Retrieved all records from remote table for mirroring", record_count=len(rows))

        if rows:
            recordsInserted = self._insertRowsToStaging(
                mergeEngine, stagingTableName, rows, allColumns, batchId, sp, column_names)
            logger.info("Mirror seed batch ingestion completed", records_inserted=recordsInserted)
            return recordsInserted

        return 0

    def _ingestIncrementalBatch(
            self, sourceConn, mergeEngine: Engine, sourceTableName: str, stagingTableName: str,
            allColumns: List[str], pkColumns: List[str], batchId: int, sp: YellowSchemaProjector,
            lastRemoteBatchId: Optional[int], currentRemoteBatchId: int) -> int:
        """Ingest only new records since last remote batch for mirror replication."""
        if lastRemoteBatchId is None:
            logger.warning("No last remote batch ID for incremental batch, falling back to seed batch")
            return self._ingestSeedBatch(sourceConn, mergeEngine, sourceTableName, stagingTableName,
                                         allColumns, pkColumns, batchId, sp, currentRemoteBatchId)

        if lastRemoteBatchId >= currentRemoteBatchId:
            logger.info("No new changes in remote table",
                        last_remote_batch=lastRemoteBatchId,
                        current_remote_batch=currentRemoteBatchId)
            return 0

        logger.info("Performing mirror incremental batch ingestion",
                    source_table=sourceTableName,
                    last_remote_batch=lastRemoteBatchId,
                    current_remote_batch=currentRemoteBatchId)

        # Get ALL new records since last batch - both new records and record closures
        # We preserve the remote hashes as-is to maintain source system consistency
        quoted_columns = [f'"{col}"' for col in allColumns]

        selectSql = f"""
        SELECT {', '.join(quoted_columns)},
            {sp.ALL_HASH_COLUMN_NAME},
            {sp.KEY_HASH_COLUMN_NAME},
            {sp.BATCH_IN_COLUMN_NAME} as remote_batch_in,
            {sp.BATCH_OUT_COLUMN_NAME} as remote_batch_out
        FROM {sourceTableName}
        WHERE ({sp.BATCH_IN_COLUMN_NAME} > {lastRemoteBatchId}
               AND {sp.BATCH_IN_COLUMN_NAME} <= {currentRemoteBatchId})
           OR ({sp.BATCH_OUT_COLUMN_NAME} >= {lastRemoteBatchId}
               AND {sp.BATCH_OUT_COLUMN_NAME} <= {currentRemoteBatchId})
        """

        logger.debug("Executing mirror incremental batch query", query=selectSql)
        result = sourceConn.execute(text(selectSql))
        rows = result.fetchall()
        column_names = result.keys()
        logger.info("Retrieved changed records from remote table for mirroring", record_count=len(rows))

        if rows:
            recordsInserted = self._insertRowsToStaging(
                mergeEngine, stagingTableName, rows, allColumns, batchId, sp, column_names)
            logger.info("Mirror incremental batch ingestion completed", records_inserted=recordsInserted)
            return recordsInserted

        return 0

    def _insertRowsToStaging(
            self, mergeEngine: Engine, stagingTableName: str, rows: List[Any],
            allColumns: List[str], batchId: int, sp: YellowSchemaProjector, column_names: List[str]) -> int:
        """Insert rows into staging table with batch metadata and remote batch_in/batch_out."""
        if not rows:
            return 0

        # Create column name mapping for robust data extraction
        column_map: dict[str, int] = {name: idx for idx, name in enumerate(column_names)}

        # Build insert statement - includes remote batch_in/batch_out columns
        quoted_columns = [f'"{col}"' for col in allColumns] + [
            f'"{sp.BATCH_ID_COLUMN_NAME}"',
            f'"{sp.ALL_HASH_COLUMN_NAME}"',
            f'"{sp.KEY_HASH_COLUMN_NAME}"',
            f'"remote_{sp.BATCH_IN_COLUMN_NAME}"',
            f'"remote_{sp.BATCH_OUT_COLUMN_NAME}"'
        ]
        placeholders = ", ".join([f":{i}" for i in range(len(quoted_columns))])
        insertSql = f"INSERT INTO {stagingTableName} ({', '.join(quoted_columns)}) VALUES ({placeholders})"

        recordsInserted = 0
        batchSize = 1000

        with mergeEngine.begin() as mergeConn:
            for i in range(0, len(rows), batchSize):
                batch_rows = rows[i:i + batchSize]
                batchValues: List[List[Any]] = []

                for row in batch_rows:
                    # Extract data columns using column mapping
                    dataValues = []
                    for col in allColumns:
                        if col not in column_map:
                            raise ValueError(f"Expected column '{col}' not found in query result")
                        dataValues.append(row[column_map[col]])

                    # Extract calculated values using column names
                    if sp.ALL_HASH_COLUMN_NAME not in column_map:
                        raise ValueError(f"Expected hash column '{sp.ALL_HASH_COLUMN_NAME}' not found in query result")
                    if sp.KEY_HASH_COLUMN_NAME not in column_map:
                        raise ValueError(f"Expected key hash column '{sp.KEY_HASH_COLUMN_NAME}' not found in query result")
                    if 'remote_batch_in' not in column_map:
                        raise ValueError("Expected 'remote_batch_in' column not found in query result")
                    if 'remote_batch_out' not in column_map:
                        raise ValueError("Expected 'remote_batch_out' column not found in query result")

                    allHash = row[column_map[sp.ALL_HASH_COLUMN_NAME]]
                    keyHash = row[column_map[sp.KEY_HASH_COLUMN_NAME]]
                    remoteBatchIn = row[column_map['remote_batch_in']]
                    remoteBatchOut = row[column_map['remote_batch_out']]
                    # Add batch metadata
                    insertValues = dataValues + [batchId, allHash, keyHash, remoteBatchIn, remoteBatchOut]
                    batchValues.append(insertValues)

                # Execute batch insert
                all_params = [{str(i): val for i, val in enumerate(values)} for values in batchValues]
                mergeConn.execute(text(insertSql), all_params)
                recordsInserted += len(batchValues)

        return recordsInserted

    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, batch_size: int = 10000) -> tuple[int, int, int]:
        """Merge staging data into forensic merge table using forensic milestoning.

        This handles both seed batches (full sync) and incremental batches (delta changes) for forensic tables:
        - Seed batches: Close all existing records and insert new versions from staging
        - Incremental batches: Process specific IUD operations with proper milestoning

        Note: batchId should be the remote batch ID that was processed.
        """
        total_inserted: int = 0
        total_updated: int = 0
        total_deleted: int = 0
        totalRecords: int = 0
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        # The batchId passed in should already be the remote batch ID
        localBatchId = batchId

        with mergeEngine.begin() as connection:
            state: BatchState = self.getBatchState(mergeEngine, connection, key, localBatchId)
            self.checkForSchemaChanges(state)

            # Determine if this is a seed batch or incremental batch
            isSeedBatch: bool = state.job_state.get(self.isSeedBatchKey, True)

            for datasetToMergeName in state.all_datasets:
                dataset: Dataset = self.store.datasets[datasetToMergeName]
                stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)
                mergeTableName: str = self.getPhysMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                allColumns: List[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = [f'"{col}"' for col in allColumns]

                # Get total count for processing
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {localBatchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                if isSeedBatch:
                    logger.debug("Processing forensic seed batch for dataset",
                                 dataset_name=datasetToMergeName,
                                 local_batch_id=localBatchId,
                                 total_records=total_records)

                    # Seed batch: Close all existing records and insert new ones
                    dataset_inserted, dataset_updated, dataset_deleted = self._processForensicSeedBatch(
                        connection, stagingTableName, mergeTableName, allColumns, quoted_all_columns,
                        localBatchId, sp, batch_size)
                else:
                    logger.debug("Processing forensic incremental batch for dataset",
                                 dataset_name=datasetToMergeName,
                                 local_batch_id=localBatchId,
                                 total_records=total_records)

                    # Incremental batch: Process specific IUD operations with forensic logic
                    dataset_inserted, dataset_updated, dataset_deleted = self._processForensicIncrementalBatch(
                        connection, stagingTableName, mergeTableName, allColumns, quoted_all_columns,
                        localBatchId, sp, batch_size)

                total_inserted += dataset_inserted
                total_updated += dataset_updated
                total_deleted += dataset_deleted

                logger.debug("Dataset forensic remote merge results",
                             dataset_name=datasetToMergeName,
                             local_batch_id=localBatchId,
                             inserted=dataset_inserted,
                             updated=dataset_updated,
                             deleted=dataset_deleted)

            # Update the batch status to merged within the existing transaction
            # Use the remote batch ID as the local batch ID in metrics
            self.markBatchMerged(
                connection, key, localBatchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

            # Update the batch counter to reflect the current remote batch ID
            # This keeps the counter table in sync with the metrics table
            self._updateBatchCounter(connection, key, localBatchId)

        logger.info("Total forensic remote merge results",
                    local_batch_id=localBatchId,
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_deleted=total_deleted)
        return total_inserted, total_updated, total_deleted

    def _processForensicSeedBatch(
            self, connection, stagingTableName: str, mergeTableName: str,
            allColumns: List[str], quoted_all_columns: List[str], batchId: int,
            sp: YellowSchemaProjector, batch_size: int) -> tuple[int, int, int]:
        """Process forensic mirror seed batch: Replace all local data with remote mirror."""

        # Step 1: Clear all existing data (we're doing a complete mirror replacement)
        logger.debug("Clearing all existing data for forensic mirror seed batch")
        delete_all_sql = f"DELETE FROM {mergeTableName}"
        delete_result = connection.execute(text(delete_all_sql))
        deleted_count = delete_result.rowcount

        # Step 2: Insert all staging records preserving remote batch_in/batch_out
        logger.debug("Inserting all records from staging preserving remote milestoning")

        # Get total count for batch processing
        count_result = connection.execute(text(f"""
        SELECT COUNT(*) FROM {stagingTableName}
        WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}
        """))
        total_records = count_result.fetchone()[0]

        inserted_count = 0
        # Process in batches for better memory usage
        for offset in range(0, total_records, batch_size):
            batch_insert_sql = f"""
            INSERT INTO {mergeTableName} (
                {', '.join(quoted_all_columns)},
                {sp.ALL_HASH_COLUMN_NAME},
                {sp.KEY_HASH_COLUMN_NAME},
                {sp.BATCH_IN_COLUMN_NAME},
                {sp.BATCH_OUT_COLUMN_NAME}
            )
            SELECT {', '.join([f's."{col}"' for col in allColumns])},
                s.{sp.ALL_HASH_COLUMN_NAME},
                s.{sp.KEY_HASH_COLUMN_NAME},
                s."remote_{sp.BATCH_IN_COLUMN_NAME}",
                CASE
                    WHEN s."remote_{sp.BATCH_OUT_COLUMN_NAME}" = {sp.LIVE_RECORD_ID} THEN {sp.LIVE_RECORD_ID}
                    ELSE s."remote_{sp.BATCH_OUT_COLUMN_NAME}"
                END
            FROM {stagingTableName} s
            WHERE s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
            ORDER BY s.{sp.KEY_HASH_COLUMN_NAME}
            LIMIT {batch_size} OFFSET {offset}
            """

            batch_result = connection.execute(text(batch_insert_sql))
            inserted_count += batch_result.rowcount

        logger.debug("Forensic mirror seed batch processing completed",
                     deleted_count=deleted_count,
                     inserted_count=inserted_count)

        # For mirror seed batch: all existing records were deleted, all remote records were mirrored
        return inserted_count, 0, deleted_count

    def _processForensicIncrementalBatch(
            self, connection, stagingTableName: str, mergeTableName: str,
            allColumns: List[str], quoted_all_columns: List[str], batchId: int,
            sp: YellowSchemaProjector, batch_size: int) -> tuple[int, int, int]:
        """Process forensic mirror incremental batch: Apply remote changes preserving exact milestoning."""

        total_inserted = 0
        total_updated = 0

        # Step 1: Update existing records that were closed remotely
        logger.debug("Updating local records that were closed remotely")
        update_closed_sql = f"""
        UPDATE {mergeTableName} m
        SET {sp.BATCH_OUT_COLUMN_NAME} = s."remote_{sp.BATCH_OUT_COLUMN_NAME}"
        FROM {stagingTableName} s
        WHERE m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
            AND m.{sp.BATCH_IN_COLUMN_NAME} = s."remote_{sp.BATCH_IN_COLUMN_NAME}"
            AND m.{sp.BATCH_OUT_COLUMN_NAME} = {sp.LIVE_RECORD_ID}
            AND s."remote_{sp.BATCH_OUT_COLUMN_NAME}" != {sp.LIVE_RECORD_ID}
            AND s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
        """

        result1 = connection.execute(text(update_closed_sql))
        updated_records = result1.rowcount
        total_updated += updated_records

        # Step 2: Insert new records preserving remote milestoning
        logger.debug("Inserting new records preserving remote milestoning")
        insert_new_sql = f"""
        INSERT INTO {mergeTableName} (
            {', '.join(quoted_all_columns)},
            {sp.ALL_HASH_COLUMN_NAME},
            {sp.KEY_HASH_COLUMN_NAME},
            {sp.BATCH_IN_COLUMN_NAME},
            {sp.BATCH_OUT_COLUMN_NAME}
        )
        SELECT {', '.join([f's."{col}"' for col in allColumns])},
            s.{sp.ALL_HASH_COLUMN_NAME},
            s.{sp.KEY_HASH_COLUMN_NAME},
            s."remote_{sp.BATCH_IN_COLUMN_NAME}",
            CASE
                WHEN s."remote_{sp.BATCH_OUT_COLUMN_NAME}" = {sp.LIVE_RECORD_ID} THEN {sp.LIVE_RECORD_ID}
                ELSE s."remote_{sp.BATCH_OUT_COLUMN_NAME}"
            END
        FROM {stagingTableName} s
        WHERE s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
        AND NOT EXISTS (
            SELECT 1 FROM {mergeTableName} m
            WHERE m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
            AND m.{sp.BATCH_IN_COLUMN_NAME} = s."remote_{sp.BATCH_IN_COLUMN_NAME}"
        )
        """

        result2 = connection.execute(text(insert_new_sql))
        new_records = result2.rowcount
        total_inserted += new_records

        logger.debug("Forensic mirror incremental batch processing completed",
                     new_records=new_records,
                     updated_records=updated_records)

        # No deletions in mirror mode - we preserve all history and only update closure dates
        return total_inserted, total_updated, 0

    def _updateBatchCounter(self, connection, key: str, batchId: int) -> None:
        """Update the batch counter to reflect the current remote batch ID.

        This ensures the counter table stays in sync with the metrics table
        when using remote batch IDs instead of sequential local batch IDs.

        Note: This method assumes it's called within an existing transaction context.
        """
        # First check if a counter record exists
        result = connection.execute(text(f"""
            SELECT "currentBatch" FROM {self.getPhysBatchCounterTableName()}
            WHERE key = :key
        """), {"key": key})
        existing_row = result.fetchone()

        if existing_row is not None:
            # Update existing counter to the remote batch ID
            connection.execute(text(f"""
                UPDATE {self.getPhysBatchCounterTableName()}
                SET "currentBatch" = :batch_id
                WHERE key = :key
            """), {"key": key, "batch_id": batchId})
            logger.debug("Updated batch counter", key=key, batch_id=batchId)
        else:
            # Insert new counter record with remote batch ID
            connection.execute(text(f"""
                INSERT INTO {self.getPhysBatchCounterTableName()} (key, "currentBatch")
                VALUES (:key, :batch_id)
            """), {"key": key, "batch_id": batchId})
            logger.debug("Inserted new batch counter", key=key, batch_id=batchId)

    def _clearStagingTables(self, connection) -> None:
        """Clear all staging tables for this store to prepare for new batch processing.

        This is equivalent to the TRUNCATE TABLE operation in the normal startBatch method.
        """
        for datasetName in self.store.datasets.keys():
            dataset: Dataset = self.store.datasets[datasetName]
            stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)
            connection.execute(text(f"TRUNCATE TABLE {stagingTableName}"))
            logger.debug("Cleared staging table for new batch", table_name=stagingTableName)
