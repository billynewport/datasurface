"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset
)
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datasurface.md.schema import DDLTable, DDLColumn, PrimaryKeyList, NullableStatus
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
from datasurface.platforms.yellow.jobs import Job
from datasurface.platforms.yellow.merge_remote_live import (
    IS_SEED_BATCH_KEY, REMOTE_BATCH_ID_KEY
)
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants

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
        stagingDataset: Dataset = sp.computeSchema(dataset, YellowSchemaConstants.SCHEMA_TYPE_STAGING, self.merge_db_ops)

        # Add remote batch columns to preserve original milestoning
        ddlSchema: DDLTable = cast(DDLTable, stagingDataset.originalSchema)
        ddlSchema.add(DDLColumn(name=f"remote_{YellowSchemaConstants.BATCH_IN_COLUMN_NAME}", data_type=Integer(), nullable=NullableStatus.NOT_NULLABLE))
        ddlSchema.add(DDLColumn(name=f"remote_{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME}", data_type=Integer(), nullable=NullableStatus.NOT_NULLABLE))

        # For forensic staging tables, modify the primary key to include remote_batch_in
        # This allows multiple versions of the same record to exist in staging
        if ddlSchema.primaryKeyColumns:
            # Create new primary key with original columns plus remote_batch_in
            new_pk_columns = list(ddlSchema.primaryKeyColumns.colNames) + [f"remote_{YellowSchemaConstants.BATCH_IN_COLUMN_NAME}"]
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
        remoteBatchId: int = self._getHighCommittedRemoteBatchId(sourceEngine, self.schemaProjector)

        # Fail fast if there is no committed remote batch to pull
        if remoteBatchId == 0:
            raise NoopJobException("No committed remote batches found on remote source; cannot run remote forensic sync")

        # Ingest to staging for this remote batch id (method handles early exit if already committed)
        recordsInserted, _, _ = self.ingestNextBatchToStaging(sourceEngine, mergeEngine, key, remoteBatchId)

        # Proceed to merge the staging data into the forensic merge table
        # Even if no records were inserted, we need to mark the batch as committed
        # to indicate we've processed up to this remote batch ID
        # Handle case where dataset might be None (e.g., in tests)
        chunkSize: int = self.getIngestionOverrideValue(self.CHUNK_SIZE_KEY, self.CHUNK_SIZE_DEFAULT)
        self.mergeStagingToMergeAndCommit(mergeEngine, remoteBatchId, key, chunkSize)
        return JobStatus.DONE

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int) -> tuple[int, int, int]:
        """Ingest data from remote forensic merge table to staging.

        First batch is a seed batch that pulls all live records.
        Subsequent batches use milestoning to get only changed records since last remote batch.
        Note: The local batchId will be updated to match the currentRemoteBatchId being processed.
        """
        # Use the provided batchId (remote batch id determined by caller) consistently for this run
        # Avoid re-reading remote batch metrics here to prevent divergence if a new remote batch commits mid-run
        currentRemoteBatchId = batchId
        localBatchId = batchId

        state: Optional[BatchState] = None
        # Get job state from the last completed batch (not the current one we're about to process)
        with mergeEngine.begin() as connection:
            # Create/ensure batch record exists for the remote batch ID
            should_continue = SnapshotMergeJobRemoteForensic._createBatchForRemoteId(self, connection, key, localBatchId)
            if not should_continue:
                # Remote batch already committed, we're up to date
                logger.info("Job already up to date with remote batch, exiting",
                            remote_batch_id=localBatchId, key=key)
                return 0, 0, 0  # No records processed since already up to date
            # Look for the last completed batch to get job state
            # Query batch metrics table for the highest completed batch
            try:
                result = connection.execute(text(f"""
                    SELECT MAX({self.mrgNM.fmtCol("batch_id")})
                    FROM {self.getPhysBatchMetricsTableName()}
                    WHERE {self.mrgNM.fmtCol("key")} = :key AND {self.mrgNM.fmtCol("batch_status")} = :batch_status
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
                                last_remote_batch_id=state.job_state.get(REMOTE_BATCH_ID_KEY))
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
        isSeedBatch: bool = state.job_state.get(IS_SEED_BATCH_KEY, True)
        lastRemoteBatchId: Optional[int] = state.job_state.get(REMOTE_BATCH_ID_KEY, None)

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

                assert self.schemaProjector is not None
                sp: YellowSchemaProjector = self.schemaProjector

                if isSeedBatch:
                    # Seed batch: Get all live records as of current remote batch
                    recordsInserted += SnapshotMergeJobRemoteForensic._ingestSeedBatch(
                        self,
                        sourceConn, mergeEngine, sourceTableName, stagingTableName,
                        allColumns, pkColumns, localBatchId, sp, currentRemoteBatchId)
                else:
                    # Incremental batch: Get changes since last remote batch
                    recordsInserted += SnapshotMergeJobRemoteForensic._ingestIncrementalBatch(
                        self,
                        sourceConn, mergeEngine, sourceTableName, stagingTableName,
                        allColumns, pkColumns, localBatchId, sp, lastRemoteBatchId, currentRemoteBatchId)

                # Move to next dataset
                state.moveToNextDataset()
                if state.hasMoreDatasets():
                    logger.debug("Moving to next dataset", next_dataset=state.getCurrentDataset())
                else:
                    logger.debug("All datasets ingested, setting status to INGESTED")
                    # Update final state before marking as ingested
                    state.job_state[REMOTE_BATCH_ID_KEY] = currentRemoteBatchId
                    state.job_state[IS_SEED_BATCH_KEY] = False  # Next batch will be incremental

                    # Create schema hashes for all datasets
                    # Store the current schema hashes in the batch state
                    schema_versions = {}
                    for dataset_name in state.all_datasets:
                        schema_versions[dataset_name] = self.getSchemaHash(self.store.datasets[dataset_name])
                    state.schema_versions = schema_versions

                    self.updateBatchStatusInTx(mergeEngine, key, localBatchId, BatchStatus.INGESTED, state=state)

        return recordsInserted, 0, totalRecords

    @staticmethod
    def _createBatchForRemoteId(job: Job, connection, key: str, remoteBatchId: int) -> bool:
        """Create a batch record using the remote batch ID instead of incrementing local counter.

        This bypasses the normal createBatchCommon logic which increments the local counter.
        Instead, we directly create a batch with the remote batch ID.

        Returns:
            bool: True if processing should continue, False if already up to date
        """
        # Check if this batch already exists
        # Fetch batch state and batch status
        result = connection.execute(text(f"""
            SELECT {job.mrgNM.fmtCol("batch_status")}, {job.mrgNM.fmtCol("state")} FROM {job.getPhysBatchMetricsTableName()}
            WHERE {job.mrgNM.fmtCol("key")} = :key AND {job.mrgNM.fmtCol("batch_id")} = :batch_id
        """), {"key": key, "batch_id": remoteBatchId})
        existing_batch = result.fetchone()

        if existing_batch is not None:
            batch_status = existing_batch[0]
            batch_state = BatchState.model_validate_json(existing_batch[1])
            if batch_status == BatchStatus.COMMITTED.value:
                logger.info("Remote batch already committed, job is up to date",
                            remote_batch_id=remoteBatchId, key=key)
                return False  # Stop processing, already up to date
            else:
                logger.info("Remote batch exists but not committed, will reprocess",
                            remote_batch_id=remoteBatchId, key=key, status=batch_status)
                # Clear staging records for current batch before reprocessing
                job.truncateStagingTables(connection, batch_state, remoteBatchId)
                return True  # Continue processing
        else:
            # Create initial batch state for remote processing
            if not hasattr(job.store, 'datasets') or not job.store.datasets:
                raise ValueError("Store must have datasets configured for remote merge processing")
            state = BatchState(all_datasets=list(job.store.datasets.keys()))

            # Insert a new batch record with the remote batch ID
            connection.execute(text(f"""
                INSERT INTO {job.getPhysBatchMetricsTableName()}
                (
                    {job.mrgNM.fmtCol("key")}, {job.mrgNM.fmtCol("batch_id")}, {job.mrgNM.fmtCol("batch_start_time")},
                    {job.mrgNM.fmtCol("batch_status")}, {job.mrgNM.fmtCol("state")})
                VALUES (:key, :batch_id, {job.merge_db_ops.get_current_timestamp_expression()}, :batch_status, :state)
            """), {
                "key": key,
                "batch_id": remoteBatchId,
                "batch_status": BatchStatus.STARTED.value,
                "state": state.model_dump_json()
            })

            # Update the batch counter to reflect the remote batch ID when creating the batch
            SnapshotMergeJobRemoteForensic._updateBatchCounter(job, connection, key, remoteBatchId)

            # Clear staging records for current batch before processing
            job.truncateStagingTables(connection, state, remoteBatchId)

            logger.info("Created new batch record for remote batch",
                        remote_batch_id=remoteBatchId, key=key)
            return True  # Continue processing

    @staticmethod
    def _ingestSeedBatch(
            job: Job, sourceConn, mergeEngine: Engine, sourceTableName: str, stagingTableName: str,
            allColumns: List[str], pkColumns: List[str], batchId: int, sp: YellowSchemaProjector,
            currentRemoteBatchId: int) -> int:
        """Ingest ALL records from remote table as of the current remote batch ID for mirror replication."""
        logger.info("Performing mirror seed batch ingestion",
                    source_table=sourceTableName,
                    as_of_remote_batch=currentRemoteBatchId)

        # Query for ALL records that existed as of the current remote batch ID
        # This includes both live and historical records to create a complete mirror
        # We preserve the remote hashes as-is to maintain source system consistency
        quoted_columns = job.source_db_ops.get_quoted_columns(allColumns)
        selectSql = f"""
        SELECT {', '.join(quoted_columns)},
            {job.srcNM.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME)},
            {job.srcNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)},
            {job.srcNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)} as remote_batch_in,
            {job.srcNM.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)} as remote_batch_out
        FROM {sourceTableName}
        WHERE {job.srcNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)} <= {currentRemoteBatchId}
        """

        logger.debug("Executing mirror seed batch query", query=selectSql)
        result = sourceConn.execute(text(selectSql))
        rows = result.fetchall()
        column_names = result.keys()
        logger.info("Retrieved all records from remote table for mirroring", record_count=len(rows))

        if rows:
            recordsInserted = SnapshotMergeJobRemoteForensic._insertRowsToStaging(
                job,
                mergeEngine, stagingTableName, rows, allColumns, batchId, sp, column_names)
            logger.info("Mirror seed batch ingestion completed", records_inserted=recordsInserted)
            return recordsInserted

        return 0

    @staticmethod
    def _ingestIncrementalBatch(
            job: Job, sourceConn, mergeEngine: Engine, sourceTableName: str, stagingTableName: str,
            allColumns: List[str], pkColumns: List[str], batchId: int, sp: YellowSchemaProjector,
            lastRemoteBatchId: Optional[int], currentRemoteBatchId: int) -> int:
        """Ingest only new records since last remote batch for mirror replication."""
        if lastRemoteBatchId is None:
            logger.warning("No last remote batch ID for incremental batch, falling back to seed batch")
            return SnapshotMergeJobRemoteForensic._ingestSeedBatch(
                job, sourceConn, mergeEngine, sourceTableName, stagingTableName,
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
        quoted_columns = job.source_db_ops.get_quoted_columns(allColumns)

        selectSql = f"""
        SELECT {', '.join(quoted_columns)},
            {job.srcNM.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME)},
            {job.srcNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)},
            {job.srcNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)} as remote_batch_in,
            {job.srcNM.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)} as remote_batch_out
        FROM {sourceTableName}
        WHERE ({job.srcNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)} > {lastRemoteBatchId}
               AND {job.srcNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)} <= {currentRemoteBatchId})
           OR ({job.srcNM.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)} >= {lastRemoteBatchId}
               AND {job.srcNM.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)} <= {currentRemoteBatchId})
        """

        logger.debug("Executing mirror incremental batch query", query=selectSql)
        result = sourceConn.execute(text(selectSql))
        rows = result.fetchall()
        column_names = result.keys()
        logger.info("Retrieved changed records from remote table for mirroring", record_count=len(rows))

        if rows:
            recordsInserted = SnapshotMergeJobRemoteForensic._insertRowsToStaging(
                job,
                mergeEngine, stagingTableName, rows, allColumns, batchId, sp, column_names)
            logger.info("Mirror incremental batch ingestion completed", records_inserted=recordsInserted)
            return recordsInserted

        return 0

    @staticmethod
    def _insertRowsToStaging(
            job: Job, mergeEngine: Engine, stagingTableName: str, rows: List[Any],
            allColumns: List[str], batchId: int, sp: YellowSchemaProjector, column_names: List[str]) -> int:
        """Insert rows into staging table with batch metadata and remote batch_in/batch_out."""
        if not rows:
            return 0

        # Create column name mapping for robust data extraction
        column_map: dict[str, int] = {name: idx for idx, name in enumerate(column_names)}

        # Build insert statement - includes remote batch_in/batch_out columns
        quoted_columns = job.merge_db_ops.get_quoted_columns(allColumns) + [
            job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME),
            job.mrgNM.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME),
            job.mrgNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME),
            job.mrgNM.fmtCol(f'remote_{YellowSchemaConstants.BATCH_IN_COLUMN_NAME}'),
            job.mrgNM.fmtCol(f'remote_{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME}')
        ]
        placeholders = ", ".join([f":{i}" for i in range(len(quoted_columns))])
        insertSql = f"INSERT INTO {stagingTableName} ({', '.join(quoted_columns)}) VALUES ({placeholders})"

        recordsInserted = 0
        chunkSize: int = job.getIngestionOverrideValue(job.CHUNK_SIZE_KEY, job.CHUNK_SIZE_DEFAULT)

        with mergeEngine.begin() as mergeConn:
            for i in range(0, len(rows), chunkSize):
                batch_rows = rows[i:i + chunkSize]
                batchValues: List[List[Any]] = []

                for row in batch_rows:
                    # Extract data columns using column mapping
                    dataValues = []
                    for col in allColumns:
                        if col not in column_map:
                            raise ValueError(f"Expected column '{col}' not found in query result")
                        dataValues.append(row[column_map[col]])

                    # Extract calculated values using column names
                    if YellowSchemaConstants.ALL_HASH_COLUMN_NAME not in column_map:
                        raise ValueError(f"Expected hash column '{YellowSchemaConstants.ALL_HASH_COLUMN_NAME}' not found in query result")
                    if YellowSchemaConstants.KEY_HASH_COLUMN_NAME not in column_map:
                        raise ValueError(f"Expected key hash column '{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}' not found in query result")
                    if 'remote_batch_in' not in column_map:
                        raise ValueError("Expected 'remote_batch_in' column not found in query result")
                    if 'remote_batch_out' not in column_map:
                        raise ValueError("Expected 'remote_batch_out' column not found in query result")

                    allHash = row[column_map[YellowSchemaConstants.ALL_HASH_COLUMN_NAME]]
                    keyHash = row[column_map[YellowSchemaConstants.KEY_HASH_COLUMN_NAME]]
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

    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, chunkSize: int = 10000) -> tuple[int, int, int]:
        return SnapshotMergeJobRemoteForensic.genericMergeStagingToMergeAndCommitForensic(self, mergeEngine, batchId, key, chunkSize)

    @staticmethod
    def genericMergeStagingToMergeAndCommitForensic(job: Job, mergeEngine: Engine, batchId: int, key: str, chunkSize: int = 10000) -> tuple[int, int, int]:
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
        assert job.schemaProjector is not None
        sp: YellowSchemaProjector = job.schemaProjector

        # The batchId passed in should already be the remote batch ID
        localBatchId = batchId

        with mergeEngine.begin() as connection:
            state: BatchState = job.getBatchState(mergeEngine, connection, key, localBatchId)
            job.checkForSchemaChanges(state)

            # Determine if this is a seed batch or incremental batch
            isSeedBatch: bool = state.job_state.get(IS_SEED_BATCH_KEY, True)

            for datasetToMergeName in state.all_datasets:
                dataset: Dataset = job.store.datasets[datasetToMergeName]
                stagingTableName: str = job.getPhysStagingTableNameForDataset(dataset)
                mergeTableName: str = job.getPhysMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                allColumns: List[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = job.merge_db_ops.get_quoted_columns(allColumns)

                # Get total count for processing
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME)} = {localBatchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                if isSeedBatch:
                    logger.debug("Processing forensic seed batch for dataset",
                                 dataset_name=datasetToMergeName,
                                 local_batch_id=localBatchId,
                                 total_records=total_records)

                    # Seed batch: Close all existing records and insert new ones
                    dataset_inserted, dataset_updated, dataset_deleted = SnapshotMergeJobRemoteForensic._processForensicSeedBatch(
                        job,
                        connection, stagingTableName, mergeTableName, allColumns, quoted_all_columns,
                        localBatchId, sp, chunkSize)
                else:
                    logger.debug("Processing forensic incremental batch for dataset",
                                 dataset_name=datasetToMergeName,
                                 local_batch_id=localBatchId,
                                 total_records=total_records)

                    # Incremental batch: Process specific IUD operations with forensic logic
                    dataset_inserted, dataset_updated, dataset_deleted = SnapshotMergeJobRemoteForensic._processForensicIncrementalBatch(
                        job,
                        connection, stagingTableName, mergeTableName, allColumns, quoted_all_columns,
                        localBatchId, sp, chunkSize)

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
            job.markBatchMerged(
                connection, key, localBatchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

            # Update the batch counter to reflect the current remote batch ID
            # This keeps the counter table in sync with the metrics table
            SnapshotMergeJobRemoteForensic._updateBatchCounter(job, connection, key, localBatchId)

        logger.info("Total forensic remote merge results",
                    local_batch_id=localBatchId,
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_deleted=total_deleted)
        return total_inserted, total_updated, total_deleted

    @staticmethod
    def _processForensicSeedBatch(
            job: Job, connection, stagingTableName: str, mergeTableName: str,
            allColumns: List[str], quoted_all_columns: List[str], batchId: int,
            sp: YellowSchemaProjector, chunkSize: int) -> tuple[int, int, int]:
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
        WHERE {job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME)} = {batchId}
        """))
        total_records = count_result.fetchone()[0]

        inserted_count = 0
        # Process in batches for better memory usage
        remote_batch_in_column_name = f'"remote_{job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)}"'
        remote_batch_out_column_name = f'"remote_{job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)}"'
        for offset in range(0, total_records, chunkSize):
            batch_insert_sql = f"""
            INSERT INTO {mergeTableName} (
                {', '.join(quoted_all_columns)},
                {job.mrgNM.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME)},
                {job.mrgNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)},
                {job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)},
                {job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)}
            )
            SELECT {', '.join([f's."{col}"' for col in allColumns])},
                s.{YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
                s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME},
                s.{remote_batch_in_column_name},
                CASE
                    WHEN s.{remote_batch_out_column_name} = {YellowSchemaConstants.LIVE_RECORD_ID}
                    THEN {YellowSchemaConstants.LIVE_RECORD_ID}
                    ELSE s.{remote_batch_out_column_name}
                END
            FROM {stagingTableName} s
            WHERE s.{job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME)} = {batchId}
            ORDER BY s.{job.mrgNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)}
            {job.merge_db_ops.get_limit_offset_clause(chunkSize, offset)}
            """

            batch_result = connection.execute(text(batch_insert_sql))
            inserted_count += batch_result.rowcount

        logger.debug("Forensic mirror seed batch processing completed",
                     deleted_count=deleted_count,
                     inserted_count=inserted_count)

        # For mirror seed batch: all existing records were deleted, all remote records were mirrored
        return inserted_count, 0, deleted_count

    @staticmethod
    def _processForensicIncrementalBatch(
            job: Job, connection, stagingTableName: str, mergeTableName: str,
            allColumns: List[str], quoted_all_columns: List[str], batchId: int,
            sp: YellowSchemaProjector, chunkSize: int) -> tuple[int, int, int]:
        """Process forensic mirror incremental batch: Apply remote changes preserving exact milestoning."""

        total_inserted = 0
        total_updated = 0

        # Step 1: Update existing records that were closed remotely
        logger.debug("Updating local records that were closed remotely")
        # Use database-specific UPDATE FROM syntax
        update_closed_sql = job.merge_db_ops.get_remote_forensic_update_closed_sql(
            mergeTableName, stagingTableName, sp, batchId
        )

        result1 = connection.execute(text(update_closed_sql))
        updated_records = result1.rowcount
        total_updated += updated_records

        # Step 2: Insert new records preserving remote milestoning
        logger.debug("Inserting new records preserving remote milestoning")
        remote_batch_in_column_name = job.mrgNM.fmtCol(f'remote_{YellowSchemaConstants.BATCH_IN_COLUMN_NAME}')
        remote_batch_out_column_name = job.mrgNM.fmtCol(f'remote_{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME}')
        insert_new_sql = f"""
        INSERT INTO {mergeTableName} (
            {', '.join(quoted_all_columns)},
            {job.mrgNM.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME)},
            {job.mrgNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)},
            {job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)},
            {job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)}
        )
        SELECT {', '.join([f's."{col}"' for col in allColumns])},
            s.{job.mrgNM.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME)},
            s.{job.mrgNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)},
            s.{remote_batch_in_column_name},
            CASE
                WHEN s.{remote_batch_out_column_name} = {YellowSchemaConstants.LIVE_RECORD_ID}
                THEN {YellowSchemaConstants.LIVE_RECORD_ID}
                ELSE s.{remote_batch_out_column_name}
            END
        FROM {stagingTableName} s
        WHERE s.{job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME)} = {batchId}
        AND NOT EXISTS (
            SELECT 1 FROM {mergeTableName} m
            WHERE m.{job.mrgNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)} = s.{job.mrgNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)}
            AND m.{job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)} = s.{remote_batch_in_column_name}
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

    @staticmethod
    def _updateBatchCounter(job: Job, connection, key: str, batchId: int) -> None:
        """Update the batch counter to reflect the current remote batch ID.

        This ensures the counter table stays in sync with the metrics table
        when using remote batch IDs instead of sequential local batch IDs.

        Note: This method assumes it's called within an existing transaction context.
        """
        # First check if a counter record exists
        result = connection.execute(text(f"""
            SELECT {job.mrgNM.fmtCol("currentBatch")} FROM {job.getPhysBatchCounterTableName()}
            WHERE {job.mrgNM.fmtCol("key")} = :key
        """), {"key": key})
        existing_row = result.fetchone()

        if existing_row is not None:
            # Update existing counter to the remote batch ID
            connection.execute(text(f"""
                UPDATE {job.getPhysBatchCounterTableName()}
                SET {job.mrgNM.fmtCol("currentBatch")} = :batch_id
                WHERE {job.mrgNM.fmtCol("key")} = :key
            """), {"key": key, "batch_id": batchId})
            logger.debug("Updated batch counter", key=key, batch_id=batchId)
        else:
            # Insert new counter record with remote batch ID
            connection.execute(text(f"""
                INSERT INTO {job.getPhysBatchCounterTableName()} ({job.mrgNM.fmtCol("key")}, {job.mrgNM.fmtCol("currentBatch")})
                VALUES (:key, :batch_id)
            """), {"key": key, "batch_id": batchId})
            logger.debug("Inserted new batch counter", key=key, batch_id=batchId)
