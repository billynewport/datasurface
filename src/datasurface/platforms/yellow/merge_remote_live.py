"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset
)
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datasurface.md.schema import DDLTable, DDLColumn
from datasurface.md.types import VarChar
from typing import cast, List, Optional, Any
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, YellowSchemaProjector,
    BatchStatus, BatchState, YellowDatasetUtilities
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge import Job, JobStatus
from sqlalchemy import Table, MetaData
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
from datasurface.md import SQLMergeIngestion
from datasurface.platforms.yellow.merge import NoopJobException

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class MergeRemoteJob(Job):
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
                 store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)
        self.remoteBatchIdKey = "remote_batch_id"
        self.isSeedBatchKey = "is_seed_batch"
        assert store.cmd is not None
        assert isinstance(store.cmd, SQLMergeIngestion)
        self.remoteDP: YellowDataPlatform = cast(YellowDataPlatform, store.cmd.dataPlatform)
        self.remoteYDU: YellowDatasetUtilities
        if datasetName is not None:
            self.remoteYDU: YellowDatasetUtilities = YellowDatasetUtilities(eco, credStore, self.remoteDP, store, datasetName)
        else:
            self.remoteYDU: YellowDatasetUtilities = YellowDatasetUtilities(eco, credStore, self.remoteDP, store)

    def _getHighCommittedRemoteBatchId(self, sourceEngine: Engine, sp: YellowSchemaProjector) -> int:
        """Fetch the highest committed batch ID from the remote batch metrics table.

        This ensures we only pull committed batches to maintain data consistency.
        Both live and forensic modes should use this approach.
        """
        key: str = self.remoteYDU.getIngestionStreamKey()
        remoteMetricsTableName: str = self.remoteYDU.getPhysBatchMetricsTableName()

        with sourceEngine.connect() as sourceConn:
            # Get the highest committed batch ID
            result = sourceConn.execute(text(f"""
                SELECT MAX(batch_id)
                FROM {remoteMetricsTableName}
                WHERE key = :key AND batch_status = '{BatchStatus.COMMITTED.value}'
            """), {"key": key})

            row = result.fetchone()
            if row is None or row[0] is None:
                # No committed batches found
                return 0
            else:
                return row[0]


class SnapshotMergeJobRemoteLive(MergeRemoteJob):
    """This job will ingest data from a remote forensic merge table. It maintains efficient incremental
    synchronization by:

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

    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        return self.executeNormalRollingBatch(sourceEngine, mergeEngine, key)

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str, engine: Optional[Engine] = None):
        """Override to add IUD column for remote merge ingestion staging tables."""

        # Create a modified dataset with IUD column for staging
        # Add standard staging columns
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector
        stagingDataset: Dataset = sp.computeSchema(dataset, sp.SCHEMA_TYPE_STAGING)
        ddlSchema: DDLTable = cast(DDLTable, stagingDataset.originalSchema)
        # Add IUD column for remote merge operations
        ddlSchema.add(DDLColumn(name=sp.IUD_COLUMN_NAME, data_type=VarChar(maxSize=1)))

        t: Table = datasetToSQLAlchemyTable(stagingDataset, tableName, MetaData(), engine)
        return t

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int) -> tuple[int, int, int]:
        """Ingest data from remote forensic merge table to staging.

        First batch is a seed batch that pulls all live records.
        Subsequent batches use milestoning to get only changed records since last remote batch.
        """
        state: Optional[BatchState] = None
        # Fetch restart state from batch metrics table
        SeedBatch: bool
        with mergeEngine.begin() as connection:
            state = self.getBatchState(mergeEngine, connection, key, batchId-1)  # Get state from previous batch
            if state is None:
                # Initial batch state.
                state = BatchState(all_datasets=list(self.store.datasets.keys()))
                SeedBatch = True
            else:
                # EVery other batch is a delta batch
                SeedBatch = False

        # Check for schema changes before ingestion
        self.checkForSchemaChanges(state)

        # Determine if the previous batch was a seed batch or incremental
        lastRemoteBatchId: Optional[int] = state.job_state.get(self.remoteBatchIdKey, None)

        logger.info("Starting remote merge ingestion",
                    batch_id=batchId,
                    is_seed_batch=SeedBatch,
                    last_remote_batch_id=lastRemoteBatchId,
                    datasets_count=len(state.all_datasets))

        recordsInserted = 0
        totalRecords = 0
        currentRemoteBatchId: Optional[int] = None

        # Get current remote batch ID if not already determined
        if currentRemoteBatchId is None:
            assert self.schemaProjector is not None
            currentRemoteBatchId = self._getHighCommittedRemoteBatchId(sourceEngine, self.schemaProjector)
            logger.info("Current remote batch ID determined", remote_batch_id=currentRemoteBatchId)

        # Fail fast if there is no committed remote batch to pull
        if currentRemoteBatchId == 0:
            raise NoopJobException("No committed remote batches found on remote source; cannot run remote live sync")

        # Fetch all datasets for the batch
        state.reset()
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

                if SeedBatch:
                    # Seed batch: Get all live records as of current remote batch
                    recordsInserted += self._ingestSeedBatch(
                        sourceConn, mergeEngine, sourceTableName, stagingTableName,
                        allColumns, pkColumns, batchId, sp, currentRemoteBatchId)
                else:
                    # Incremental batch: Get changes since last remote batch
                    recordsInserted += self._ingestIncrementalBatch(
                        sourceConn, mergeEngine, sourceTableName, stagingTableName,
                        allColumns, pkColumns, batchId, sp, lastRemoteBatchId, currentRemoteBatchId)

                # Move to next dataset
                state.moveToNextDataset()
                if state.hasMoreDatasets():
                    logger.debug("Moving to next dataset", next_dataset=state.getCurrentDataset())
                else:
                    logger.debug("All datasets ingested, setting status to INGESTED")
                    # Update final state before marking as ingested
                    state.job_state[self.remoteBatchIdKey] = currentRemoteBatchId
                    state.job_state[self.isSeedBatchKey] = SeedBatch  # Whether batch is delta or seed
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.INGESTED, state=state)

        return recordsInserted, 0, totalRecords

    def _ingestSeedBatch(
            self, sourceConn, mergeEngine: Engine, sourceTableName: str, stagingTableName: str,
            allColumns: List[str], pkColumns: List[str], batchId: int, sp: YellowSchemaProjector,
            currentRemoteBatchId: int) -> int:
        """Ingest all live records from remote table as of the current remote batch ID."""
        logger.info("Performing seed batch ingestion",
                    source_table=sourceTableName,
                    as_of_remote_batch=currentRemoteBatchId)

        # Query for records that were live as of the current remote batch ID
        # This means: created by currentRemoteBatchId or earlier AND still alive after currentRemoteBatchId
        # We preserve the remote hashes as-is to maintain source system consistency
        quoted_columns = [f'"{col}"' for col in allColumns]
        selectSql = f"""
        SELECT {', '.join(quoted_columns)},
            {sp.ALL_HASH_COLUMN_NAME},
            {sp.KEY_HASH_COLUMN_NAME},
            'I' as {sp.IUD_COLUMN_NAME}
        FROM {sourceTableName}
        WHERE {sp.BATCH_IN_COLUMN_NAME} <= {currentRemoteBatchId}
        AND {sp.BATCH_OUT_COLUMN_NAME} > {currentRemoteBatchId}
        """

        logger.debug("Executing seed batch query", query=selectSql)
        result = sourceConn.execute(text(selectSql))
        rows = result.fetchall()
        column_names = result.keys()
        logger.info("Retrieved live records from remote table", record_count=len(rows))

        if rows:
            recordsInserted = self._insertRowsToStaging(
                mergeEngine, stagingTableName, rows, allColumns, batchId, sp, column_names)
            logger.info("Seed batch ingestion completed", records_inserted=recordsInserted)
            return recordsInserted

        return 0

    def _ingestIncrementalBatch(
            self, sourceConn, mergeEngine: Engine, sourceTableName: str, stagingTableName: str,
            allColumns: List[str], pkColumns: List[str], batchId: int, sp: YellowSchemaProjector,
            lastRemoteBatchId: Optional[int], currentRemoteBatchId: int) -> int:
        """Ingest only changed records since last remote batch using milestoning."""
        if lastRemoteBatchId is None:
            logger.warning("No last remote batch ID for incremental batch, falling back to seed batch")
            return self._ingestSeedBatch(sourceConn, mergeEngine, sourceTableName, stagingTableName,
                                         allColumns, pkColumns, batchId, sp, currentRemoteBatchId)

        if lastRemoteBatchId >= currentRemoteBatchId:
            logger.info("No new changes in remote table",
                        last_remote_batch=lastRemoteBatchId,
                        current_remote_batch=currentRemoteBatchId)
            return 0

        logger.info("Performing incremental batch ingestion",
                    source_table=sourceTableName,
                    last_remote_batch=lastRemoteBatchId,
                    current_remote_batch=currentRemoteBatchId)

        # Get changes since last batch using milestoning
        # We preserve the remote hashes as-is to maintain source system consistency
        quoted_columns = [f'"{col}"' for col in allColumns]

        # For live-only destination, we need to identify keys that were affected by changes
        # and then get the current state of those keys as of currentRemoteBatchId
        selectSql = f"""
        WITH changed_keys AS (
            -- Find all keys that had any changes between lastRemoteBatchId and currentRemoteBatchId
            SELECT DISTINCT {sp.KEY_HASH_COLUMN_NAME}
            FROM {sourceTableName}
            WHERE ({sp.BATCH_IN_COLUMN_NAME} > {lastRemoteBatchId}
                   AND {sp.BATCH_IN_COLUMN_NAME} <= {currentRemoteBatchId})
               OR ({sp.BATCH_OUT_COLUMN_NAME} >= {lastRemoteBatchId}
                   AND {sp.BATCH_OUT_COLUMN_NAME} <= {currentRemoteBatchId})
        ),
        current_state AS (
            -- Get the current state of changed keys as of currentRemoteBatchId
            SELECT {', '.join(quoted_columns)},
                {sp.ALL_HASH_COLUMN_NAME},
                {sp.KEY_HASH_COLUMN_NAME},
                {sp.BATCH_IN_COLUMN_NAME},
                {sp.BATCH_OUT_COLUMN_NAME}
            FROM {sourceTableName} s
            WHERE s.{sp.KEY_HASH_COLUMN_NAME} IN (SELECT {sp.KEY_HASH_COLUMN_NAME} FROM changed_keys)
              AND s.{sp.BATCH_IN_COLUMN_NAME} <= {currentRemoteBatchId}
              AND s.{sp.BATCH_OUT_COLUMN_NAME} > {currentRemoteBatchId}
        ),
        previous_state AS (
            -- Get the previous state of changed keys as of lastRemoteBatchId
            SELECT {sp.KEY_HASH_COLUMN_NAME},
                {sp.ALL_HASH_COLUMN_NAME} as prev_all_hash
            FROM {sourceTableName} s
            WHERE s.{sp.KEY_HASH_COLUMN_NAME} IN (SELECT {sp.KEY_HASH_COLUMN_NAME} FROM changed_keys)
              AND s.{sp.BATCH_IN_COLUMN_NAME} <= {lastRemoteBatchId}
              AND s.{sp.BATCH_OUT_COLUMN_NAME} >= {lastRemoteBatchId}
        )
        SELECT {', '.join(quoted_columns)},
            cs.{sp.ALL_HASH_COLUMN_NAME},
            cs.{sp.KEY_HASH_COLUMN_NAME},
            CASE
                WHEN ps.{sp.KEY_HASH_COLUMN_NAME} IS NULL THEN 'I'  -- New record
                WHEN cs.{sp.ALL_HASH_COLUMN_NAME} != ps.prev_all_hash THEN 'U'  -- Updated record
                ELSE 'U'  -- Default to update for safety
            END as {sp.IUD_COLUMN_NAME}
        FROM current_state cs
        LEFT JOIN previous_state ps ON cs.{sp.KEY_HASH_COLUMN_NAME} = ps.{sp.KEY_HASH_COLUMN_NAME}
        
        UNION ALL
        
        -- Handle deletions: keys that existed at lastRemoteBatchId but not at currentRemoteBatchId
        SELECT {', '.join([f'ps."{col}"' for col in allColumns])},
            ps.{sp.ALL_HASH_COLUMN_NAME},
            ps.{sp.KEY_HASH_COLUMN_NAME},
            'D' as {sp.IUD_COLUMN_NAME}
        FROM (
            SELECT {', '.join(quoted_columns)},
                {sp.ALL_HASH_COLUMN_NAME},
                {sp.KEY_HASH_COLUMN_NAME}
            FROM {sourceTableName} s
            WHERE s.{sp.KEY_HASH_COLUMN_NAME} IN (SELECT {sp.KEY_HASH_COLUMN_NAME} FROM changed_keys)
              AND s.{sp.BATCH_IN_COLUMN_NAME} <= {lastRemoteBatchId}
              AND s.{sp.BATCH_OUT_COLUMN_NAME} >= {lastRemoteBatchId}
        ) ps
        WHERE ps.{sp.KEY_HASH_COLUMN_NAME} NOT IN (SELECT {sp.KEY_HASH_COLUMN_NAME} FROM current_state)
        """

        logger.debug("Executing incremental batch query", query=selectSql)
        result = sourceConn.execute(text(selectSql))
        rows = result.fetchall()
        column_names = result.keys()
        logger.info("Retrieved changed records from remote table", record_count=len(rows))

        if rows:
            recordsInserted = self._insertRowsToStaging(
                mergeEngine, stagingTableName, rows, allColumns, batchId, sp, column_names)
            logger.info("Incremental batch ingestion completed", records_inserted=recordsInserted)
            return recordsInserted

        return 0

    def _insertRowsToStaging(
            self, mergeEngine: Engine, stagingTableName: str, rows: List[Any],
            allColumns: List[str], batchId: int, sp: YellowSchemaProjector, column_names: List[str]) -> int:
        """Insert rows into staging table with batch metadata."""
        if not rows:
            return 0

        # Create column name mapping for robust data extraction
        column_map: dict[str, int] = {name: idx for idx, name in enumerate(column_names)}

        # Build insert statement
        quoted_columns = [f'"{col}"' for col in allColumns] + [
            f'"{sp.BATCH_ID_COLUMN_NAME}"',
            f'"{sp.ALL_HASH_COLUMN_NAME}"',
            f'"{sp.KEY_HASH_COLUMN_NAME}"',
            f'"{sp.IUD_COLUMN_NAME}"'
        ]
        placeholders = ", ".join([f":{i}" for i in range(len(quoted_columns))])
        insertSql = f"INSERT INTO {stagingTableName} ({', '.join(quoted_columns)}) VALUES ({placeholders})"

        recordsInserted = 0
        chunkSize: int = self.getIngestionOverrideValue(self.CHUNK_SIZE_KEY, self.CHUNK_SIZE_DEFAULT)

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
                    if sp.ALL_HASH_COLUMN_NAME not in column_map:
                        raise ValueError(f"Expected hash column '{sp.ALL_HASH_COLUMN_NAME}' not found in query result")
                    if sp.KEY_HASH_COLUMN_NAME not in column_map:
                        raise ValueError(f"Expected key hash column '{sp.KEY_HASH_COLUMN_NAME}' not found in query result")
                    if sp.IUD_COLUMN_NAME not in column_map:
                        raise ValueError(f"Expected IUD column '{sp.IUD_COLUMN_NAME}' not found in query result")

                    allHash = row[column_map[sp.ALL_HASH_COLUMN_NAME]]
                    keyHash = row[column_map[sp.KEY_HASH_COLUMN_NAME]]
                    iudValue = row[column_map[sp.IUD_COLUMN_NAME]]
                    # Add batch metadata
                    insertValues = dataValues + [batchId, allHash, keyHash, iudValue]
                    batchValues.append(insertValues)

                # Execute batch insert
                all_params = [{str(i): val for i, val in enumerate(values)} for values in batchValues]
                mergeConn.execute(text(insertSql), all_params)
                recordsInserted += len(batchValues)

        return recordsInserted

    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, chunkSize: int = 10000) -> tuple[int, int, int]:
        """Merge staging data into live merge table using context-aware processing.

        This handles both seed batches (full sync) and incremental batches (delta changes) differently:
        - Seed batches: Perform bulk synchronization with the remote state
        - Incremental batches: Process specific IUD operations
        """
        total_inserted: int = 0
        total_updated: int = 0
        total_deleted: int = 0
        totalRecords: int = 0
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        with mergeEngine.begin() as connection:
            state: BatchState = self.getBatchState(mergeEngine, connection, key, batchId)
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
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                if isSeedBatch:
                    logger.debug("Processing seed batch for dataset",
                                 dataset_name=datasetToMergeName,
                                 total_records=total_records)

                    # Seed batch: Replace entire local dataset with remote state
                    dataset_inserted, dataset_updated, dataset_deleted = self._processSeedBatch(
                        connection, stagingTableName, mergeTableName, allColumns, quoted_all_columns,
                        batchId, sp, chunkSize)
                else:
                    logger.debug("Processing incremental batch for dataset",
                                 dataset_name=datasetToMergeName,
                                 total_records=total_records)

                    # Incremental batch: Process specific IUD operations
                    dataset_inserted, dataset_updated, dataset_deleted = self._processIncrementalBatch(
                        connection, stagingTableName, mergeTableName, allColumns, quoted_all_columns,
                        batchId, sp, chunkSize)

                total_inserted += dataset_inserted
                total_updated += dataset_updated
                total_deleted += dataset_deleted

                logger.debug("Dataset remote merge results",
                             dataset_name=datasetToMergeName,
                             inserted=dataset_inserted,
                             updated=dataset_updated,
                             deleted=dataset_deleted)

            # Update the batch status to merged within the existing transaction
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        logger.info("Total remote merge results",
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_deleted=total_deleted)
        return total_inserted, total_updated, total_deleted

    def _processSeedBatch(
            self, connection, stagingTableName: str, mergeTableName: str,
            allColumns: List[str], quoted_all_columns: List[str], batchId: int,
            sp: YellowSchemaProjector, chunkSize: int) -> tuple[int, int, int]:
        """Process seed batch: Replace entire local dataset with remote state.

        This is a full synchronization operation where we:
        1. Clear the local merge table
        2. Insert all records from staging (which represents remote live state)
        """
        # Step 1: Clear the existing merge table for this dataset
        logger.debug("Clearing existing merge table for seed batch")
        delete_all_sql = f"DELETE FROM {mergeTableName}"
        delete_result = connection.execute(text(delete_all_sql))
        deleted_count = delete_result.rowcount

        # Step 2: Insert all staging records (all marked as 'I' for seed batch)
        logger.debug("Inserting all records from staging for seed batch")

        # Get total count for batch processing
        count_result = connection.execute(text(f"""
        SELECT COUNT(*) FROM {stagingTableName}
        WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}
        """))
        total_records = count_result.fetchone()[0]

        inserted_count = 0
        # Process in batches for better memory usage
        for offset in range(0, total_records, chunkSize):
            batch_insert_sql = f"""
            INSERT INTO {mergeTableName} ({', '.join(quoted_all_columns)}, {sp.BATCH_ID_COLUMN_NAME}, {sp.ALL_HASH_COLUMN_NAME}, {sp.KEY_HASH_COLUMN_NAME})
            SELECT {', '.join([f's."{col}"' for col in allColumns])}, {batchId}, s.{sp.ALL_HASH_COLUMN_NAME}, s.{sp.KEY_HASH_COLUMN_NAME}
            FROM {stagingTableName} s
            WHERE s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
            ORDER BY s.{sp.KEY_HASH_COLUMN_NAME}
            LIMIT {chunkSize} OFFSET {offset}
            """

            batch_result = connection.execute(text(batch_insert_sql))
            inserted_count += batch_result.rowcount

        logger.debug("Seed batch processing completed",
                     deleted_count=deleted_count,
                     inserted_count=inserted_count)

        # For seed batch: all existing records were deleted, all staging records were inserted
        # Updated count is 0 since we did full replacement
        return inserted_count, 0, deleted_count

    def _processIncrementalBatch(
            self, connection, stagingTableName: str, mergeTableName: str,
            allColumns: List[str], quoted_all_columns: List[str], batchId: int,
            sp: YellowSchemaProjector, chunkSize: int) -> tuple[int, int, int]:
        """Process incremental batch: Handle specific IUD operations."""
        # Handle Deletes first
        dataset_deleted = self._processDeleteOperations(
            connection, stagingTableName, mergeTableName, batchId, sp)

        # Handle Inserts and Updates
        dataset_inserted, dataset_updated = self._processInsertUpdateOperations(
            connection, stagingTableName, mergeTableName, allColumns, quoted_all_columns,
            batchId, sp, chunkSize)

        return dataset_inserted, dataset_updated, dataset_deleted

    def _processDeleteOperations(
            self, connection, stagingTableName: str, mergeTableName: str,
            batchId: int, sp: YellowSchemaProjector) -> int:
        """Process delete operations from staging table."""
        delete_sql = f"""
        DELETE FROM {mergeTableName} m
        WHERE EXISTS (
            SELECT 1 FROM {stagingTableName} s
            WHERE s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
            AND s.{sp.IUD_COLUMN_NAME} = 'D'
            AND s.{sp.KEY_HASH_COLUMN_NAME} = m.{sp.KEY_HASH_COLUMN_NAME}
        )
        """

        logger.debug("Executing delete operations")
        delete_result = connection.execute(text(delete_sql))
        deleted_count = delete_result.rowcount
        logger.debug("Delete operations completed", deleted_count=deleted_count)
        return deleted_count

    def _processInsertUpdateOperations(
            self, connection, stagingTableName: str, mergeTableName: str,
            allColumns: List[str], quoted_all_columns: List[str], batchId: int,
            sp: YellowSchemaProjector, chunkSize: int) -> tuple[int, int]:
        """Process insert and update operations from staging table in batches."""
        total_inserted = 0
        total_updated = 0

        # Get total count of I/U operations for processing
        count_result = connection.execute(text(f"""
        SELECT COUNT(*) FROM {stagingTableName}
        WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}
        AND {sp.IUD_COLUMN_NAME} IN ('I', 'U')
        """))
        total_operations = count_result.fetchone()[0]

        # Process in batches
        for offset in range(0, total_operations, chunkSize):
            batch_upsert_sql = f"""
            WITH batch_data AS (
                SELECT * FROM {stagingTableName}
                WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}
                AND {sp.IUD_COLUMN_NAME} IN ('I', 'U')
                ORDER BY {sp.KEY_HASH_COLUMN_NAME}
                LIMIT {chunkSize} OFFSET {offset}
            )
            INSERT INTO {mergeTableName} ({', '.join(quoted_all_columns)}, {sp.BATCH_ID_COLUMN_NAME}, {sp.ALL_HASH_COLUMN_NAME}, {sp.KEY_HASH_COLUMN_NAME})
            SELECT {', '.join([f'b."{col}"' for col in allColumns])}, {batchId}, b.{sp.ALL_HASH_COLUMN_NAME}, b.{sp.KEY_HASH_COLUMN_NAME}
            FROM batch_data b
            ON CONFLICT ({sp.KEY_HASH_COLUMN_NAME})
            DO UPDATE SET
                {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in allColumns])},
                {sp.BATCH_ID_COLUMN_NAME} = EXCLUDED.{sp.BATCH_ID_COLUMN_NAME},
                {sp.ALL_HASH_COLUMN_NAME} = EXCLUDED.{sp.ALL_HASH_COLUMN_NAME},
                {sp.KEY_HASH_COLUMN_NAME} = EXCLUDED.{sp.KEY_HASH_COLUMN_NAME}
            WHERE {mergeTableName}.{sp.ALL_HASH_COLUMN_NAME} != EXCLUDED.{sp.ALL_HASH_COLUMN_NAME}
            """

            logger.debug("Executing batch insert/update operations", offset=offset)
            connection.execute(text(batch_upsert_sql))

        # Get final metrics for insert/update operations
        metrics_sql = f"""
        SELECT
            COUNT(*) FILTER (WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}) as current_batch_count,
            COUNT(*) FILTER (WHERE {sp.BATCH_ID_COLUMN_NAME} != {batchId} AND {sp.KEY_HASH_COLUMN_NAME} IN (
                SELECT {sp.KEY_HASH_COLUMN_NAME} FROM {stagingTableName}
                WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId} AND {sp.IUD_COLUMN_NAME} IN ('I', 'U')
            )) as updated_count
        FROM {mergeTableName}
        WHERE {sp.KEY_HASH_COLUMN_NAME} IN (
            SELECT {sp.KEY_HASH_COLUMN_NAME} FROM {stagingTableName}
            WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId} AND {sp.IUD_COLUMN_NAME} IN ('I', 'U')
        )
        """

        metrics_result = connection.execute(text(metrics_sql))
        metrics_row = metrics_result.fetchone()
        inserted_count = metrics_row[0] if metrics_row[0] else 0
        updated_count = metrics_row[1] if metrics_row[1] else 0

        total_inserted += inserted_count
        total_updated += updated_count

        logger.debug("Insert/update operations completed",
                     inserted_count=inserted_count,
                     updated_count=updated_count)

        return total_inserted, total_updated
