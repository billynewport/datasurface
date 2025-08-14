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
from datasurface.md.types import Integer
from typing import cast, List, Optional, Any
from sqlalchemy import Table, MetaData
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, YellowSchemaProjector,
    BatchStatus, BatchState
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge_remote_live import MergeRemoteJob

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
        self.remoteBatchIdKey = "remote_batch_id"
        self.isSeedBatchKey = "is_seed_batch"

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
        with mergeEngine.begin() as connection:
            state = self.getBatchState(mergeEngine, connection, key, batchId)
            if state is None:
                # Initial batch state.
                state = BatchState(all_datasets=list(self.store.datasets.keys()))

        # Check for schema changes before ingestion
        self.checkForSchemaChanges(state)

        # Determine if this is a seed batch or incremental
        isSeedBatch: bool = state.job_state.get(self.isSeedBatchKey, True)
        lastRemoteBatchId: Optional[int] = state.job_state.get(self.remoteBatchIdKey, None)

        logger.info("Starting remote forensic merge ingestion",
                    batch_id=batchId,
                    is_seed_batch=isSeedBatch,
                    last_remote_batch_id=lastRemoteBatchId,
                    datasets_count=len(state.all_datasets))

        recordsInserted = 0
        totalRecords = 0
        currentRemoteBatchId: Optional[int] = None

        # Get current remote batch ID if not already determined
        if currentRemoteBatchId is None:
            assert self.schemaProjector is not None
            currentRemoteBatchId = self._getCurrentRemoteBatchId(sourceEngine, self.schemaProjector)
            logger.info("Current remote batch ID determined", remote_batch_id=currentRemoteBatchId)

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
                    state.job_state[self.isSeedBatchKey] = False  # Next batch will be incremental
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.INGESTED, state=state)

        return recordsInserted, 0, totalRecords

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
           OR ({sp.BATCH_OUT_COLUMN_NAME} > {lastRemoteBatchId}
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
        column_map = {name: idx for idx, name in enumerate(column_names)}

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
                    dataValues = [row[column_map[col]] for col in allColumns]
                    # Extract calculated values using column names
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
                    logger.debug("Processing forensic seed batch for dataset",
                                 dataset_name=datasetToMergeName,
                                 total_records=total_records)

                    # Seed batch: Close all existing records and insert new ones
                    dataset_inserted, dataset_updated, dataset_deleted = self._processForensicSeedBatch(
                        connection, stagingTableName, mergeTableName, allColumns, quoted_all_columns,
                        batchId, sp, batch_size)
                else:
                    logger.debug("Processing forensic incremental batch for dataset",
                                 dataset_name=datasetToMergeName,
                                 total_records=total_records)

                    # Incremental batch: Process specific IUD operations with forensic logic
                    dataset_inserted, dataset_updated, dataset_deleted = self._processForensicIncrementalBatch(
                        connection, stagingTableName, mergeTableName, allColumns, quoted_all_columns,
                        batchId, sp, batch_size)

                total_inserted += dataset_inserted
                total_updated += dataset_updated
                total_deleted += dataset_deleted

                logger.debug("Dataset forensic remote merge results",
                             dataset_name=datasetToMergeName,
                             inserted=dataset_inserted,
                             updated=dataset_updated,
                             deleted=dataset_deleted)

            # Update the batch status to merged within the existing transaction
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        logger.info("Total forensic remote merge results",
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
                {sp.BATCH_ID_COLUMN_NAME},
                {sp.ALL_HASH_COLUMN_NAME},
                {sp.KEY_HASH_COLUMN_NAME},
                {sp.BATCH_IN_COLUMN_NAME},
                {sp.BATCH_OUT_COLUMN_NAME}
            )
            SELECT {', '.join([f's."{col}"' for col in allColumns])},
                {batchId},
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
            {sp.BATCH_ID_COLUMN_NAME},
            {sp.ALL_HASH_COLUMN_NAME},
            {sp.KEY_HASH_COLUMN_NAME},
            {sp.BATCH_IN_COLUMN_NAME},
            {sp.BATCH_OUT_COLUMN_NAME}
        )
        SELECT {', '.join([f's."{col}"' for col in allColumns])},
            {batchId},
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
