"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset
)
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datasurface.md.schema import DDLTable
from typing import cast, Optional
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, YellowSchemaProjector,
    BatchStatus, BatchState
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge import Job, JobStatus


# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class SnapshotMergeJobForensic(Job):
    """This job will create a new batch for this ingestion stream. It will then using the batch id, query all records in the source database tables
    and insert them in to the staging table adding the batch id and the hash of every column in the record. The staging table must be created if
    it doesn't exist and altered to match the current schema if necessary when the job starts. The staging table has 3 extra columns, the batch id,
    the all columns hash column called ds_surf_all_hash which is the md5 hash of every column in the record and a ds_surf_key_hash which is the hash of just
    the primary key columns or every column if there are no primary key columns. The operation either does every table in a single transaction
    or loops over each table in its own transaction. This depends on the ingestion mode, single_dataset or multi_dataset. It's understood that
    """
    def __init__(
            self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
            store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int) -> tuple[int, int, int]:
        """This is the same copy a snapshot from the source table to the staging table as the live only job."""
        return self.baseIngestNextBatchToStaging(sourceEngine, mergeEngine, key, batchId)

    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        return self.executeNormalRollingBatch(sourceEngine, mergeEngine, key)

    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, chunkSize: int = 10000) -> tuple[int, int, int]:
        """
        Perform a forensic merge using a single MERGE statement. All operations (insert, update, delete) are handled in one statement.
        Here is an example showing the correct behavior ingesting some batches of records.
        Batch 1:
            staging: <id1, billy, newport>
            merge: <id1, billy, newport, 1, MaxInt>
        Batch 2:
            staging: <id1, billy, newport>, <id2, laura, diaz>
            merge: {<id1, billy, newport, 1, MaxInt>,<id2,laura,diaz,1, MaxInt>}
        Batch 3:
            staging: <id1, william, newport>, <id2, laura, diaz>
            merge: [<id1, billy, newport, 1, 2><id1,william,newport,3,MaxInt><id3,laura,diaz,2,MaxInt>]
        Batch 4:
            staging: <id2, laura, diaz>
            merge: [<id1, billy, newport, 1, 2><id1,william,newport,3,3><id3,laura,diaz,2,MaxInt>]
        Batch 5:
            staging: <id1, billy, newport><id2,laura,diaz>
            merge: [<id1, billy, newport, 1, 2><id1,william,newport,3,3><id3,laura,diaz,2,MaxInt><id1,billy,newport,5,MaxInt>]
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

            for datasetToMergeName in state.all_datasets:
                dataset: Dataset = self.store.datasets[datasetToMergeName]
                stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)
                mergeTableName: str = self.getPhysMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)
                allColumns: list[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = [f'"{col}"' for col in allColumns]

                # PostgreSQL 16 compatible forensic merge operations - split into multiple statements
                # Step 1: Close changed records (equivalent to WHEN MATCHED AND hash differs)
                close_changed_sql = f"""
                UPDATE {mergeTableName} m
                SET {sp.BATCH_OUT_COLUMN_NAME} = {batchId - 1}
                FROM {stagingTableName} s
                WHERE m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                    AND m.{sp.BATCH_OUT_COLUMN_NAME} = {sp.LIVE_RECORD_ID}
                    AND m.{sp.ALL_HASH_COLUMN_NAME} != s.{sp.ALL_HASH_COLUMN_NAME}
                    AND s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
                """

                # Step 2: Close records not in source (equivalent to WHEN NOT MATCHED BY SOURCE)
                close_deleted_sql = f"""
                UPDATE {mergeTableName} m
                SET {sp.BATCH_OUT_COLUMN_NAME} = {batchId - 1}
                WHERE m.{sp.BATCH_OUT_COLUMN_NAME} = {sp.LIVE_RECORD_ID}
                AND NOT EXISTS (
                    SELECT 1 FROM {stagingTableName} s
                    WHERE s.{sp.KEY_HASH_COLUMN_NAME} = m.{sp.KEY_HASH_COLUMN_NAME}
                    AND s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
                )
                """

                # Step 3: Insert new records (equivalent to WHEN NOT MATCHED BY TARGET)
                insert_new_sql = f"""
                INSERT INTO {mergeTableName} (
                    {', '.join(quoted_all_columns)},
                    {sp.ALL_HASH_COLUMN_NAME},
                    {sp.KEY_HASH_COLUMN_NAME},
                    {sp.BATCH_IN_COLUMN_NAME},
                    {sp.BATCH_OUT_COLUMN_NAME}
                )
                SELECT
                    {', '.join([f's."{col}"' for col in allColumns])},
                    s.{sp.ALL_HASH_COLUMN_NAME},
                    s.{sp.KEY_HASH_COLUMN_NAME},
                    {batchId},
                    {sp.LIVE_RECORD_ID}
                FROM {stagingTableName} s
                WHERE s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
                AND NOT EXISTS (
                    SELECT 1 FROM {mergeTableName} m
                    WHERE m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                    AND m.{sp.BATCH_OUT_COLUMN_NAME} = {sp.LIVE_RECORD_ID}
                )
                """

                logger.debug("Executing PostgreSQL 16 compatible forensic merge for dataset", dataset_name=datasetToMergeName)
                logger.debug("Step 1 - Closing changed records")
                result1 = connection.execute(text(close_changed_sql))
                changed_records = result1.rowcount
                total_updated += changed_records
                logger.debug("Step 1 - Closed changed records", changed_records=changed_records)

                logger.debug("Step 2 - Closing deleted records")
                result2 = connection.execute(text(close_deleted_sql))
                deleted_records = result2.rowcount
                total_deleted += deleted_records
                logger.debug("Step 2 - Closed deleted records", deleted_records=deleted_records)

                logger.debug("Step 3 - Inserting new records")
                result3 = connection.execute(text(insert_new_sql))
                new_records = result3.rowcount
                total_inserted += new_records
                logger.debug("Step 3 - Inserted new records", new_records=new_records)

                # Insert new versions for changed records (where the old record was just closed)
                insert_changed_sql = f"""
                INSERT INTO {mergeTableName} ({', '.join(quoted_all_columns)},
                    {sp.ALL_HASH_COLUMN_NAME}, {sp.KEY_HASH_COLUMN_NAME}, {sp.BATCH_IN_COLUMN_NAME}, {sp.BATCH_OUT_COLUMN_NAME})
                SELECT {', '.join([f's."{col}"' for col in allColumns])}, s.{sp.ALL_HASH_COLUMN_NAME},
                    s.{sp.KEY_HASH_COLUMN_NAME}, {batchId}, {sp.LIVE_RECORD_ID}
                FROM {stagingTableName} s
                WHERE s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
                AND EXISTS (
                    SELECT 1 FROM {mergeTableName} m
                    WHERE m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                    AND m.{sp.BATCH_OUT_COLUMN_NAME} = {batchId - 1}
                )
                AND NOT EXISTS (
                    SELECT 1 FROM {mergeTableName} m2
                    WHERE m2.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                    AND m2.{sp.BATCH_IN_COLUMN_NAME} = {batchId}
                )
                """
                logger.debug("Step 4 - Inserting new versions for changed records")
                result4 = connection.execute(text(insert_changed_sql))
                changed_new_records = result4.rowcount
                total_inserted += changed_new_records
                logger.debug("Step 4 - Inserted new versions for changed records", changed_new_records=changed_new_records)

                # Count total records processed from staging for this dataset
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                logger.debug("Dataset merge results",
                             dataset_name=datasetToMergeName,
                             new_records=new_records,
                             changed_records=changed_records,
                             deleted_records=deleted_records,
                             changed_new_versions=changed_new_records)

            # Now update the batch status to merged within the existing transaction
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        logger.info("Total forensic merge results",
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_deleted=total_deleted)
        return total_inserted, total_updated, total_deleted
