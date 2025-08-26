"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset, DataContainerNamingMapper
)
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datasurface.md.schema import DDLTable
from typing import cast, List, Optional
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform,
    BatchStatus, BatchState
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge import Job, JobStatus
from datasurface.platforms.yellow.merge_forensic import SnapshotDeltaMode
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class SnapshotMergeJobLiveOnly(Job):
    """This job will create a new batch for this ingestion stream. It will then using the batch id, query all records in the source database tables
    and insert them in to the staging table adding the batch id and the hash of every column in the record. The staging table must be created if
    it doesn't exist and altered to match the current schema if necessary when the job starts. The staging table has 3 extra columns, the batch id,
    the all columns hash column called ds_surf_all_hash which is the md5 hash of every column in the record and a ds_surf_key_hash which is the hash of just
    the primary key columns or every column if there are no primary key columns. The operation either does every table in a single transaction
    or loops over each table in its own transaction. This depends on the ingestion mode, single_dataset or multi_dataset. It's understood that
    the transaction reading the source table is different than the one writing to the staging table, they are different connections to different
    databases.
    The select * from table statements can use a mapping of dataset to table name in the SQLIngestion object on the capture metadata of the store.  """

    def __init__(
            self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
            store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        return self.executeNormalRollingBatch(sourceEngine, mergeEngine, key)

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int) -> tuple[int, int, int]:
        return self.baseIngestNextBatchToStaging(sourceEngine, mergeEngine, key, batchId)

    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, chunkSize: int = 10000) -> tuple[int, int, int]:
        return SnapshotMergeJobLiveOnly.genericMergeStagingToMergeAndCommitLive(self, mergeEngine, batchId, key, SnapshotDeltaMode.SNAPSHOT, chunkSize)

    @staticmethod
    def genericMergeStagingToMergeAndCommitLive(job: Job, mergeEngine: Engine, batchId: int, key: str, mode: SnapshotDeltaMode,
                                                chunkSize: int = 10000) -> tuple[int, int, int]:
        """Merge staging data into merge table using either INSERT...ON CONFLICT (default) or PostgreSQL MERGE with DELETE support.

        Args:
            mergeEngine: Database engine for merge operations
            batchId: Current batch ID
            key: Batch key identifier
            chunkSize: Number of records to process per batch (default: 10000)
            use_merge: If True, use PostgreSQL MERGE with DELETE support (requires PostgreSQL 15+)
        """
        use_merge: bool = True
        if use_merge:
            return SnapshotMergeJobLiveOnly._mergeStagingToMergeWithMerge(job, mergeEngine, batchId, key, mode, chunkSize)
        else:
            return SnapshotMergeJobLiveOnly._mergeStagingToMergeWithUpsert(job, mergeEngine, batchId, key, mode, chunkSize)

    @staticmethod
    def _mergeStagingToMergeWithUpsert(job: Job, mergeEngine: Engine, batchId: int, key: str,
                                       mode: SnapshotDeltaMode, chunkSize: int = 10000) -> tuple[int, int, int]:
        """Merge staging data into merge table using INSERT...ON CONFLICT with batched processing for better performance on large datasets.
        This processes merge operations in smaller batches to avoid memory issues while maintaining transactional consistency.
        The entire operation is done in a single transaction to ensure consumers see consistent data."""
        # Initialize metrics variables
        total_inserted: int = 0
        total_updated: int = 0
        total_deleted: int = 0
        totalRecords: int = 0

        with mergeEngine.begin() as connection:
            state: BatchState = job.getBatchState(mergeEngine, connection, key, batchId)

            # Check for schema changes before merging
            job.checkForSchemaChanges(state)

            for datasetToMergeName in state.all_datasets:
                # Get the dataset
                dataset: Dataset = job.store.datasets[datasetToMergeName]
                # Map the dataset name if necessary
                stagingTableName: str = job.getPhysStagingTableNameForDataset(dataset)
                mergeTableName: str = job.getPhysMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                # Get all column names
                allColumns: List[str] = [col.name for col in schema.columns.values()]
                # Get total count for processing
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                logger.debug("Processing records for dataset in batches",
                             dataset_name=datasetToMergeName,
                             total_records=total_records,
                             chunkSize=chunkSize)

                # Process in batches using database-specific upsert operation
                for offset in range(0, total_records, chunkSize):
                    # Create a temporary view for this batch chunk
                    limit_clause = job.merge_db_ops.get_limit_offset_clause(chunkSize, offset)
                    batch_source_table = (f"(SELECT * FROM {stagingTableName} "
                                          f"WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId} "
                                          f"ORDER BY {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} {limit_clause})")

                    batch_upsert_sql = job.merge_db_ops.get_upsert_sql(
                        mergeTableName,
                        batch_source_table,
                        allColumns,
                        YellowSchemaConstants.KEY_HASH_COLUMN_NAME,
                        YellowSchemaConstants.ALL_HASH_COLUMN_NAME,
                        YellowSchemaConstants.BATCH_ID_COLUMN_NAME,
                        batchId
                    )

                    logger.debug("Executing batch upsert", offset=offset)
                    connection.execute(text(batch_upsert_sql))

                # Handle deletions only in SNAPSHOT mode (ignore in UPSERT mode)
                dataset_deleted = 0
                if mode == SnapshotDeltaMode.SNAPSHOT:
                    delete_sql = job.merge_db_ops.get_delete_missing_records_sql(
                        mergeTableName,
                        stagingTableName,
                        YellowSchemaConstants.KEY_HASH_COLUMN_NAME,
                        YellowSchemaConstants.BATCH_ID_COLUMN_NAME,
                        batchId
                    )
                    logger.debug("Executing deletion query for dataset", dataset_name=datasetToMergeName)
                    delete_result = connection.execute(text(delete_sql))
                    dataset_deleted = delete_result.rowcount
                total_deleted += dataset_deleted

                # Get final metrics for this dataset more efficiently
                metrics_sql = f"""
                SELECT
                    COUNT(*) FILTER (WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}) as inserted_count,
                    COUNT(*) FILTER (WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} != {batchId} AND {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} IN (
                        SELECT {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} FROM {stagingTableName}
                            WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                    )) as updated_count
                FROM {mergeTableName}
                WHERE {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} IN (
                    SELECT {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} FROM {stagingTableName} WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                )
                """

                metrics_result = connection.execute(text(metrics_sql))
                metrics_row = metrics_result.fetchone()
                dataset_inserted = metrics_row[0] if metrics_row[0] else 0
                dataset_updated = metrics_row[1] if metrics_row[1] else 0

                total_inserted += dataset_inserted
                total_updated += dataset_updated

                logger.debug("Dataset live-only merge results",
                             dataset_name=datasetToMergeName,
                             inserted=dataset_inserted,
                             updated=dataset_updated,
                             deleted=dataset_deleted)

            # Now update the batch status to merged within the existing transaction
            job.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        logger.info("Total live-only merge results",
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_deleted=total_deleted)
        return total_inserted, total_updated, total_deleted

    @staticmethod
    def _mergeStagingToMergeWithMerge(job: Job, mergeEngine: Engine, batchId: int, key: str,
                                      mode: SnapshotDeltaMode, chunkSize: int = 10000) -> tuple[int, int, int]:
        """Merge staging data into merge table using PostgreSQL MERGE with DELETE support (PostgreSQL 15+).
        This approach handles INSERT, UPDATE, and DELETE operations in a single MERGE statement."""
        # Initialize metrics variables
        total_inserted: int = 0
        total_updated: int = 0
        total_deleted: int = 0
        totalRecords: int = 0
        assert job.schemaProjector is not None

        with mergeEngine.begin() as connection:
            state: BatchState = job.getBatchState(mergeEngine, connection, key, batchId)

            # Check for schema changes before merging
            job.checkForSchemaChanges(state)
            nm: DataContainerNamingMapper = job.dp.psp.namingMapper

            for datasetToMergeName in state.all_datasets:
                # Get the dataset
                dataset: Dataset = job.store.datasets[datasetToMergeName]
                # Map the dataset name if necessary
                stagingTableName: str = job.getPhysStagingTableNameForDataset(dataset)
                mergeTableName: str = job.getPhysMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                # Get all column names
                allColumns: List[str] = [col.name for col in schema.columns.values()]
                # Get total count for processing
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                logger.debug("Processing records for dataset using MERGE with DELETE",
                             dataset_name=datasetToMergeName,
                             total_records=total_records)

                # Create a staging view; include delete operations only in SNAPSHOT mode
                if mode == SnapshotDeltaMode.SNAPSHOT:
                    # Identify records that exist in merge but not in staging (deletions)
                    staging_with_deletes_sql = f"""
                    WITH staging_records AS (
                        SELECT {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} FROM {stagingTableName}
                            WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                    ),
                    merge_records AS (
                        SELECT {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} FROM {mergeTableName}
                    ),
                    records_to_delete AS (
                        SELECT m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                        FROM merge_records m
                        WHERE NOT EXISTS (
                            SELECT 1 FROM staging_records s
                               WHERE s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                        )
                    )
                    SELECT
                        s.*,
                        'INSERT' as operation
                    FROM {stagingTableName} s
                    WHERE s.{YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                    UNION ALL
                    SELECT
                        m.*,
                        'DELETE' as operation
                    FROM {mergeTableName} m
                    INNER JOIN records_to_delete d ON m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = d.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                    """
                else:
                    # UPSERT mode: only inserts/updates, no deletes
                    staging_with_deletes_sql = f"""
                    SELECT
                        s.*,
                        'INSERT' as operation
                    FROM {stagingTableName} s
                    WHERE s.{YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                    """

                # Create temporary staging table with operation column
                temp_staging_table = nm.fmtTVI(f"temp_staging_{batchId}_{datasetToMergeName.replace('-', '_')}")
                create_temp_sql = f"""
                CREATE TEMP TABLE {temp_staging_table} AS {staging_with_deletes_sql}
                """
                connection.execute(text(create_temp_sql))

                # Now use MERGE with DELETE support
                merge_sql = f"""
                MERGE INTO {mergeTableName} m
                USING {temp_staging_table} s
                ON m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
                WHEN MATCHED AND s.operation = 'INSERT' AND m.{YellowSchemaConstants.ALL_HASH_COLUMN_NAME} != s.{YellowSchemaConstants.ALL_HASH_COLUMN_NAME} THEN
                    UPDATE SET
                        {', '.join([f'"{col}" = s."{col}"' for col in allColumns])},
                        {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId},
                        {YellowSchemaConstants.ALL_HASH_COLUMN_NAME} = s.{YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
                        {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                WHEN NOT MATCHED AND s.operation = 'INSERT' THEN
                    INSERT ({', '.join([f'"{col}"' for col in allColumns])}, {YellowSchemaConstants.BATCH_ID_COLUMN_NAME}, {YellowSchemaConstants.ALL_HASH_COLUMN_NAME}, {YellowSchemaConstants.KEY_HASH_COLUMN_NAME})
                    VALUES ({', '.join([f's."{col}"' for col in allColumns])}, {batchId},
                             s.{YellowSchemaConstants.ALL_HASH_COLUMN_NAME}, s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME})
                """

                logger.debug("Executing MERGE with DELETE for dataset", dataset_name=datasetToMergeName)
                connection.execute(text(merge_sql))

                # Get metrics from the MERGE operation
                # Note: PostgreSQL doesn't return row counts from MERGE, so we need to calculate them
                metrics_sql = f"""
                SELECT
                    COUNT(*) FILTER (WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}) as inserted_count,
                    COUNT(*) FILTER (WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} != {batchId} AND {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} IN (
                        SELECT {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} FROM {stagingTableName}
                            WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                    )) as updated_count
                FROM {mergeTableName}
                WHERE {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} IN (
                    SELECT {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} FROM {stagingTableName} WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                )
                """

                metrics_result = connection.execute(text(metrics_sql))
                metrics_row = metrics_result.fetchone()
                dataset_inserted = metrics_row[0] if metrics_row[0] else 0
                dataset_updated = metrics_row[1] if metrics_row[1] else 0

                # For deletions, only count/apply in SNAPSHOT mode
                if mode == SnapshotDeltaMode.SNAPSHOT:
                    deleted_count_sql = f"""
                    SELECT COUNT(*) FROM {mergeTableName} m
                    WHERE m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} NOT IN (
                        SELECT {YellowSchemaConstants.KEY_HASH_COLUMN_NAME} FROM {stagingTableName}
                            WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                    )
                    AND m.{YellowSchemaConstants.BATCH_ID_COLUMN_NAME} != {batchId}
                    """
                    deleted_result = connection.execute(text(deleted_count_sql))
                    dataset_deleted = deleted_result.fetchone()[0]
                else:
                    dataset_deleted = 0

                total_inserted += dataset_inserted
                total_updated += dataset_updated
                total_deleted += dataset_deleted

                logger.debug("Dataset MERGE results",
                             dataset_name=datasetToMergeName,
                             inserted=dataset_inserted,
                             updated=dataset_updated,
                             deleted=dataset_deleted)

            # Now update the batch status to merged within the existing transaction
            job.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        logger.info("Total MERGE operation results",
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_deleted=total_deleted)
        return total_inserted, total_updated, total_deleted
