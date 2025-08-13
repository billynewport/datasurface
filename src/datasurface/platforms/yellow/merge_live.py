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
    YellowDataPlatform, YellowSchemaProjector,
    BatchStatus, BatchState
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge import Job

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

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int) -> tuple[int, int, int]:
        return self.baseIngestNextBatchToStaging(sourceEngine, mergeEngine, key, batchId)

    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, batch_size: int = 10000) -> tuple[int, int, int]:
        """Merge staging data into merge table using either INSERT...ON CONFLICT (default) or PostgreSQL MERGE with DELETE support.

        Args:
            mergeEngine: Database engine for merge operations
            batchId: Current batch ID
            key: Batch key identifier
            batch_size: Number of records to process per batch (default: 10000)
            use_merge: If True, use PostgreSQL MERGE with DELETE support (requires PostgreSQL 15+)
        """
        use_merge: bool = True
        if use_merge:
            return self._mergeStagingToMergeWithMerge(mergeEngine, batchId, key, batch_size)
        else:
            return self._mergeStagingToMergeWithUpsert(mergeEngine, batchId, key, batch_size)

    def _mergeStagingToMergeWithUpsert(self, mergeEngine: Engine, batchId: int, key: str, batch_size: int = 10000) -> tuple[int, int, int]:
        """Merge staging data into merge table using INSERT...ON CONFLICT with batched processing for better performance on large datasets.
        This processes merge operations in smaller batches to avoid memory issues while maintaining transactional consistency.
        The entire operation is done in a single transaction to ensure consumers see consistent data."""
        # Initialize metrics variables
        total_inserted: int = 0
        total_updated: int = 0
        total_deleted: int = 0
        totalRecords: int = 0

        with mergeEngine.begin() as connection:
            state: BatchState = self.getBatchState(mergeEngine, connection, key, batchId)

            # Check for schema changes before merging
            self.checkForSchemaChanges(state)

            for datasetToMergeName in state.all_datasets:
                # Get the dataset
                dataset: Dataset = self.store.datasets[datasetToMergeName]
                # Map the dataset name if necessary
                stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)
                mergeTableName: str = self.getPhysMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                # Get all column names
                allColumns: List[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = [f'"{col}"' for col in allColumns]

                # Get total count for processing
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {self.schemaProjector.BATCH_ID_COLUMN_NAME} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                logger.debug("Processing records for dataset in batches",
                             dataset_name=datasetToMergeName,
                             total_records=total_records,
                             batch_size=batch_size)

                # Process in batches using INSERT...ON CONFLICT for better PostgreSQL compatibility
                for offset in range(0, total_records, batch_size):
                    batch_upsert_sql = f"""
                    WITH batch_data AS (
                        SELECT * FROM {stagingTableName}
                        WHERE {self.schemaProjector.BATCH_ID_COLUMN_NAME} = {batchId}
                        ORDER BY {self.schemaProjector.KEY_HASH_COLUMN_NAME}
                        LIMIT {batch_size} OFFSET {offset}
                    )
                    INSERT INTO {mergeTableName} ({', '.join(quoted_all_columns)}, {self.schemaProjector.BATCH_ID_COLUMN_NAME}, {self.schemaProjector.ALL_HASH_COLUMN_NAME}, {self.schemaProjector.KEY_HASH_COLUMN_NAME})
                    SELECT {', '.join([f'b."{col}"' for col in allColumns])}, {batchId}, b.{self.schemaProjector.ALL_HASH_COLUMN_NAME}, b.{self.schemaProjector.KEY_HASH_COLUMN_NAME}
                    FROM batch_data b
                    ON CONFLICT ({self.schemaProjector.KEY_HASH_COLUMN_NAME})
                    DO UPDATE SET
                        {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in allColumns])},
                        {self.schemaProjector.BATCH_ID_COLUMN_NAME} = EXCLUDED.{self.schemaProjector.BATCH_ID_COLUMN_NAME},
                        {self.schemaProjector.ALL_HASH_COLUMN_NAME} = EXCLUDED.{self.schemaProjector.ALL_HASH_COLUMN_NAME},
                        {self.schemaProjector.KEY_HASH_COLUMN_NAME} = EXCLUDED.{self.schemaProjector.KEY_HASH_COLUMN_NAME}
                    WHERE {mergeTableName}.{self.schemaProjector.ALL_HASH_COLUMN_NAME} != EXCLUDED.{self.schemaProjector.ALL_HASH_COLUMN_NAME}
                    """

                    logger.debug("Executing batch upsert", offset=offset)
                    connection.execute(text(batch_upsert_sql))

                # Handle deletions once after all inserts/updates for this dataset
                delete_sql = f"""
                DELETE FROM {mergeTableName} m
                WHERE NOT EXISTS (
                    SELECT 1 FROM {stagingTableName} s
                    WHERE s.{self.schemaProjector.BATCH_ID_COLUMN_NAME} = {batchId}
                    AND s.{self.schemaProjector.KEY_HASH_COLUMN_NAME} = m.{self.schemaProjector.KEY_HASH_COLUMN_NAME}
                )
                """
                logger.debug("Executing deletion query for dataset", dataset_name=datasetToMergeName)
                delete_result = connection.execute(text(delete_sql))
                dataset_deleted = delete_result.rowcount
                total_deleted += dataset_deleted

                # Get final metrics for this dataset more efficiently
                metrics_sql = f"""
                SELECT
                    COUNT(*) FILTER (WHERE {self.schemaProjector.BATCH_ID_COLUMN_NAME} = {batchId}) as inserted_count,
                    COUNT(*) FILTER (WHERE {self.schemaProjector.BATCH_ID_COLUMN_NAME} != {batchId} AND {self.schemaProjector.KEY_HASH_COLUMN_NAME} IN (
                        SELECT {self.schemaProjector.KEY_HASH_COLUMN_NAME} FROM {stagingTableName} WHERE {self.schemaProjector.BATCH_ID_COLUMN_NAME} = {batchId}
                    )) as updated_count
                FROM {mergeTableName}
                WHERE {self.schemaProjector.KEY_HASH_COLUMN_NAME} IN (
                    SELECT {self.schemaProjector.KEY_HASH_COLUMN_NAME} FROM {stagingTableName} WHERE {self.schemaProjector.BATCH_ID_COLUMN_NAME} = {batchId}
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
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        logger.info("Total live-only merge results",
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_deleted=total_deleted)
        return total_inserted, total_updated, total_deleted

    def _mergeStagingToMergeWithMerge(self, mergeEngine: Engine, batchId: int, key: str, batch_size: int = 10000) -> tuple[int, int, int]:
        """Merge staging data into merge table using PostgreSQL MERGE with DELETE support (PostgreSQL 15+).
        This approach handles INSERT, UPDATE, and DELETE operations in a single MERGE statement."""
        # Initialize metrics variables
        total_inserted: int = 0
        total_updated: int = 0
        total_deleted: int = 0
        totalRecords: int = 0
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        with mergeEngine.begin() as connection:
            state: BatchState = self.getBatchState(mergeEngine, connection, key, batchId)

            # Check for schema changes before merging
            self.checkForSchemaChanges(state)
            nm: DataContainerNamingMapper = self.dp.psp.namingMapper

            for datasetToMergeName in state.all_datasets:
                # Get the dataset
                dataset: Dataset = self.store.datasets[datasetToMergeName]
                # Map the dataset name if necessary
                stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)
                mergeTableName: str = self.getPhysMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                # Get all column names
                allColumns: List[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = [f'"{col}"' for col in allColumns]

                # Get total count for processing
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                logger.debug("Processing records for dataset using MERGE with DELETE",
                             dataset_name=datasetToMergeName,
                             total_records=total_records)

                # Create a staging view that includes records to be deleted
                # We need to identify records that exist in merge but not in staging (deletions)
                staging_with_deletes_sql = f"""
                WITH staging_records AS (
                    SELECT {sp.KEY_HASH_COLUMN_NAME} FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}
                ),
                merge_records AS (
                    SELECT {sp.KEY_HASH_COLUMN_NAME} FROM {mergeTableName}
                ),
                records_to_delete AS (
                    SELECT m.{sp.KEY_HASH_COLUMN_NAME}
                    FROM merge_records m
                    WHERE NOT EXISTS (
                        SELECT 1 FROM staging_records s WHERE s.{sp.KEY_HASH_COLUMN_NAME} = m.{sp.KEY_HASH_COLUMN_NAME}
                    )
                )
                SELECT
                    s.*,
                    'INSERT' as operation
                FROM {stagingTableName} s
                WHERE s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
                UNION ALL
                SELECT
                    m.*,
                    'DELETE' as operation
                FROM {mergeTableName} m
                INNER JOIN records_to_delete d ON m.{sp.KEY_HASH_COLUMN_NAME} = d.{sp.KEY_HASH_COLUMN_NAME}
                """

                # Create temporary staging table with operation column
                temp_staging_table = nm.mapNoun(f"temp_staging_{batchId}_{datasetToMergeName.replace('-', '_')}")
                create_temp_sql = f"""
                CREATE TEMP TABLE {temp_staging_table} AS {staging_with_deletes_sql}
                """
                connection.execute(text(create_temp_sql))

                # Now use MERGE with DELETE support
                merge_sql = f"""
                MERGE INTO {mergeTableName} m
                USING {temp_staging_table} s
                ON m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
                WHEN MATCHED AND s.operation = 'INSERT' AND m.{sp.ALL_HASH_COLUMN_NAME} != s.{sp.ALL_HASH_COLUMN_NAME} THEN
                    UPDATE SET
                        {', '.join([f'"{col}" = s."{col}"' for col in allColumns])},
                        {sp.BATCH_ID_COLUMN_NAME} = {batchId},
                        {sp.ALL_HASH_COLUMN_NAME} = s.{sp.ALL_HASH_COLUMN_NAME},
                        {sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                WHEN NOT MATCHED AND s.operation = 'INSERT' THEN
                    INSERT ({', '.join(quoted_all_columns)}, {sp.BATCH_ID_COLUMN_NAME}, {sp.ALL_HASH_COLUMN_NAME}, {sp.KEY_HASH_COLUMN_NAME})
                    VALUES ({', '.join([f's."{col}"' for col in allColumns])}, {batchId}, s.{sp.ALL_HASH_COLUMN_NAME}, s.{sp.KEY_HASH_COLUMN_NAME})
                """

                logger.debug("Executing MERGE with DELETE for dataset", dataset_name=datasetToMergeName)
                connection.execute(text(merge_sql))

                # Get metrics from the MERGE operation
                # Note: PostgreSQL doesn't return row counts from MERGE, so we need to calculate them
                metrics_sql = f"""
                SELECT
                    COUNT(*) FILTER (WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}) as inserted_count,
                    COUNT(*) FILTER (WHERE {sp.BATCH_ID_COLUMN_NAME} != {batchId} AND {sp.KEY_HASH_COLUMN_NAME} IN (
                        SELECT {sp.KEY_HASH_COLUMN_NAME} FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}
                    )) as updated_count
                FROM {mergeTableName}
                WHERE {sp.KEY_HASH_COLUMN_NAME} IN (
                    SELECT {sp.KEY_HASH_COLUMN_NAME} FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}
                )
                """

                metrics_result = connection.execute(text(metrics_sql))
                metrics_row = metrics_result.fetchone()
                dataset_inserted = metrics_row[0] if metrics_row[0] else 0
                dataset_updated = metrics_row[1] if metrics_row[1] else 0

                # For deletions, count records that were in merge but not in staging
                deleted_count_sql = f"""
                SELECT COUNT(*) FROM {mergeTableName} m
                WHERE m.{sp.KEY_HASH_COLUMN_NAME} NOT IN (
                    SELECT {sp.KEY_HASH_COLUMN_NAME} FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}
                )
                AND m.{sp.BATCH_ID_COLUMN_NAME} != {batchId}
                """
                deleted_result = connection.execute(text(deleted_count_sql))
                dataset_deleted = deleted_result.fetchone()[0]

                total_inserted += dataset_inserted
                total_updated += dataset_updated
                total_deleted += dataset_deleted

                logger.debug("Dataset MERGE results",
                             dataset_name=datasetToMergeName,
                             inserted=dataset_inserted,
                             updated=dataset_updated,
                             deleted=dataset_deleted)

            # Now update the batch status to merged within the existing transaction
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        logger.info("Total MERGE operation results",
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_deleted=total_deleted)
        return total_inserted, total_updated, total_deleted
