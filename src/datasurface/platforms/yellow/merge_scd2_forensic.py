"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset
)
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus
from typing import cast, List, Optional, Tuple
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, YellowSchemaProjector,
    BatchStatus, BatchState, JobStatus
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge import Job
from sqlalchemy import Table, MetaData
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
from datasurface.md.types import VarChar
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class MergeSCD2ForensicJob(Job):
    """Generic SCD2 forensic merge job driven by IUD staging rows.

    Subclasses should implement `ingestNextBatchToStaging` to populate the staging
    tables for the current `batchId` with rows labeled by an IUD column, where:
      - 'I' means insert a brand new key (no live row exists yet)
      - 'U' means upsert a changed key (close current live version, insert new version)
      - 'D' means delete/close current live version

    This class provides the merge phase that applies I/U/D semantics into forensic
    (SCD2) merge tables using `ds_surf_batch_in`/`ds_surf_batch_out` milestoning.
    """

    def __init__(
            self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
            store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        return self.executeNormalRollingBatch(sourceEngine, mergeEngine, key)

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str, engine: Optional[Engine] = None) -> Table:
        """Return staging schema adding the IUD column used by this merge job.

        Base staging already includes ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash.
        We add a single-character IUD column for operation labeling.
        """
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector
        stagingDataset: Dataset = sp.computeSchema(dataset, YellowSchemaConstants.SCHEMA_TYPE_STAGING, self.merge_db_ops)
        ddlSchema: DDLTable = cast(DDLTable, stagingDataset.originalSchema)
        ddlSchema.add(DDLColumn(name=YellowSchemaConstants.IUD_COLUMN_NAME, data_type=VarChar(maxSize=1), nullable=NullableStatus.NOT_NULLABLE))
        t: Table = datasetToSQLAlchemyTable(stagingDataset, tableName, MetaData(), engine)
        return t

    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, chunkSize: int = 10000) -> Tuple[int, int, int]:
        """Apply IUD-based SCD2 merge from staging to forensic merge tables.

        The merge behavior per operation is:
          - 'D': Close live rows (set batch_out = batchId - 1)
          - 'U': If content changed (all_hash differs), close live row and insert new live version. If no current live row exists, treat as 'I'.
          - 'I': Insert new live version if the key does not exist live
        """
        total_inserted: int = 0
        total_updated: int = 0
        total_deleted: int = 0
        totalRecords: int = 0

        with mergeEngine.begin() as connection:
            state: BatchState = self.getBatchState(mergeEngine, connection, key, batchId)

            # Check for schema changes before merging
            self.checkForSchemaChanges(state)

            for datasetToMergeName in state.all_datasets:
                dataset: Dataset = self.store.datasets[datasetToMergeName]
                stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)
                mergeTableName: str = self.getPhysMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                allColumns: List[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = [f'"{col}"' for col in allColumns]

                # Count staged rows for metrics
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}")
                )
                total_records = count_result.fetchone()[0]
                totalRecords += int(total_records) if total_records else 0

                # 1) Close deletions: D means key is gone -> set current live rows' batch_out to batchId - 1
                close_deleted_sql = f"""
                UPDATE {mergeTableName} m
                SET {YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = {batchId - 1}
                WHERE m.{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = {YellowSchemaConstants.LIVE_RECORD_ID}
                AND EXISTS (
                    SELECT 1 FROM {stagingTableName} s
                    WHERE s.{YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                    AND s.{YellowSchemaConstants.IUD_COLUMN_NAME} = 'D'
                    AND s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                )
                """
                logger.debug("Closing deleted live records", dataset_name=datasetToMergeName)
                result_deleted = connection.execute(text(close_deleted_sql))
                deleted_count = result_deleted.rowcount or 0
                total_deleted += deleted_count

                # 2) Close changed records for 'U' when content differs
                close_changed_sql = f"""
                UPDATE {mergeTableName} m
                SET {YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = {batchId - 1}
                FROM {stagingTableName} s
                WHERE m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                  AND m.{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = {YellowSchemaConstants.LIVE_RECORD_ID}
                  AND s.{YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                  AND s.{YellowSchemaConstants.IUD_COLUMN_NAME} = 'U'
                  AND m.{YellowSchemaConstants.ALL_HASH_COLUMN_NAME} != s.{YellowSchemaConstants.ALL_HASH_COLUMN_NAME}
                """
                logger.debug("Closing changed live records", dataset_name=datasetToMergeName)
                result_changed = connection.execute(text(close_changed_sql))
                changed_count = result_changed.rowcount or 0
                total_updated += changed_count

                # 3) Insert new live versions for 'I' and 'U' where no live version exists (treat 'U' as insert if missing)
                insert_inserts_sql = f"""
                INSERT INTO {mergeTableName} (
                    {', '.join(quoted_all_columns)},
                    {YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
                    {YellowSchemaConstants.KEY_HASH_COLUMN_NAME},
                    {YellowSchemaConstants.BATCH_IN_COLUMN_NAME},
                    {YellowSchemaConstants.BATCH_OUT_COLUMN_NAME}
                )
                SELECT
                    {', '.join([f's."{col}"' for col in allColumns])},
                    s.{YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
                    s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME},
                    {batchId},
                    {YellowSchemaConstants.LIVE_RECORD_ID}
                FROM {stagingTableName} s
                WHERE s.{YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                  AND s.{YellowSchemaConstants.IUD_COLUMN_NAME} IN ('I','U')
                  AND NOT EXISTS (
                      SELECT 1 FROM {mergeTableName} m
                      WHERE m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                        AND m.{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = {YellowSchemaConstants.LIVE_RECORD_ID}
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {mergeTableName} m2
                      WHERE m2.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                        AND m2.{YellowSchemaConstants.BATCH_IN_COLUMN_NAME} = {batchId}
                  )
                """
                logger.debug("Inserting new live records (I/U when no live exists)", dataset_name=datasetToMergeName)
                result_inserts = connection.execute(text(insert_inserts_sql))
                inserted_i_count = result_inserts.rowcount or 0

                # 4) Insert new versions for 'U' that were just closed
                insert_updates_sql = f"""
                INSERT INTO {mergeTableName} (
                    {', '.join(quoted_all_columns)},
                    {YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
                    {YellowSchemaConstants.KEY_HASH_COLUMN_NAME},
                    {YellowSchemaConstants.BATCH_IN_COLUMN_NAME},
                    {YellowSchemaConstants.BATCH_OUT_COLUMN_NAME}
                )
                SELECT
                    {', '.join([f's."{col}"' for col in allColumns])},
                    s.{YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
                    s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME},
                    {batchId},
                    {YellowSchemaConstants.LIVE_RECORD_ID}
                FROM {stagingTableName} s
                WHERE s.{YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batchId}
                  AND s.{YellowSchemaConstants.IUD_COLUMN_NAME} = 'U'
                  AND EXISTS (
                      SELECT 1 FROM {mergeTableName} m
                      WHERE m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                        AND m.{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = {batchId - 1}
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {mergeTableName} m2
                      WHERE m2.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
                        AND m2.{YellowSchemaConstants.BATCH_IN_COLUMN_NAME} = {batchId}
                  )
                """
                logger.debug("Inserting new versions for changed records (U)", dataset_name=datasetToMergeName)
                result_updates = connection.execute(text(insert_updates_sql))
                inserted_u_count = result_updates.rowcount or 0

                total_inserted += inserted_i_count + inserted_u_count

                logger.debug(
                    "Dataset SCD2 IUD merge results",
                    dataset_name=datasetToMergeName,
                    inserted_i=inserted_i_count,
                    inserted_u=inserted_u_count,
                    closed_changed=changed_count,
                    closed_deleted=deleted_count
                )

            # Mark batch committed with totals
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords
            )

        logger.info(
            "Total SCD2 IUD merge results",
            total_inserted=total_inserted,
            total_updated=total_updated,
            total_deleted=total_deleted
        )
        return total_inserted, total_updated, total_deleted

    # Subclasses MUST implement the ingestion and produce IUD-labeled staging data
    def ingestNextBatchToStaging(self, sourceEngine: Engine, mergeEngine: Engine, key: str, batchId: int) -> Tuple[int, int, int]:
        raise NotImplementedError("Subclasses must implement ingestion that populates IUD-labeled staging rows")
