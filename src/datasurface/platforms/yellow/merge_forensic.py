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
    YellowDataPlatform,
    BatchStatus, BatchState
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge import Job, JobStatus
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants
from enum import Enum
# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class SnapshotDeltaMode(Enum):
    """
    This enum is used to determine the mode of the snapshot delta merge job.
    SNAPSHOT: This mode remove any records from the merge table that are not in the source table.
    UPSERT: This is basically an upsert of the source table into the merge table, no deletes.
    """
    SNAPSHOT = "snapshot"
    UPSERT = "upsert"


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
        """This is the same as the normal mergeStagingToMergeAndCommit but it uses the forensic merge statements."""
        return SnapshotMergeJobForensic.genericMergeStagingToMergeAndCommitForensic(self, mergeEngine, batchId, key, SnapshotDeltaMode.SNAPSHOT, chunkSize)

    @staticmethod
    def genericMergeStagingToMergeAndCommitForensic(job: Job, mergeEngine: Engine, batchId: int, key: str,
                                                    mode: SnapshotDeltaMode, chunkSize: int = 10000) -> tuple[int, int, int]:
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

        with mergeEngine.begin() as connection:
            state: BatchState = job.getBatchState(mergeEngine, connection, key, batchId)
            job.checkForSchemaChanges(state)

            for datasetToMergeName in state.all_datasets:
                dataset: Dataset = job.store.datasets[datasetToMergeName]
                stagingTableName: str = job.getPhysStagingTableNameForDataset(dataset)
                mergeTableName: str = job.getPhysMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)
                allColumns: list[str] = [col.name for col in schema.columns.values()]
                # Database-specific forensic merge operations
                forensic_merge_statements = job.merge_db_ops.get_forensic_merge_sql(
                    mergeTableName,
                    stagingTableName,
                    allColumns,
                    job.mrgNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME),
                    job.mrgNM.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME),
                    job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME),
                    job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME),
                    job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME),
                    YellowSchemaConstants.LIVE_RECORD_ID,
                    batchId
                )

                logger.debug("Executing database-specific forensic merge for dataset", dataset_name=datasetToMergeName)

                # Execute forensic merge statements
                changed_records = 0
                deleted_records = 0
                new_records = 0

                for i, sql_statement in enumerate(forensic_merge_statements):
                    # Skip the deletion step (i == 1) in UPSERT mode
                    if mode == SnapshotDeltaMode.UPSERT and i == 1:
                        logger.debug("Skipping deletion step in UPSERT mode")
                        continue
                    logger.debug(f"Step {i+1} - Executing forensic merge statement")
                    result = connection.execute(text(sql_statement))
                    if i == 0:  # Close changed records
                        changed_records = result.rowcount
                        total_updated += changed_records
                        logger.debug("Step 1 - Closed changed records", changed_records=changed_records)
                    elif i == 1:  # Close deleted records
                        deleted_records = result.rowcount
                        total_deleted += deleted_records
                        logger.debug("Step 2 - Closed deleted records", deleted_records=deleted_records)
                    elif i == 2:  # Insert new records
                        new_records = result.rowcount
                        total_inserted += new_records
                        logger.debug("Step 3 - Inserted new records", new_records=new_records)
                    elif i == 3:  # Insert new versions for changed records
                        changed_new_records = result.rowcount
                        total_inserted += changed_new_records
                        logger.debug("Step 4 - Inserted new versions for changed records", changed_new_records=changed_new_records)

                # Count total records processed from staging for this dataset
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {job.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME)} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                logger.debug("Dataset merge results",
                             dataset_name=datasetToMergeName,
                             new_records=new_records,
                             changed_records=changed_records,
                             deleted_records=deleted_records)

            # Now update the batch status to merged within the existing transaction
            job.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        logger.info("Total forensic merge results",
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_deleted=total_deleted)
        return total_inserted, total_updated, total_deleted
