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
from typing import Any, cast, Dict, List, Optional, Tuple
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, YellowSchemaProjector,
    BatchStatus, BatchState, JobStatus
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge_scd2_forensic import MergeSCD2ForensicJob
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class SnapshotMergeJobCDCForensic(MergeSCD2ForensicJob):
    """SCD2 forensic merge driven by a CDC table with TX and IUD columns.

    Behavior:
    - Tracks a batch-wide last processed TX ID in batch_state under key 'cdc_last_tx'
    - On each run, computes a batch-wide high TX across all targeted CDC tables
    - Ingests CDC rows with TX in (last_tx, high_tx] for each dataset, conflates by key to the last TX, maps source IUD values to 'I'/'U'/'D'
    - Inserts conflated rows into staging with IUD, hashes, and current batch_id; merge phase provided by base class

    Configuration (via ingestion job hints):
    - cdcTxColumn (default: 'tx_id'): Name of the TX column in CDC table
    - cdcIudColumn (default: 'iud'): Name of the IUD column in CDC table
    - cdcMapInsert (default: 'I'): Source value that maps to 'I'
    - cdcMapUpdate (default: 'U'): Source value that maps to 'U'
    - cdcMapDelete (default: 'D'): Source value that maps to 'D'
    """

    LAST_TX_KEY: str = "cdc_last_tx"
    DEFAULT_TX_COLUMN: str = "tx_id"
    DEFAULT_IUD_COLUMN: str = "iud"
    DEFAULT_MAP_INSERT: str = "I"
    DEFAULT_MAP_UPDATE: str = "U"
    DEFAULT_MAP_DELETE: str = "D"

    def __init__(
            self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
            store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        return self.executeNormalRollingBatch(sourceEngine, mergeEngine, key)

    def _getJobHintKV(self) -> Dict[str, Any]:
        # Mirrors logic in Job.getIngestionOverrideValue to pick dataset-specific hint if available
        if self.dataset is not None:
            jobHint = self.dp.psp.getIngestionJobHint(self.store.name, self.dataset.name)
        else:
            jobHint = self.dp.psp.getIngestionJobHint(self.store.name)
        return jobHint.kv if jobHint is not None else {}

    def _getConfig(self) -> Tuple[str, str, str, str, str]:
        kv = self._getJobHintKV()
        tx_col = cast(str, kv.get("cdcTxColumn", self.DEFAULT_TX_COLUMN))
        iud_col = cast(str, kv.get("cdcIudColumn", self.DEFAULT_IUD_COLUMN))
        map_i = cast(str, kv.get("cdcMapInsert", self.DEFAULT_MAP_INSERT))
        map_u = cast(str, kv.get("cdcMapUpdate", self.DEFAULT_MAP_UPDATE))
        map_d = cast(str, kv.get("cdcMapDelete", self.DEFAULT_MAP_DELETE))
        return tx_col, iud_col, map_i, map_u, map_d

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str, batchId: int) -> Tuple[int, int, int]:
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        tx_col, iud_col, map_i, map_u, map_d = self._getConfig()
        quoted_tx = f'"{tx_col}"'
        quoted_iud = f'"{iud_col}"'

        # Get batch state for last processed TX (from last committed batch if exists)
        with mergeEngine.begin() as connection:
            try:
                result = connection.execute(text(f"""
                    SELECT MAX("batch_id")
                    FROM {self.getPhysBatchMetricsTableName()}
                    WHERE "key" = :key AND batch_status = :batch_status
                """), {"key": key, "batch_status": BatchStatus.COMMITTED.value})
                lastCompletedBatch = result.fetchone()
                lastCompletedBatch = lastCompletedBatch[0] if lastCompletedBatch and lastCompletedBatch[0] else None
            except Exception as e:
                logger.error("Error getting last completed batch", error=e)
                lastCompletedBatch = None

            if lastCompletedBatch is not None:
                lastState = self.getBatchState(mergeEngine, connection, key, lastCompletedBatch)
                state = BatchState(all_datasets=list(self.store.datasets.keys()))
                state.job_state = lastState.job_state.copy()
                logger.info("Retrieved previous CDC state", last_batch_id=lastCompletedBatch)
            else:
                state = BatchState(all_datasets=list(self.store.datasets.keys()))
                logger.info("No previous CDC state found; starting from initial TX")

        # Resolve last_tx (batch-wide)
        raw_last_tx = state.job_state.get(self.LAST_TX_KEY, 0)
        try:
            last_tx: int = int(raw_last_tx)
        except Exception:
            last_tx = 0

        recordsInserted = 0
        totalRecords = 0

        # Compute a batch-wide high TX across all datasets first, to ensure a consistent window
        with sourceEngine.connect() as sourceConn:
            high_tx_global = last_tx
            for datasetName in self.store.datasets.keys():
                dataset = self.store.datasets[datasetName]
                cdcTableName: str = self.getPhysSourceTableName(dataset)
                high_tx_result = sourceConn.execute(text(f"SELECT MAX({quoted_tx}) FROM {cdcTableName}"))
                high_tx_row = high_tx_result.fetchone()
                if high_tx_row and high_tx_row[0] is not None:
                    try:
                        high_tx_global = max(high_tx_global, int(high_tx_row[0]))
                    except Exception:
                        pass

        logger.info("CDC batch window", last_tx=last_tx, high_tx=high_tx_global)

        if high_tx_global <= last_tx:
            # Nothing to ingest; still set state to INGESTED to proceed to merge (which will be a no-op)
            with mergeEngine.begin() as connection:
                state.job_state[self.LAST_TX_KEY] = last_tx
                self.updateBatchStatusInTx(
                    mergeEngine, key, batchId, BatchStatus.INGESTED,
                    state=state, totalRecords=0
                )
            return 0, 0, 0

        # Process per dataset for the consistent window (last_tx, high_tx_global]
        with sourceEngine.connect() as sourceConn:
            with mergeEngine.begin() as mergeConn:
                for datasetName in state.all_datasets:
                    dataset: Dataset = self.store.datasets[datasetName]
                    cdcTableName: str = self.getPhysSourceTableName(dataset)
                    stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)

                    schema: DDLTable = cast(DDLTable, dataset.originalSchema)
                    pkColumns: List[str] = schema.primaryKeyColumns.colNames
                    if not pkColumns:
                        pkColumns = [col.name for col in schema.columns.values()]
                    allColumns: List[str] = [col.name for col in schema.columns.values()]
                    quoted_columns = [f'"{col}"' for col in allColumns]

                    # Use database operations for CDC conflation
                    insert_sql = self.merge_db_ops.get_cdc_conflation_sql(
                        cdcTableName,
                        stagingTableName,
                        allColumns,
                        pkColumns,
                        tx_col,
                        iud_col,
                        YellowSchemaConstants.ALL_HASH_COLUMN_NAME,
                        YellowSchemaConstants.KEY_HASH_COLUMN_NAME,
                        YellowSchemaConstants.BATCH_ID_COLUMN_NAME,
                        batchId,
                        last_tx,
                        high_tx_global,
                        map_i,
                        map_u,
                        map_d
                    )

                    # Note: CDC conflation SQL doesn't use named parameters in the current implementation
                    result = mergeConn.execute(text(insert_sql))
                    inserted = int(result.rowcount or 0)

                    recordsInserted += inserted
                    totalRecords += inserted

        # Persist updated state and mark batch as INGESTED
        with mergeEngine.begin() as connection:
            state.job_state[self.LAST_TX_KEY] = high_tx_global
            self.updateBatchStatusInTx(
                mergeEngine, key, batchId, BatchStatus.INGESTED,
                state=state, totalRecords=totalRecords
            )

        return recordsInserted, 0, totalRecords
