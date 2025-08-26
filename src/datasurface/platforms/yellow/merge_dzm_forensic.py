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


class SnapshotMergeJobDebeziumForensic(MergeSCD2ForensicJob):
    """SCD2 forensic merge driven by Debezium JDBC sink tables (after/before/op/tx).

    Behavior:
    - Tracks a batch-wide last processed TX ID in `batch_state['dzm_last_tx']`
    - Computes a batch-wide high TX across all CDC sink tables
    - Ingests rows where last_tx < tx <= high_tx per dataset, conflates to last row per key using tx (and optional tx_order)
    - Maps Debezium op values to 'I'/'U'/'D' and inserts into staging for SCD2 merge

    Configurable keys (job hints):
    - dzmTxColumn (default: 'tx_id')
    - dzmTxOrderColumn (default: 'tx_order')
    - dzmOpColumn (default: 'op')  # Debezium op codes: 'c','u','d','r'
    - dzmAfterColumn (default: 'after')  # JSON/JSONB with new state
    - dzmBeforeColumn (default: 'before')  # JSON/JSONB with old state (for deletes)
    - dzmMapInsert (default: 'c')
    - dzmMapUpdate (default: 'u')
    - dzmMapDelete (default: 'd')
    """

    LAST_TX_KEY: str = "dzm_last_tx"
    DEFAULT_TX_COLUMN: str = "tx_id"
    DEFAULT_TX_ORDER_COLUMN: str = "tx_order"
    DEFAULT_OP_COLUMN: str = "op"
    DEFAULT_AFTER_COLUMN: str = "after"
    DEFAULT_BEFORE_COLUMN: str = "before"
    DEFAULT_MAP_INSERT: str = "c"
    DEFAULT_MAP_UPDATE: str = "u"
    DEFAULT_MAP_DELETE: str = "d"

    def __init__(
            self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform,
            store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        return self.executeNormalRollingBatch(sourceEngine, mergeEngine, key)

    def _getJobHintKV(self) -> Dict[str, Any]:
        if self.dataset is not None:
            jobHint = self.dp.psp.getIngestionJobHint(self.store.name, self.dataset.name)
        else:
            jobHint = self.dp.psp.getIngestionJobHint(self.store.name)
        return jobHint.kv if jobHint is not None else {}

    def _getConfig(self) -> Tuple[str, Optional[str], str, str, str, str, str, str]:
        kv = self._getJobHintKV()
        tx_col = cast(str, kv.get("dzmTxColumn", self.DEFAULT_TX_COLUMN))
        tx_order_col = cast(Optional[str], kv.get("dzmTxOrderColumn", self.DEFAULT_TX_ORDER_COLUMN))
        op_col = cast(str, kv.get("dzmOpColumn", self.DEFAULT_OP_COLUMN))
        after_col = cast(str, kv.get("dzmAfterColumn", self.DEFAULT_AFTER_COLUMN))
        before_col = cast(str, kv.get("dzmBeforeColumn", self.DEFAULT_BEFORE_COLUMN))
        map_i = cast(str, kv.get("dzmMapInsert", self.DEFAULT_MAP_INSERT))
        map_u = cast(str, kv.get("dzmMapUpdate", self.DEFAULT_MAP_UPDATE))
        map_d = cast(str, kv.get("dzmMapDelete", self.DEFAULT_MAP_DELETE))
        return tx_col, tx_order_col, op_col, after_col, before_col, map_i, map_u, map_d

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str, batchId: int) -> Tuple[int, int, int]:
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        tx_col, tx_order_col, op_col, after_col, before_col, map_i, map_u, map_d = self._getConfig()
        quoted_tx = f'"{tx_col}"'
        quoted_op = f'"{op_col}"'
        quoted_after = f'"{after_col}"'
        quoted_before = f'"{before_col}"'
        quoted_tx_order = f'"{tx_order_col}"' if tx_order_col else None

        # Retrieve prior last_tx from last committed batch
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
                logger.info("Retrieved previous Debezium CDC state", last_batch_id=lastCompletedBatch)
            else:
                state = BatchState(all_datasets=list(self.store.datasets.keys()))
                logger.info("No previous Debezium CDC state found; starting from initial TX")

        # Resolve last_tx (batch-wide)
        raw_last_tx = state.job_state.get(self.LAST_TX_KEY, 0)
        try:
            last_tx: int = int(raw_last_tx)
        except Exception:
            last_tx = 0

        # Compute batch-wide high_tx across all datasets
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

        logger.info("Debezium CDC batch window", last_tx=last_tx, high_tx=high_tx_global)

        if high_tx_global <= last_tx:
            # Nothing to ingest; still set state to INGESTED
            with mergeEngine.begin() as connection:
                state.job_state[self.LAST_TX_KEY] = last_tx
                self.updateBatchStatusInTx(
                    mergeEngine, key, batchId, BatchStatus.INGESTED,
                    state=state, totalRecords=0
                )
            return 0, 0, 0

        recordsInserted = 0
        totalRecords = 0

        # Ingest per dataset for the consistent window (last_tx, high_tx_global]
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

                    # Use database operations for Debezium CDC conflation
                    insert_sql = self.merge_db_ops.get_debezium_cdc_conflation_sql(
                        cdcTableName,
                        stagingTableName,
                        allColumns,
                        pkColumns,
                        tx_col,
                        tx_order_col,
                        op_col,
                        after_col,
                        before_col,
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

                    # Note: Debezium conflation SQL doesn't use named parameters in the current implementation
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
