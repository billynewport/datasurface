"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset, IngestionConsistencyType, PlatformRuntimeHint, HostPortSQLDatabase, SnowFlakeDatabase,
    DataPlatformManagedDataContainer
)
from sqlalchemy import Table, text
import sqlalchemy
from sqlalchemy.engine import Engine, Connection
from datasurface.md.schema import DDLTable
from sqlalchemy.engine.row import Row
from typing import cast, List, Any, Optional, Callable
from datasurface.md.governance import DataContainerNamingMapper, SQLIngestion, DataTransformerOutput
from datasurface.platforms.yellow.db_utils import createEngine
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, BatchStatus, BatchState
)
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable, createOrUpdateTable
from datasurface.platforms.yellow.yellow_dp import YellowDatasetUtilities, JobStatus
from abc import abstractmethod
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger,
)
from sqlalchemy.engine.reflection import Inspector
from datasurface.platforms.yellow.database_operations import DatabaseOperations
from datasurface.platforms.yellow.data_ops_factory import DatabaseOperationsFactory
from datasurface.platforms.yellow.db_utils import createInspector
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


"""
This job runs to ingest/merge new data into a dataset. The data is ingested from a source in to a staging table named
after the dataset. The data is ingested in batches and every ingested record is extended with the batch id that ingested
it as well as key and all column md5 hashs. The hashes improve performance when comparing records for equality or
looking up specific records as it allows a single column to be used rather than the entire record or complete primary
key.

The process is tracked using a metadata table, the batch_status table and a table for storing batch counters. The job ingested the
next batch of data from an ingestion stream and then merges it in to the merge table. Thus, an ingestion stream is a serial
collection of batches. Each batch ingests a chunk of data from the source and then merges it with the corresponding merge table
for the dataset. The process state is tracked in a batch_metrics record keyed to the ingestion stream and the batch id. This record
tracks that the batch has been started, is ingesting, is ready for merge or is complete. Some basic batch metrics are also
stored in the batch_metrics table.

The ingestion streams are named/keyed depending on whather the ingestion stream is for every dataset in a store or for just
one dataset in a datastore. The key is either just {storeName} or '{storeName}#{datasetName}.

The state of the ingestion has the following keys:

- datasets_to_go: A list of outstanding datasets to ingest
- current: tuple of dataset_name and offset. The offset is where ingestion of the dataset should restart.

When the last dataset is ingested, the state is set to MERGING and all datasets are merged in a single tx.

This job is designed to be run by Airflow as a KubernetesPodOperator. It returns:
- Exit code 0: "KEEP_WORKING" - The batch is still in progress, reschedule the job
- Exit code 1: "DONE" - The batch is committed or failed, stop rescheduling
"""


class NoopJobException(Exception):
    """This is a special exception that is used to indicate that the job is a noop and should not be run."""
    pass


class Job(YellowDatasetUtilities):
    """This is the base class for all jobs. The batch counter and batch_metric/state tables are likely to be common across batch implementations. The 2
    step process, stage and then merge is also likely to be common. Some may use external staging but then it's just a noop stage with a merge."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform, store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)
        # Initialize database operations based on merge store type
        assert self.schemaProjector is not None, "Schema projector must be initialized"
        self.merge_db_ops: DatabaseOperations = DatabaseOperationsFactory.create_database_operations(
            dp.psp.mergeStore, self.schemaProjector
        )
        assert self.store.cmd is not None

        # For DataTransformerOutput, data is already in merge database, no external source needed
        if isinstance(self.store.cmd, DataTransformerOutput):
            # Use merge database as source for DataTransformer outputs
            self.source_db_ops: DatabaseOperations = self.merge_db_ops
        else:
            assert self.store.cmd.dataContainer is not None
            assert isinstance(self.store.cmd.dataContainer, (HostPortSQLDatabase, SnowFlakeDatabase))
            self.source_db_ops: DatabaseOperations = DatabaseOperationsFactory.create_database_operations(
                self.store.cmd.dataContainer, self.schemaProjector
            )
        self.numReconcileDDLs: int = 0

        self.srcNM: DataContainerNamingMapper
        if isinstance(self.store.cmd.dataContainer, DataPlatformManagedDataContainer):
            self.srcNM = self.dp.psp.mergeStore.getNamingAdapter()
        else:
            self.srcNM = self.store.cmd.dataContainer.getNamingAdapter()
        self.mrgNM: DataContainerNamingMapper = self.dp.psp.mergeStore.getNamingAdapter()

    def createBatchCounterTable(self, mergeConnection: Connection, inspector: Inspector) -> None:
        """This creates the batch counter table"""
        t: Table = self.getBatchCounterTable()
        createOrUpdateTable(mergeConnection, inspector, t, createOnly=True)

    CHUNK_SIZE_KEY: str = "chunkSize"
    CHUNK_SIZE_DEFAULT: int = 50000

    STAGING_BATCHES_TO_KEEP_KEY: str = "stagingBatchesToKeep"

    def getIngestionOverrideValue(self, key: str, defaultValue: int) -> int:
        """This allows the an runtime parameter to be overridden for a given ingestionstream"""
        # Handle case where dataset might be None (e.g., in tests)
        jobHint: Optional[PlatformRuntimeHint] = None
        if self.dataset is not None:
            jobHint = self.dp.psp.getIngestionJobHint(self.store.name, self.dataset.name)
        else:
            jobHint = self.dp.psp.getIngestionJobHint(self.store.name)

        value: int = defaultValue
        if jobHint is not None:
            value = jobHint.kv.get(key, defaultValue)
            # Log the override
            if self.dataset is not None:
                logger.info(f"Parameter override for {key}", value=value, key=key, store_name=self.store.name, dataset_name=self.dataset.name)
            else:
                logger.info(f"Parameter override for {key}", value=value, key=key, store_name=self.store.name)
        return value

    def createBatchMetricsTable(self, mergeConnection: Connection, inspector: Inspector) -> None:
        """This creates the batch metrics table"""
        t: Table = self.getBatchMetricsTable(mergeConnection)
        createOrUpdateTable(mergeConnection, inspector, t, createOnly=True)

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str, engine: Optional[Engine] = None) -> Table:
        """This returns the staging schema for a dataset"""
        stagingDS: Dataset = self.schemaProjector.computeSchema(dataset, YellowSchemaConstants.SCHEMA_TYPE_STAGING, self.merge_db_ops)
        t: Table = datasetToSQLAlchemyTable(stagingDS, tableName, sqlalchemy.MetaData(), engine)
        return t

    def getMergeSchemaForDataset(self, dataset: Dataset, tableName: str, engine: Optional[Engine] = None) -> Table:
        """This returns the merge schema for a dataset"""
        mergeDS: Dataset = self.schemaProjector.computeSchema(dataset, YellowSchemaConstants.SCHEMA_TYPE_MERGE, self.merge_db_ops)
        t: Table = datasetToSQLAlchemyTable(mergeDS, tableName, sqlalchemy.MetaData(), engine)
        return t

    def createBatchCommon(self, connection: Connection, key: str, state: BatchState) -> int:
        """This creates a batch and returns the batch id. The transaction is managed by the caller.
        This current batch must be commited before a new batch can be created.
        Get the current batch for the store. The first time, there will be no record in the batch counter table.
        In this case, the current batch is 1.
        """
        currentBatchId: int = self.getCurrentBatchId(connection, key)

        # Check if the current batch is committed
        result = connection.execute(text(
            f'SELECT "batch_status" FROM {self.getPhysBatchMetricsTableName()} WHERE "key" = :key AND "batch_id" = :batch_id'
        ), {"key": key, "batch_id": currentBatchId})
        batchStatusRow: Optional[Row[Any]] = result.fetchone()
        if batchStatusRow is not None:
            batchStatus: str = batchStatusRow[0]
            if batchStatus != BatchStatus.COMMITTED.value:
                raise Exception(f"Batch {currentBatchId} is not committed")
            newBatch = currentBatchId + 1
            connection.execute(text(
                f'UPDATE {self.getPhysBatchCounterTableName()} SET "currentBatch" = :new_batch WHERE "key" = :key'
            ), {"new_batch": newBatch, "key": key})
        else:
            newBatch = 1

        # Insert a new batch event record with started status
        connection.execute(text(
            f"INSERT INTO {self.getPhysBatchMetricsTableName()} "
            f'("key", "batch_id", "batch_start_time", "batch_status", "state") '
            f"VALUES (:key, :batch_id, {self.merge_db_ops.get_current_timestamp_expression()}, :batch_status, :state)"
        ), {
            "key": key,
            "batch_id": newBatch,
            "batch_status": BatchStatus.STARTED.value,
            "state": state.model_dump_json()
        })

        return newBatch

    def createSingleBatch(self, store: Datastore, connection: Connection) -> int:
        """This creates a single-dataset batch and returns the batch id. The transaction is managed by the caller."""
        assert self.dataset is not None
        key: str = self.getIngestionStreamKey()
        # Create schema hashes for the dataset
        schema_versions = {self.dataset.name: self.getSchemaHash(self.dataset)}
        state: BatchState = BatchState(all_datasets=[self.dataset.name], schema_versions=schema_versions)  # Just one dataset to ingest, start at offset 0
        return self.createBatchCommon(connection, key, state)

    def createMultiBatch(self, store: Datastore, connection: Connection) -> int:
        """This create a multi-dataset batch and returns the batch id. The transaction is managed by the caller.
        This current batch must be commited before a new batch can be created.
        Get the current batch for the store. The first time, there will be no record in the batch counter table.
        In this case, the current batch is 0.
        """
        key: str = self.getIngestionStreamKey()
        allDatasets: List[str] = list(store.datasets.keys())

        # Create schema hashes for all datasets
        schema_versions = {}
        for dataset_name in allDatasets:
            dataset = store.datasets[dataset_name]
            schema_versions[dataset_name] = self.getSchemaHash(dataset)

        # Start with the first dataset and the rest of the datasets to go
        state: BatchState = BatchState(all_datasets=allDatasets, schema_versions=schema_versions)  # Start with the first dataset
        return self.createBatchCommon(connection, key, state)

    def truncateStagingTables(self, mergeConnection: Connection, state: BatchState, batchId: int) -> None:
        """This truncates the staging tables for each dataset in the batch state"""
        for datasetName in state.all_datasets:
            dataset: Dataset = self.store.datasets[datasetName]
            stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)
            batchesToKeep: int = self.getIngestionOverrideValue(self.STAGING_BATCHES_TO_KEEP_KEY, self.dp.stagingBatchesToKeep)
            if batchesToKeep < 0:
                # Delete all records for batch id in staging
                # Use BATCH_ID_COLUMN_NAME from the schema projector
                mergeConnection.execute(
                    text((
                        f"DELETE FROM {stagingTableName} "
                        f"WHERE {self.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME)} = :batch_id"
                    )), {"batch_id": batchId})
            else:
                minBatchToKeep: int = batchId - batchesToKeep
                mergeConnection.execute(
                    text((
                        f"DELETE FROM {stagingTableName} "
                        f"WHERE {self.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME)} < :min_batch_to_keep"
                    )), {"min_batch_to_keep": minBatchToKeep})

    def startBatch(self, mergeEngine: Engine) -> int:
        """This starts a new batch. If the current batch is not committed, it will raise an exception. A existing batch must be restarted."""
        # Start a new transaction
        newBatchId: int
        with mergeEngine.begin() as connection:
            # Create a new batch
            assert self.store.cmd is not None
            if self.store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET:
                assert self.dataset is not None
                newBatchId = self.createSingleBatch(self.store, connection)
            else:
                newBatchId = self.createMultiBatch(self.store, connection)
            # Grab batch state from the batch metrics table
            state: BatchState = self.getBatchState(mergeEngine, connection, self.getIngestionStreamKey(), newBatchId)
            # Truncate the staging table for each dataset in the batch state
            self.truncateStagingTables(connection, state, newBatchId)
        return newBatchId

    def getBatchState(self, mergeEngine: Engine, connection: Connection, key: str, batchId: int) -> BatchState:
        """Get the batch status for a given key and batch id"""
        result = connection.execute(text(
            f'SELECT "state" FROM {self.getPhysBatchMetricsTableName()} WHERE "key" = :key AND "batch_id" = :batch_id'
        ), {"key": key, "batch_id": batchId})
        row = result.fetchone()
        if row is not None:
            return BatchState.model_validate_json(row[0])
        else:
            return BatchState(all_datasets=list(self.store.datasets.keys()))

    def checkBatchStatus(self, connection: Connection, key: str, batchId: int) -> Optional[str]:
        """Check the current batch status for a given key. Returns the status or None if no batch exists."""
        result = connection.execute(text(f"""
            SELECT bm."batch_status"
            FROM {self.getPhysBatchMetricsTableName()} bm
            WHERE bm."key" = :key AND bm."batch_id" = :batch_id
        """), {"key": key, "batch_id": batchId})
        row = result.fetchone()
        return row[0] if row else None

    def getCurrentBatchId(self, connection: Connection, key: str) -> int:
        """Get the current batch ID for a given key, create a new batch counter if one doesn't exist."""

        # Now query the table
        result = connection.execute(text(
            f'SELECT "currentBatch" FROM {self.getPhysBatchCounterTableName()} WHERE "key" = :key'
        ), {"key": key})
        row = result.fetchone()
        if row is None:
            # Insert a new batch counter
            connection.execute(text(
                f'INSERT INTO {self.getPhysBatchCounterTableName()} ("key", "currentBatch") VALUES (:key, :current_batch)'
            ), {"key": key, "current_batch": 1})
            return 1
        else:
            return row[0]

    def updateBatchStatusInTx(
            self, mergeEngine: Engine, key: str, batchId: int, status: BatchStatus,
            recordsInserted: Optional[int] = None, recordsUpdated: Optional[int] = None,
            recordsDeleted: Optional[int] = None, totalRecords: Optional[int] = None,
            state: Optional[BatchState] = None) -> None:
        """Update the batch status and metrics in a transaction"""
        nm: DataContainerNamingMapper = self.mrgNM
        with mergeEngine.begin() as connection:
            update_parts = [f"{nm.fmtCol('batch_status')} = :batch_status"]
            update_params = {"batch_status": status.value, "key": key, "batch_id": batchId}

            if status in [BatchStatus.COMMITTED, BatchStatus.FAILED]:
                update_parts.append(f"{nm.fmtCol('batch_end_time')} = {self.merge_db_ops.get_current_timestamp_expression()}")
            if recordsInserted is not None:
                update_parts.append(f"{nm.fmtCol('records_inserted')} = :records_inserted")
                update_params["records_inserted"] = recordsInserted
            if recordsUpdated is not None:
                update_parts.append(f"{nm.fmtCol('records_updated')} = :records_updated")
                update_params["records_updated"] = recordsUpdated
            if recordsDeleted is not None:
                update_parts.append(f"{nm.fmtCol('records_deleted')} = :records_deleted")
                update_params["records_deleted"] = recordsDeleted
            if totalRecords is not None:
                update_parts.append(f"{nm.fmtCol('total_records')} = :total_records")
                update_params["total_records"] = totalRecords
            if state is not None:
                update_parts.append(f"{nm.fmtCol('state')} = :state")
                update_params["state"] = state.model_dump_json()

            update_sql = (
                f'UPDATE {self.getPhysBatchMetricsTableName()} SET {", ".join(update_parts)} '
                f'WHERE {nm.fmtCol("key")} = :key AND {nm.fmtCol("batch_id")} = :batch_id'
            )
            connection.execute(text(update_sql), update_params)

    def markBatchMerged(self, connection: Connection, key: str, batchId: int, status: BatchStatus,
                        recordsInserted: Optional[int] = None, recordsUpdated: Optional[int] = None,
                        recordsDeleted: Optional[int] = None, totalRecords: Optional[int] = None) -> None:
        """Mark the batch as merged"""
        nm: DataContainerNamingMapper = self.mrgNM
        update_parts = [f"{nm.fmtCol('batch_status')} = '{status.value}'"]
        if status in [BatchStatus.COMMITTED, BatchStatus.FAILED]:
            update_parts.append(f"{nm.fmtCol('batch_end_time')} = {self.merge_db_ops.get_current_timestamp_expression()}")
        if recordsInserted is not None:
            update_parts.append(f"{nm.fmtCol('records_inserted')} = {recordsInserted}")
        if recordsUpdated is not None:
            update_parts.append(f"{nm.fmtCol('records_updated')} = {recordsUpdated}")
        if recordsDeleted is not None:
            update_parts.append(f"{nm.fmtCol('records_deleted')} = {recordsDeleted}")
        if totalRecords is not None:
            update_parts.append(f"{nm.fmtCol('total_records')} = {totalRecords}")

        update_sql = (
            f'UPDATE {self.getPhysBatchMetricsTableName()} SET '
            f'{", ".join(update_parts)} WHERE {nm.fmtCol("key")} = \'{key}\' AND {nm.fmtCol("batch_id")} = {batchId}'
        )
        connection.execute(text(update_sql))

    @abstractmethod
    def ingestNextBatchToStaging(self, sourceEngine: Engine, mergeEngine: Engine, key: str, batchId: int) -> tuple[int, int, int]:
        """This will ingest the next batch to staging"""
        pass

    @abstractmethod
    def mergeStagingToMergeAndCommit(self, mergeEngine: Engine, batchId: int, key: str, chunkSize: int = 10000) -> tuple[int, int, int]:
        """This will merge the staging table to the merge table"""
        pass

    @abstractmethod
    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        raise NoopJobException("This method must be implemented by the subclass")

    def getMaxCommittedBatchId(self, connection: Connection, key: str) -> Optional[int]:
        """Get the max committed batch id for a given key"""
        result = connection.execute(text(f"""
            SELECT MAX({self.mrgNM.fmtCol("batch_id")})
            FROM {self.getPhysBatchMetricsTableName()}
            WHERE {self.mrgNM.fmtCol("key")} = :key AND {self.mrgNM.fmtCol("batch_status")} = :batch_status
        """), {"key": key, "batch_status": BatchStatus.COMMITTED.value})
        row = result.fetchone()
        return row[0] if row else None

    def executeNormalRollingBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        """This executes an ingestion/merge batch. The logic is as follows:
        If there is no current batch, i.e. initial batch or the last batch is committed then
            start one and keep going.
        If there is an open batch then
            reset the batch.
        Now, we have an open batch ready to be ingested.
        TX: Ingest the batch to staging tables.
        TX: Merge the staging in to the merge tables.
        TX: Commit the batch."""
        """This is used by all ingestion types except remote forensic for now."""
        with mergeEngine.begin() as connection:
            batchId: int = self.getCurrentBatchId(connection, key)
            currentStatus = self.checkBatchStatus(connection, key, batchId)

        if currentStatus is None or currentStatus == BatchStatus.COMMITTED.value:
            # No batch exists, start a new one
            batchId = self.startBatch(mergeEngine)
            logger.info("Started new batch", batch_id=batchId, key=key)
            # Batch is started, continue with ingestion
            with mergeEngine.begin() as connection:
                batchId = self.getCurrentBatchId(connection, key)
                currentStatus = self.checkBatchStatus(connection, key, batchId)
            logger.info("Continuing batch ingestion", batch_id=batchId, key=key, status=currentStatus)

        if currentStatus == BatchStatus.STARTED.value:
            # Ingest the batch to staging tables if new batch.
            logger.info("Continuing batch ingestion", batch_id=batchId, key=key, status=currentStatus)
            self.ingestNextBatchToStaging(sourceEngine, mergeEngine, key, batchId)

        with mergeEngine.begin() as connection:
            currentStatus = self.checkBatchStatus(connection, key, batchId)

        chunkSize: int = self.getIngestionOverrideValue(self.CHUNK_SIZE_KEY, self.CHUNK_SIZE_DEFAULT)

        if currentStatus == BatchStatus.INGESTED.value:
            # Merge the staging in to the merge tables.
            logger.info("Continuing batch merge", batch_id=batchId, key=key, status=currentStatus)
            self.mergeStagingToMergeAndCommit(mergeEngine, batchId, key, chunkSize)
        return JobStatus.DONE

    def run(self) -> JobStatus:
        # Check if this is a DataTransformer output store
        isDataTransformerOutput = isinstance(self.store.cmd, DataTransformerOutput)

        # Now, get a connection to the merge database
        mergeUser, mergePassword = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.mergeRW_Credential)
        mergeEngine: Engine = createEngine(self.dp.psp.mergeStore, mergeUser, mergePassword)

        ingestionType: IngestionConsistencyType = IngestionConsistencyType.MULTI_DATASET
        if isDataTransformerOutput:
            # For DataTransformer output, source and merge are the same (merge database)
            sourceEngine: Engine = mergeEngine
            ingestionType = IngestionConsistencyType.MULTI_DATASET
        elif isinstance(self.store.cmd, SQLIngestion):
            # First, get a connection to the source database
            cmd: SQLIngestion = cast(SQLIngestion, self.store.cmd)
            assert cmd.credential is not None

            # Now, get an Engine for the source database
            sourceUser, sourcePassword = self.credStore.getAsUserPassword(cmd.credential)
            assert self.store.cmd.dataContainer is not None
            assert isinstance(self.store.cmd.dataContainer, (HostPortSQLDatabase, SnowFlakeDatabase))
            sourceEngine: Engine = createEngine(self.store.cmd.dataContainer, sourceUser, sourcePassword)
            assert cmd.singleOrMultiDatasetIngestion is not None
            ingestionType = cmd.singleOrMultiDatasetIngestion
        else:
            raise Exception(f"Unknown store command type: {type(self.store.cmd)}")

        # Check current batch status to determine what to do
        if ingestionType == IngestionConsistencyType.SINGLE_DATASET:
            # For single dataset ingestion, process each dataset separately
            ydu: YellowDatasetUtilities = YellowDatasetUtilities(self.eco, self.credStore, self.dp, self.store, self.dataset.name)
        else:
            # For multi-dataset ingestion, process all datasets in a single batch
            ydu: YellowDatasetUtilities = YellowDatasetUtilities(self.eco, self.credStore, self.dp, self.store)
        key: str = ydu.getIngestionStreamKey()

        # Create batch counter and metrics tables if they don't exist
        inspector = createInspector(mergeEngine)

        with mergeEngine.begin() as mergeConnection:
            self.createBatchCounterTable(mergeConnection, inspector)
            self.createBatchMetricsTable(mergeConnection, inspector)
            # Make sure the staging and merge tables exist and have the current schema for each dataset
            # WE only want to do this if the schema hash for the schemas we care about are different than the schema hash in the previous batch
            # Need to get the max committed batch id if any
            maxCommittedBatchId: Optional[int] = self.getMaxCommittedBatchId(mergeConnection, key)
            if maxCommittedBatchId is None:
                currentState: BatchState = BatchState(all_datasets=list(self.store.datasets.keys()))
            else:
                currentState: BatchState = self.getBatchState(mergeEngine, mergeConnection, key, maxCommittedBatchId)
            reconcileNeeded: bool = False
            for dataset in self.store.datasets.values():
                if dataset.name not in currentState.schema_versions or currentState.schema_versions[dataset.name] != ydu.getSchemaHash(dataset):
                    reconcileNeeded = True
                    break
            if reconcileNeeded:
                logger.info("Reconciling staging and merge tables", key=key)
                self.numReconcileDDLs += 1
                self.reconcileStagingTableSchemas(mergeConnection, inspector, self.store)
                self.reconcileMergeTableSchemas(mergeConnection, inspector, self.store)
                logger.info("Reconciling staging and merge tables completed", key=key)

        currentStatus = self.executeBatch(sourceEngine, mergeEngine, key)
        return currentStatus

    def getPhysSourceTableName(self, dataset: Dataset) -> str:
        """This returns the source table name for a dataset"""
        if isinstance(self.store.cmd, DataTransformerOutput):
            return self.getPhysDataTransformerOutputTableNameForDatasetForIngestionOnly(self.store, dataset)
        elif isinstance(self.store.cmd, SQLIngestion):
            if dataset.name not in self.store.cmd.tableForDataset:
                tableName: str = dataset.name
            else:
                tableName: str = self.store.cmd.tableForDataset[dataset.name]
            return tableName
        else:
            raise Exception(f"Unknown store command type: {type(self.store.cmd)}")

    def baseIngestSingleDataset(
            self, state: BatchState, sourceConn: Connection, mergeEngine: Engine, key: str, batchId: int,
            sql_generator: Callable[[str, List[str], List[str]], str]) -> tuple[int, int, int]:
        # Get source table name, Map the dataset name if necessary
        datasetToIngestName: str = state.getCurrentDataset()
        dataset: Dataset = self.store.datasets[datasetToIngestName]

        # Get source table name - for DataTransformer output, use dt_ prefixed tables
        if isinstance(self.store.cmd, DataTransformerOutput):
            sourceTableName: str = self.getPhysDataTransformerOutputTableNameForDatasetForIngestionOnly(self.store, dataset)
        else:
            # Get source table name using mapping if necessary
            sourceTableName: str = self.getPhysSourceTableName(dataset)

        # Get destination staging table name
        stagingTableName: str = self.getPhysStagingTableNameForDataset(dataset)

        # Get primary key columns
        schema: DDLTable = cast(DDLTable, dataset.originalSchema)
        pkColumns: List[str] = schema.primaryKeyColumns.colNames
        if not pkColumns:
            # If no primary key, use all columns
            pkColumns = [col.name for col in schema.columns.values()]

        # Get all column names
        allColumns: List[str] = [col.name for col in schema.columns.values()]

        # Hash expressions are now built within the database operations

        # This job will for a new batch, initialize the state with all the datasets in the store and then
        # remove one with an offset of 0 to start. The job will read a "chunk" from that source table and
        # write it to the staging table, updating the offset in the state as it goes. When all data is
        # written to the staging table for that dataset, the job will set the state to the next dataset.
        # When all datasets are done then the job will set the batch status to ingested. The transistion is start
        # means we're ingesting the data for all datasets.

        # Execute single query to get all records with consistent snapshot
        # Use the provided SQL generator callable
        selectSql = sql_generator(sourceTableName, allColumns, pkColumns)
        logger.debug("Executing source query for complete dataset",
                     dataset_name=datasetToIngestName)
        result = sourceConn.execute(text(selectSql))
        column_names = result.keys()

        # Build insert statement with SQLAlchemy named parameters
        quoted_insert_columns = self.merge_db_ops.get_quoted_columns(allColumns) + [
            self.mrgNM.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME),
            self.mrgNM.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME),
            self.mrgNM.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)
        ]
        placeholders = ", ".join([f":{i}" for i in range(len(quoted_insert_columns))])
        insertSql = f"INSERT INTO {stagingTableName} ({', '.join(quoted_insert_columns)}) VALUES ({placeholders})"

        # Process records in blocks for memory efficiency
        chunkSize = self.getIngestionOverrideValue(self.CHUNK_SIZE_KEY, self.CHUNK_SIZE_DEFAULT)
        recordsInserted = 0

        while True:
            # Read batch from result set (no re-querying)
            rows = result.fetchmany(chunkSize)
            if not rows:
                logger.info("Completed dataset ingestion",
                            dataset_name=datasetToIngestName,
                            total_records_ingested=recordsInserted)
                break

            logger.debug("Processing chunk of rows",
                         dataset_name=datasetToIngestName,
                         chunk_size=len(rows),
                         total_processed=recordsInserted)

            # Process batch and insert into staging
            # Each batch is delimited from others because each record in staging has the batch id which
            # ingested it. This lets us delete the ingested records when resetting the batch and filter for
            # the records in a batch when merging them in to the merge table.
            with mergeEngine.begin() as mergeConn:
                # Create column name mapping for robust data extraction
                column_map: dict[str, int] = {name: idx for idx, name in enumerate(column_names)}
                # Prepare batch data - now includes pre-calculated hashes
                batchValues: List[List[Any]] = []
                for row in rows:
                    # Extract data columns using column mapping
                    dataValues = [row[column_map[col]] for col in allColumns]
                    # Extract hash values using column names
                    allHash = row[column_map[YellowSchemaConstants.ALL_HASH_COLUMN_NAME]]
                    keyHash = row[column_map[YellowSchemaConstants.KEY_HASH_COLUMN_NAME]]
                    # Add batch metadata
                    insertValues = dataValues + [batchId, allHash, keyHash]
                    batchValues.append(insertValues)

                # Execute batch insert using proper SQLAlchemy batch execution
                # Use executemany for true batch efficiency - single execute call for all rows
                all_params = [{str(i): val for i, val in enumerate(values)} for values in batchValues]
                mergeConn.execute(text(insertSql), all_params)
                numRowsInserted: int = len(batchValues)
                recordsInserted += numRowsInserted

        logger.debug("Completed dataset ingestion", dataset_name=datasetToIngestName, records_inserted=recordsInserted)
        # All rows for this dataset have been ingested, move to next dataset.
        return recordsInserted, 0, 0

    def baseIngestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int) -> tuple[int, int, int]:
        # Default SQL generator using the standard source query builder
        def default_sql_generator(sourceTableName: str, allColumns: List[str], pkColumns: List[str]) -> str:
            return self.source_db_ops.build_source_query_with_hashes(
                sourceTableName, allColumns, pkColumns
            )

        state: Optional[BatchState] = None
        # Fetch restart state from batch metrics table
        with mergeEngine.begin() as connection:
            # Get the state from the batch metrics table
            state = self.getBatchState(mergeEngine, connection, key, batchId)
            if state is None:
                raise Exception(f"No state found for batch {batchId} for key {key}")

        # Check for schema changes before ingestion
        self.checkForSchemaChanges(state)

        # Ingest the source records in a single transaction
        logger.info("Starting ingestion for batch",
                    batch_id=batchId,
                    has_more_datasets=state.hasMoreDatasets(),
                    current_dataset_index=state.current_dataset_index,
                    total_datasets=len(state.all_datasets))
        recordsInserted = 0  # Initialize counter for all datasets

        # Truncate the staging tables for each dataset in the batch state
        with mergeEngine.begin() as mergeConn:
            self.truncateStagingTables(mergeConn, state, batchId)

        with sourceEngine.connect() as sourceConn:
            while state.hasMoreDatasets():  # While there is a dataset to ingest
                # Get source table name, Map the dataset name if necessary
                datasetRecordsInserted, datasetRecordsUpdated, datasetRecordsDeleted = self.baseIngestSingleDataset(
                    state, sourceConn, mergeEngine, key, batchId, default_sql_generator)
                recordsInserted += datasetRecordsInserted
                # All rows for this dataset have been ingested, move to next dataset.

                # Move to next dataset if any
                state.moveToNextDataset()
                # If there are more datasets then we stay in STARTED and the next dataset will be ingested
                # on the next iteration. If there are no more datasets then we set the status to INGESTED and
                # the job finishes and waits for the next trigger for MERGE to start.
                if state.hasMoreDatasets():
                    # Move to next dataset
                    logger.debug("Moving to next dataset", next_dataset=state.getCurrentDataset())
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.STARTED, state=state)
                else:
                    # No more datasets to ingest, set the state to merging
                    logger.debug("No more datasets to ingest, setting status to INGESTED")
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.INGESTED, state=state)

        # If we get here, all datasets have been processed (even if they had no data)
        # Make sure the batch status is set to INGESTED
        with mergeEngine.begin() as connection:
            currentStatus = self.checkBatchStatus(connection, key, batchId)
            if currentStatus == BatchStatus.STARTED.value:
                logger.debug("Ensuring batch status is set to INGESTED after processing all datasets")
                self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.INGESTED, state=state)

        return recordsInserted, 0, recordsInserted  # No updates or deletes in snapshot ingestion
