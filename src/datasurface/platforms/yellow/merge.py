"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, Dataset, IngestionConsistencyType, PlatformRuntimeHint
)
from sqlalchemy import Table, MetaData, text
import sqlalchemy
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.schema import Column
from datasurface.md.schema import DDLTable
from sqlalchemy.types import Integer, String, TIMESTAMP
from sqlalchemy.engine.row import Row
from typing import cast, List, Any, Optional
from datasurface.md.governance import SQLIngestion, DataTransformerOutput
from datasurface.platforms.yellow.db_utils import createEngine
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, BatchStatus, BatchState
)
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable, createOrUpdateTable
from datasurface.platforms.yellow.yellow_dp import YellowDatasetUtilities, JobStatus, STREAM_KEY_MAX_LENGTH
from abc import abstractmethod
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger,
)

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

    def getBatchCounterTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch counter table"""
        t: Table = Table(self.getPhysBatchCounterTableName(), MetaData(),
                         Column("key", String(length=STREAM_KEY_MAX_LENGTH), primary_key=True),
                         Column("currentBatch", Integer()))
        return t

    def getBatchMetricsTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch metrics table. The key is either the data store name or the
        data store name and the dataset name."""
        t: Table = Table(self.getPhysBatchMetricsTableName(), MetaData(),
                         Column("key", String(length=STREAM_KEY_MAX_LENGTH), primary_key=True),
                         Column("batch_id", Integer(), primary_key=True),
                         Column("batch_start_time", TIMESTAMP()),
                         Column("batch_end_time", TIMESTAMP(), nullable=True),
                         Column("batch_status", String(length=32)),
                         Column("records_inserted", Integer(), nullable=True),
                         Column("records_updated", Integer(), nullable=True),
                         Column("records_deleted", Integer(), nullable=True),
                         Column("total_records", Integer(), nullable=True),
                         Column("state", String(length=2048), nullable=True))  # This needs to be large enough to hold a BatchState in JSON format.
        return t

    def createBatchCounterTable(self, mergeEngine: Engine) -> None:
        """This creates the batch counter table"""
        t: Table = self.getBatchCounterTable()
        createOrUpdateTable(mergeEngine, t)

    CHUNK_SIZE_KEY: str = "chunkSize"
    CHUNK_SIZE_DEFAULT: int = 50000

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

    def createBatchMetricsTable(self, mergeEngine: Engine) -> None:
        """This creates the batch metrics table"""
        t: Table = self.getBatchMetricsTable()
        createOrUpdateTable(mergeEngine, t)

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str, engine: Optional[Engine] = None) -> Table:
        """This returns the staging schema for a dataset"""
        stagingDS: Dataset = self.schemaProjector.computeSchema(dataset, self.schemaProjector.SCHEMA_TYPE_STAGING)
        t: Table = datasetToSQLAlchemyTable(stagingDS, tableName, sqlalchemy.MetaData(), engine)
        return t

    def getMergeSchemaForDataset(self, dataset: Dataset, tableName: str, engine: Optional[Engine] = None) -> Table:
        """This returns the merge schema for a dataset"""
        mergeDS: Dataset = self.schemaProjector.computeSchema(dataset, self.schemaProjector.SCHEMA_TYPE_MERGE)
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
                f'UPDATE {self.getPhysBatchCounterTableName()} SET "currentBatch" = :new_batch WHERE key = :key'
            ), {"new_batch": newBatch, "key": key})
        else:
            newBatch = 1

        # Insert a new batch event record with started status
        connection.execute(text(
            f"INSERT INTO {self.getPhysBatchMetricsTableName()} "
            f'("key", "batch_id", "batch_start_time", "batch_status", "state") '
            f"VALUES (:key, :batch_id, NOW(), :batch_status, :state)"
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
            # Delete all records for batch id in staging
            # Use BATCH_ID_COLUMN_NAME from the schema projector
            mergeConnection.execute(
                text(f"DELETE FROM {stagingTableName} WHERE {self.schemaProjector.BATCH_ID_COLUMN_NAME} = :batch_id"), {"batch_id": batchId})

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

        return BatchState.model_validate_json(row[0]) if row else BatchState(all_datasets=list(self.store.datasets.keys()))

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
        # First check if the table exists
        from sqlalchemy import inspect
        inspector = inspect(connection.engine)
        table_name = self.getPhysBatchCounterTableName()
        logger.debug("Checking if table exists", table_name=table_name)
        if not inspector.has_table(table_name):
            logger.info("Creating batch counter table", table_name=table_name)
            # Create the table
            t = self.getBatchCounterTable()
            t.create(connection.engine)
            logger.info("Batch counter table created successfully", table_name=table_name)
        else:
            logger.debug("Batch counter table already exists", table_name=table_name)

        # Now query the table
        result = connection.execute(text(
            f'SELECT "currentBatch" FROM {self.getPhysBatchCounterTableName()} WHERE key = :key'
        ), {"key": key})
        row = result.fetchone()
        if row is None:
            # Insert a new batch counter
            connection.execute(text(
                f'INSERT INTO {self.getPhysBatchCounterTableName()} (key, "currentBatch") VALUES (:key, :current_batch)'
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
        with mergeEngine.begin() as connection:
            update_parts = ["batch_status = :batch_status"]
            update_params = {"batch_status": status.value, "key": key, "batch_id": batchId}

            if status in [BatchStatus.COMMITTED, BatchStatus.FAILED]:
                update_parts.append("batch_end_time = NOW()")
            if recordsInserted is not None:
                update_parts.append("records_inserted = :records_inserted")
                update_params["records_inserted"] = recordsInserted
            if recordsUpdated is not None:
                update_parts.append("records_updated = :records_updated")
                update_params["records_updated"] = recordsUpdated
            if recordsDeleted is not None:
                update_parts.append("records_deleted = :records_deleted")
                update_params["records_deleted"] = recordsDeleted
            if totalRecords is not None:
                update_parts.append("total_records = :total_records")
                update_params["total_records"] = totalRecords
            if state is not None:
                update_parts.append("state = :state")
                update_params["state"] = state.model_dump_json()

            update_sql = f'UPDATE {self.getPhysBatchMetricsTableName()} SET {", ".join(update_parts)} WHERE "key" = :key AND "batch_id" = :batch_id'
            connection.execute(text(update_sql), update_params)

    def markBatchMerged(self, connection: Connection, key: str, batchId: int, status: BatchStatus,
                        recordsInserted: Optional[int] = None, recordsUpdated: Optional[int] = None,
                        recordsDeleted: Optional[int] = None, totalRecords: Optional[int] = None) -> None:
        """Mark the batch as merged"""
        update_parts = [f"batch_status = '{status.value}'"]
        if status in [BatchStatus.COMMITTED, BatchStatus.FAILED]:
            update_parts.append("batch_end_time = NOW()")
        if recordsInserted is not None:
            update_parts.append(f"records_inserted = {recordsInserted}")
        if recordsUpdated is not None:
            update_parts.append(f"records_updated = {recordsUpdated}")
        if recordsDeleted is not None:
            update_parts.append(f"records_deleted = {recordsDeleted}")
        if totalRecords is not None:
            update_parts.append(f"total_records = {totalRecords}")

        update_sql = f'UPDATE {self.getPhysBatchMetricsTableName()} SET {", ".join(update_parts)} WHERE "key" = \'{key}\' AND "batch_id" = {batchId}'
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
        mergeUser, mergePassword = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.postgresCredential)
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
            sourceEngine: Engine = createEngine(self.store.cmd.dataContainer, sourceUser, sourcePassword)
            assert cmd.singleOrMultiDatasetIngestion is not None
            ingestionType = cmd.singleOrMultiDatasetIngestion
        else:
            raise Exception(f"Unknown store command type: {type(self.store.cmd)}")

        # Make sure the staging and merge tables exist and have the current schema for each dataset
        self.reconcileStagingTableSchemas(mergeEngine, self.store)
        self.reconcileMergeTableSchemas(mergeEngine, self.store)

        # Create batch counter and metrics tables if they don't exist
        self.createBatchCounterTable(mergeEngine)
        self.createBatchMetricsTable(mergeEngine)

        # Check current batch status to determine what to do
        if ingestionType == IngestionConsistencyType.SINGLE_DATASET:
            # For single dataset ingestion, process each dataset separately
            for dataset in self.store.datasets.values():
                ydu: YellowDatasetUtilities = YellowDatasetUtilities(self.eco, self.credStore, self.dp, self.store, dataset.name)
                key = ydu.getIngestionStreamKey()
                currentStatus = self.executeBatch(sourceEngine, mergeEngine, key)
        else:
            # For multi-dataset ingestion, process all datasets in a single batch
            ydu: YellowDatasetUtilities = YellowDatasetUtilities(self.eco, self.credStore, self.dp, self.store)
            key = ydu.getIngestionStreamKey()
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

    def baseIngestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int) -> tuple[int, int, int]:

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

                # Build hash expressions for PostgreSQL MD5 function
                # For all columns hash: concatenate all column values
                allColumnsHashExpr = " || ".join([f'COALESCE("{col}"::text, \'\')' for col in allColumns])

                # For key columns hash: concatenate only primary key column values
                keyColumnsHashExpr = " || ".join([f'COALESCE("{col}"::text, \'\')' for col in pkColumns])

                # This job will for a new batch, initialize the state with all the datasets in the store and then
                # remove one with an offset of 0 to start. The job will read a "chunk" from that source table and
                # write it to the staging table, updating the offset in the state as it goes. When all data is
                # written to the staging table for that dataset, the job will set the state to the next dataset.
                # When all datasets are done then the job will set the batch status to ingested. The transistion is start
                # means we're ingesting the data for all datasets.

                # Execute single query to get all records with consistent snapshot
                quoted_columns = [f'"{col}"' for col in allColumns]
                selectSql = f"""
                SELECT {', '.join(quoted_columns)},
                    MD5({allColumnsHashExpr}) as {self.schemaProjector.ALL_HASH_COLUMN_NAME},
                    MD5({keyColumnsHashExpr}) as {self.schemaProjector.KEY_HASH_COLUMN_NAME}
                FROM {sourceTableName}
                """
                logger.debug("Executing source query for complete dataset",
                             dataset_name=datasetToIngestName)
                result = sourceConn.execute(text(selectSql))
                column_names = result.keys()

                # Build insert statement with SQLAlchemy named parameters
                quoted_insert_columns = [f'"{col}"' for col in allColumns] + \
                    [
                        f'"{self.schemaProjector.BATCH_ID_COLUMN_NAME}"',
                        f'"{self.schemaProjector.ALL_HASH_COLUMN_NAME}"',
                        f'"{self.schemaProjector.KEY_HASH_COLUMN_NAME}"']
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
                            allHash = row[column_map[self.schemaProjector.ALL_HASH_COLUMN_NAME]]
                            keyHash = row[column_map[self.schemaProjector.KEY_HASH_COLUMN_NAME]]
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
