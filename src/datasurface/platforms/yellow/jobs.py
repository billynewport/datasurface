"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, SQLSnapshotIngestion, DataContainer, PostgresDatabase,
    MySQLDatabase, OracleDatabase, SQLServerDatabase, DB2Database, Dataset, IngestionConsistencyType
)
from sqlalchemy import create_engine, Table, MetaData, text
import sqlalchemy
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.schema import Column
from datasurface.md.schema import DDLTable
from sqlalchemy.types import Integer, String, TIMESTAMP
from sqlalchemy.engine.row import Row
from enum import Enum
from typing import cast, List, Any, Optional
from datasurface.md.governance import DatastoreCacheEntry, EcosystemPipelineGraph, PlatformPipelineGraph
from datasurface.md.lint import ValidationTree
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable, createOrUpdateTable
import argparse
import sys
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md import DataPlatform
import json
import hashlib


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


class BatchStatus(Enum):
    """This is the status of a batch"""
    STARTED = "started"  # The batch is started, ingestion is in progress
    INGESTED = "ingested"  # The batch is ingested, merge is in progress
    COMMITTED = "committed"  # The batch is committed, no more work to do
    FAILED = "failed"  # The batch failed, reset batch to try again


class JobStatus(Enum):
    """This is the status of a job"""
    KEEP_WORKING = 1  # The job is still in progress, put on queue and continue ASAP
    DONE = 0  # The job is complete, wait for trigger to run batch again
    ERROR = -1  # The job failed, stop the job and don't run again


class BatchState:
    """This is the state of a batch being processed. It provides a list of datasets which need to be
    ingested still and for the dataset currently being ingested, where to start ingestion from in terms
    of an offset in the source table."""

    def __init__(self, datasetsToProcess: List[str], schema_versions: Optional[dict[str, str]] = None) -> None:
        self.all_datasets: List[str] = datasetsToProcess
        self.current_dataset_index: int = 0
        self.current_offset: int = 0
        self.schema_versions: dict[str, str] = schema_versions or {}

    def reset(self) -> None:
        """This resets the state to the start of the batch"""
        self.current_dataset_index = 0
        self.current_offset = 0

    def getCurrentDataset(self) -> str:
        """This returns the current dataset"""
        return self.all_datasets[self.current_dataset_index]

    def moveToNextDataset(self) -> None:
        """This moves to the next dataset"""
        self.current_dataset_index += 1
        self.current_offset = 0

    def hasMoreDatasets(self) -> bool:
        """This returns True if there are more datasets to ingest"""
        return self.current_dataset_index < len(self.all_datasets)

    @staticmethod
    def from_json(json_str: str) -> "BatchState":
        # Inflate the json in to this class
        state: BatchState = BatchState([])
        # Load the json string in to a dictionary
        json_dict: dict[str, Any] = json.loads(json_str)
        state.all_datasets = json_dict["all_datasets"]
        state.current_dataset_index = json_dict["current_dataset_index"]
        state.current_offset = json_dict["current_offset"]
        state.schema_versions = json_dict.get("schema_versions", {})
        return state

    def to_json(self) -> str:
        # Convert the class in to a dictionary
        json_dict: dict[str, Any] = self.__dict__
        return json.dumps(json_dict)


class SnapshotMergeJob:
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
        self.eco: Ecosystem = eco
        self.credStore: CredentialStore = credStore
        self.dp: YellowDataPlatform = dp
        self.store: Datastore = store
        self.datasetName: Optional[str] = datasetName
        self.dataset: Optional[Dataset] = self.store.datasets[datasetName] if datasetName is not None else None
        self.cmd: SQLSnapshotIngestion = cast(SQLSnapshotIngestion, store.cmd)  # type: ignore[attr-defined]

    def getSchemaHash(self, dataset: Dataset) -> str:
        """Generate a hash of the dataset schema"""
        schema: DDLTable = cast(DDLTable, dataset.originalSchema)
        schema_data = {
            'columns': {name: {'type': str(col.type), 'nullable': str(col.nullable)}
                        for name, col in schema.columns.items()},
            'primaryKey': schema.primaryKeyColumns.colNames if schema.primaryKeyColumns else []
        }
        schema_str = json.dumps(schema_data, sort_keys=True)
        return hashlib.md5(schema_str.encode()).hexdigest()

    def validateSchemaUnchanged(self, dataset: Dataset, stored_hash: str) -> bool:
        """Validate that the schema hasn't changed since batch start"""
        current_hash = self.getSchemaHash(dataset)
        return current_hash == stored_hash

    def checkForSchemaChanges(self, state: BatchState) -> None:
        """Check if any dataset schemas have changed since batch start"""
        for dataset_name in state.all_datasets:
            dataset = self.store.datasets[dataset_name]
            stored_hash = state.schema_versions.get(dataset_name)
            if stored_hash and not self.validateSchemaUnchanged(dataset, stored_hash):
                raise Exception(f"Schema changed for dataset {dataset_name} during batch processing")

    def createEngine(self, container: DataContainer, userName: str, password: str) -> Engine:
        if isinstance(container, PostgresDatabase):
            return create_engine(  # type: ignore[attr-defined]
                'postgresql://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                    username=userName,
                    password=password,
                    hostName=container.hostPortPair.hostName,
                    port=container.hostPortPair.port,
                    databaseName=container.databaseName
                ),
                isolation_level="READ COMMITTED"
            )
        elif isinstance(container, MySQLDatabase):
            return create_engine(
                'mysql+pymysql://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                    username=userName,
                    password=password,
                    hostName=container.hostPortPair.hostName,
                    port=container.hostPortPair.port,
                    databaseName=container.databaseName
                ),
                isolation_level="READ COMMITTED"
            )
        elif isinstance(container, OracleDatabase):
            return create_engine(
                'oracle+cx_oracle://{username}:{password}@{hostName}:{port}/?service_name={databaseName}'.format(
                    username=userName,
                    password=password,
                    hostName=container.hostPortPair.hostName,
                    port=container.hostPortPair.port,
                    databaseName=container.databaseName
                ),
                isolation_level="READ COMMITTED"
            )
        elif isinstance(container, SQLServerDatabase):
            return create_engine(
                'mssql+pyodbc://{username}:{password}@{hostName}:{port}/{databaseName}?driver=ODBC+Driver+17+for+SQL+Server'.format(
                    username=userName,
                    password=password,
                    hostName=container.hostPortPair.hostName,
                    port=container.hostPortPair.port,
                    databaseName=container.databaseName
                ),
                isolation_level="READ COMMITTED"
            )
        elif isinstance(container, DB2Database):
            return create_engine(
                'ibm_db_sa://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                    username=userName,
                    password=password,
                    hostName=container.hostPortPair.hostName,
                    port=container.hostPortPair.port,
                    databaseName=container.databaseName
                ),
                isolation_level="READ COMMITTED"
            )
        else:
            raise Exception(f"Unsupported container type {type(container)}")

    def getTableForPlatform(self, tableName: str) -> str:
        """This returns the table name for the platform"""
        return f"{self.dp.name}_{tableName}".lower()

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the staging schema for a dataset"""
        t: Table = datasetToSQLAlchemyTable(dataset, tableName, sqlalchemy.MetaData())
        # Add the platform specific columns
        t.append_column(Column(name="ds_surf_batch_id", type_=Integer()))  # type: ignore[attr-defined]
        t.append_column(Column(name="ds_surf_all_hash", type_=String(length=32)))  # type: ignore[attr-defined]
        t.append_column(Column(name="ds_surf_key_hash", type_=String(length=32)))  # type: ignore[attr-defined]
        return t

    def getMergeSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the merge schema for a dataset"""
        t: Table = datasetToSQLAlchemyTable(dataset, tableName, sqlalchemy.MetaData())
        # Add the platform specific columns
        # batch_id here represents the batch a record was inserted in to the merge table
        t.append_column(Column(name="ds_surf_batch_id", type_=Integer()))  # type: ignore[attr-defined]
        # The md5 hash of all the columns in the record
        t.append_column(Column(name="ds_surf_all_hash", type_=String(length=32)))  # type: ignore[attr-defined]
        # The md5 hash of the primary key columns or all the columns if there are no primary key columns
        # This needs to be unique for INSERT...ON CONFLICT to work
        t.append_column(Column(name="ds_surf_key_hash", type_=String(length=32), unique=True))  # type: ignore[attr-defined]
        return t

    def getBaseTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the base table name for a dataset"""
        return f"{self.store.name}_{dataset.name if dataset.name not in self.cmd.tableForDataset else self.cmd.tableForDataset[dataset.name]}"

    def getStagingTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the staging table name for a dataset"""
        tableName: str = self.getBaseTableNameForDataset(dataset)
        return self.getTableForPlatform(tableName + "_staging")

    def getMergeTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the merge table name for a dataset"""
        tableName: str = self.getBaseTableNameForDataset(dataset)
        return self.getTableForPlatform(tableName + "_merge")

    def reconcileStagingTableSchemas(self, mergeEngine: Engine, store: Datastore, cmd: SQLSnapshotIngestion) -> None:
        """This will make sure the staging table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = self.getStagingTableNameForDataset(dataset)
            stagingTable: Table = self.getStagingSchemaForDataset(dataset, tableName)
            createOrUpdateTable(mergeEngine, stagingTable)

    def reconcileMergeTableSchemas(self, mergeEngine: Engine, store: Datastore, cmd: SQLSnapshotIngestion) -> None:
        """This will make sure the merge table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = self.getMergeTableNameForDataset(dataset)
            mergeTable: Table = self.getMergeSchemaForDataset(dataset, tableName)
            createOrUpdateTable(mergeEngine, mergeTable)
            # Ensure the unique constraint exists for ds_surf_key_hash
            self.ensureUniqueConstraintExists(mergeEngine, tableName, "ds_surf_key_hash")

    def ensureUniqueConstraintExists(self, mergeEngine: Engine, tableName: str, columnName: str) -> None:
        """Ensure that a unique constraint exists on the specified column"""
        with mergeEngine.begin() as connection:
            # Check if the constraint already exists
            check_sql = f"""
            SELECT COUNT(*) FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = '{tableName}'
                AND tc.constraint_type = 'UNIQUE'
                AND kcu.column_name = '{columnName}'
            """
            result = connection.execute(text(check_sql))
            constraint_exists = result.fetchone()[0] > 0

            if not constraint_exists:
                # Create the unique constraint
                constraint_name = f"{tableName}_{columnName}_unique"
                create_constraint_sql = f"ALTER TABLE {tableName} ADD CONSTRAINT {constraint_name} UNIQUE ({columnName})"
                print(f"DEBUG: Creating unique constraint: {create_constraint_sql}")
                connection.execute(text(create_constraint_sql))
                print(f"DEBUG: Successfully created unique constraint on {tableName}.{columnName}")

    def getBatchCounterTableName(self) -> str:
        """This returns the name of the batch counter table"""
        return self.getTableForPlatform("batch_counter")

    def getBatchMetricsTableName(self) -> str:
        """This returns the name of the batch metrics table"""
        return self.getTableForPlatform("batch_metrics")

    def getBatchCounterTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch counter table"""
        t: Table = Table(self.getBatchCounterTableName(), MetaData(),
                         Column("key", String(length=255), primary_key=True),
                         Column("currentBatch", Integer()))
        return t

    def getBatchMetricsTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch metrics table. The key is either the data store name or the
        data store name and the dataset name."""
        t: Table = Table(self.getBatchMetricsTableName(), MetaData(),
                         Column("key", String(length=255), primary_key=True),
                         Column("batch_id", Integer(), primary_key=True),
                         Column("batch_start_time", TIMESTAMP()),
                         Column("batch_end_time", TIMESTAMP(), nullable=True),
                         Column("batch_status", String(length=32)),
                         Column("records_inserted", Integer(), nullable=True),
                         Column("records_updated", Integer(), nullable=True),
                         Column("records_deleted", Integer(), nullable=True),
                         Column("total_records", Integer(), nullable=True),
                         Column("state", String(length=2048), nullable=True))  # This needs to be large enough to hold the state of the ingestion
        return t

    def createBatchCounterTable(self, mergeEngine: Engine) -> None:
        """This creates the batch counter table"""
        t: Table = self.getBatchCounterTable()
        createOrUpdateTable(mergeEngine, t)

    def createBatchMetricsTable(self, mergeEngine: Engine) -> None:
        """This creates the batch metrics table"""
        t: Table = self.getBatchMetricsTable()
        createOrUpdateTable(mergeEngine, t)

    def createBatchCommon(self, connection: Connection, key: str, state: BatchState) -> int:
        """This creates a batch and returns the batch id. The transaction is managed by the caller.
        This current batch must be commited before a new batch can be created.
        Get the current batch for the store. The first time, there will be no record in the batch counter table.
        In this case, the current batch is 1.
        """
        currentBatchId: int = self.getCurrentBatchId(connection, key)

        # Check if the current batch is committed
        result = connection.execute(text(
            f'SELECT "batch_status" FROM {self.getBatchMetricsTableName()} WHERE "key" = \'{key}\' AND "batch_id" = {currentBatchId}'
        ))
        batchStatusRow: Optional[Row[Any]] = result.fetchone()
        if batchStatusRow is not None:
            batchStatus: str = batchStatusRow[0]
            if batchStatus != BatchStatus.COMMITTED.value:
                raise Exception(f"Batch {currentBatchId} is not committed")
            newBatch = currentBatchId + 1
            connection.execute(text(f'UPDATE {self.getBatchCounterTableName()} SET "currentBatch" = {newBatch} WHERE key = \'{key}\''))
        else:
            newBatch = 1

        # Insert a new batch event record with started status
        connection.execute(text(
            f"INSERT INTO {self.getBatchMetricsTableName()} "
            f'("key", "batch_id", "batch_start_time", "batch_status", "state") '
            f"VALUES ('{key}', {newBatch}, NOW(), '{BatchStatus.STARTED.value}', '{state.to_json()}')"
        ))

        return newBatch

    def getKey(self) -> str:
        """This returns the key for the batch"""
        return f"{self.store.name}#{self.dataset.name}" if self.dataset is not None else self.store.name

    def createSingleBatch(self, store: Datastore, connection: Connection) -> int:
        """This creates a single-dataset batch and returns the batch id. The transaction is managed by the caller."""
        assert self.dataset is not None
        key: str = self.getKey()
        # Create schema hashes for the dataset
        schema_versions = {self.dataset.name: self.getSchemaHash(self.dataset)}
        state: BatchState = BatchState([self.dataset.name], schema_versions)  # Just one dataset to ingest, start at offset 0
        return self.createBatchCommon(connection, key, state)

    def createMultiBatch(self, store: Datastore, connection: Connection) -> int:
        """This create a multi-dataset batch and returns the batch id. The transaction is managed by the caller.
        This current batch must be commited before a new batch can be created.
        Get the current batch for the store. The first time, there will be no record in the batch counter table.
        In this case, the current batch is 0.
        """
        key: str = self.getKey()
        allDatasets: List[str] = list(store.datasets.keys())

        # Create schema hashes for all datasets
        schema_versions = {}
        for dataset_name in allDatasets:
            dataset = store.datasets[dataset_name]
            schema_versions[dataset_name] = self.getSchemaHash(dataset)

        # Start with the first dataset and the rest of the datasets to go
        state: BatchState = BatchState(allDatasets, schema_versions)  # Start with the first dataset
        return self.createBatchCommon(connection, key, state)

    def startBatch(self, mergeEngine: Engine) -> int:
        """This starts a new batch. If the current batch is not committed, it will raise an exception. A existing batch must be restarted."""
        # Start a new transaction
        newBatchId: int
        with mergeEngine.begin() as connection:
            # Create a new batch
            assert self.cmd is not None
            if self.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET:
                assert self.dataset is not None
                newBatchId = self.createSingleBatch(self.store, connection)
            else:
                newBatchId = self.createMultiBatch(self.store, connection)
            # Grab batch state from the batch metrics table
            state: BatchState = self.getBatchState(mergeEngine, connection, self.getKey(), newBatchId)
            # Truncate the staging table for each dataset in the batch state
            for datasetName in state.all_datasets:
                dataset: Dataset = self.store.datasets[datasetName]
                stagingTableName: str = self.getStagingTableNameForDataset(dataset)
                connection.execute(text(f"TRUNCATE TABLE {stagingTableName}"))
        return newBatchId

    def updateBatchStatusInTx(
            self, mergeEngine: Engine, key: str, batchId: int, status: BatchStatus,
            recordsInserted: Optional[int] = None, recordsUpdated: Optional[int] = None,
            recordsDeleted: Optional[int] = None, totalRecords: Optional[int] = None,
            state: Optional[BatchState] = None) -> None:
        """Update the batch status and metrics in a transaction"""
        with mergeEngine.begin() as connection:
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
            if state is not None:
                update_parts.append(f"state = '{state.to_json()}'")

            update_sql = f'UPDATE {self.getBatchMetricsTableName()} SET {", ".join(update_parts)} WHERE "key" = \'{key}\' AND "batch_id" = {batchId}'
            connection.execute(text(update_sql))

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

        update_sql = f'UPDATE {self.getBatchMetricsTableName()} SET {", ".join(update_parts)} WHERE "key" = \'{key}\' AND "batch_id" = {batchId}'
        connection.execute(text(update_sql))

    def getBatchState(self, mergeEngine: Engine, connection: Connection, key: str, batchId: int) -> BatchState:
        """Get the batch status for a given key and batch id"""
        result = connection.execute(text(f'SELECT "state" FROM {self.getBatchMetricsTableName()} WHERE "key" = \'{key}\' AND "batch_id" = {batchId}'))
        row = result.fetchone()

        return BatchState.from_json(row[0]) if row else BatchState([])

    def ingestNextBatchToStaging(
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
        print(f"DEBUG: Starting ingestion for batch {batchId}, hasMoreDatasets: {state.hasMoreDatasets()}")
        with sourceEngine.connect() as sourceConn:
            while state.hasMoreDatasets():  # While there is a dataset to ingest
                # Get source table name, Map the dataset name if necessary
                datasetToIngestName: str = state.getCurrentDataset()
                dataset: Dataset = self.store.datasets[datasetToIngestName]

                # Get source table name using mapping if necessary
                tableName: str = datasetToIngestName if datasetToIngestName not in self.cmd.tableForDataset else self.cmd.tableForDataset[datasetToIngestName]
                sourceTableName: str = tableName

                # Get destination staging table name
                stagingTableName: str = self.getStagingTableNameForDataset(dataset)

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

                # Read from source table
                # Get total count for metrics
                countResult = sourceConn.execute(text(f"SELECT COUNT(*) FROM {sourceTableName}"))
                totalRecords = countResult.fetchone()[0]

                # Read data in batches to avoid memory issues
                batchSize = 1000
                offset: int = state.current_offset
                recordsInserted = 0

                while True:
                    # Read batch from source with hash calculation in SQL
                    quoted_columns = [f'"{col}"' for col in allColumns]
                    selectSql = f"""
                    SELECT {', '.join(quoted_columns)},
                        MD5({allColumnsHashExpr}) as ds_surf_all_hash,
                        MD5({keyColumnsHashExpr}) as ds_surf_key_hash
                    FROM {sourceTableName}
                    LIMIT {batchSize} OFFSET {offset}
                    """
                    print(f"DEBUG: Executing SQL: {selectSql}")
                    result = sourceConn.execute(text(selectSql))
                    rows = result.fetchall()
                    print(f"DEBUG: Got {len(rows)} rows from source table")

                    if not rows:
                        print("DEBUG: No rows returned, breaking out of ingestion loop")
                        break

                    # Process batch and insert into staging
                    # Each batch is delimited from others because each record in staging has the batch id which
                    # ingested it. This lets us delete the ingested records when resetting the batch and filter for
                    # the records in a batch when merging them in to the merge table.
                    with mergeEngine.begin() as mergeConn:
                        # Prepare batch data - now includes pre-calculated hashes
                        batchValues: List[List[Any]] = []
                        for row in rows:
                            # Extract data columns (excluding the hash columns we added)
                            dataValues = list(row)[:len(allColumns)]
                            # Extract hash values (last two columns from our SELECT)
                            allHash = row[-2]  # ds_surf_all_hash
                            keyHash = row[-1]  # ds_surf_key_hash
                            # Add batch metadata
                            insertValues = dataValues + [batchId, allHash, keyHash]
                            batchValues.append(insertValues)

                        # Build insert statement with SQLAlchemy named parameters
                        quoted_columns = [f'"{col}"' for col in allColumns] + ["ds_surf_batch_id", "ds_surf_all_hash", "ds_surf_key_hash"]
                        placeholders = ", ".join([f":{i}" for i in range(len(quoted_columns))])
                        insertSql = f"INSERT INTO {stagingTableName} ({', '.join(quoted_columns)}) VALUES ({placeholders})"

                        # Execute batch insert using proper SQLAlchemy batch execution
                        # Use executemany from the underlying DBAPI for true batch efficiency
                        # This is the most efficient way to do batch inserts
                        for values in batchValues:
                            params = {str(i): val for i, val in enumerate(values)}
                            mergeConn.execute(text(insertSql), params)
                        numRowsInserted: int = len(batchValues)
                        recordsInserted += numRowsInserted
                        # Write the offset to the state
                        state.current_offset = offset + numRowsInserted
                        # Update the state in the batch metrics table
                        mergeConn.execute(text(
                            f'UPDATE {self.getBatchMetricsTableName()} SET "state" = \'{state.to_json()}\' WHERE "key" = \'{key}\' AND "batch_id" = {batchId}'))

                    offset = state.current_offset

                print(f"DEBUG: Exited ingestion loop for dataset {datasetToIngestName}")
                # All rows for this dataset have been ingested, move to next dataset.
                print(f"DEBUG: Finished ingesting dataset {datasetToIngestName}, hasMoreDatasets: {state.hasMoreDatasets()}")

                # Move to next dataset if any
                state.moveToNextDataset()
                # If there are more datasets then we stay in STARTED and the next dataset will be ingested
                # on the next iteration. If there are no more datasets then we set the status to INGESTED and
                # the job finishes and waits for the next trigger for MERGE to start.
                if state.hasMoreDatasets():
                    # Move to next dataset
                    print(f"DEBUG: Moving to next dataset: {state.getCurrentDataset()}")
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.STARTED, state=state)
                else:
                    # No more datasets to ingest, set the state to merging
                    print("DEBUG: No more datasets to ingest, setting status to INGESTED")
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.INGESTED, state=state)

        # If we get here, all datasets have been processed (even if they had no data)
        # Make sure the batch status is set to INGESTED
        with mergeEngine.begin() as connection:
            currentStatus = self.checkBatchStatus(connection, key, batchId)
            if currentStatus == BatchStatus.STARTED.value:
                print("DEBUG: Ensuring batch status is set to INGESTED after processing all datasets")
                self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.INGESTED, state=state)

        return recordsInserted, 0, totalRecords  # No updates or deletes in snapshot ingestion

    def mergeStagingToMerge(self, mergeEngine: Engine, batchId: int, key: str, batch_size: int = 10000, use_merge: bool = False) -> tuple[int, int, int]:
        """Merge staging data into merge table using either INSERT...ON CONFLICT (default) or PostgreSQL MERGE with DELETE support.

        Args:
            mergeEngine: Database engine for merge operations
            batchId: Current batch ID
            key: Batch key identifier
            batch_size: Number of records to process per batch (default: 10000)
            use_merge: If True, use PostgreSQL MERGE with DELETE support (requires PostgreSQL 15+)
        """
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
                stagingTableName: str = self.getStagingTableNameForDataset(dataset)
                mergeTableName: str = self.getMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                # Get all column names
                allColumns: List[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = [f'"{col}"' for col in allColumns]

                # Get total count for processing
                count_result = connection.execute(text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE ds_surf_batch_id = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                print(f"DEBUG: Processing {total_records} records for dataset {datasetToMergeName} in batches of {batch_size}")

                # Process in batches using INSERT...ON CONFLICT for better PostgreSQL compatibility
                for offset in range(0, total_records, batch_size):
                    batch_upsert_sql = f"""
                    WITH batch_data AS (
                        SELECT * FROM {stagingTableName}
                        WHERE ds_surf_batch_id = {batchId}
                        ORDER BY ds_surf_key_hash
                        LIMIT {batch_size} OFFSET {offset}
                    )
                    INSERT INTO {mergeTableName} ({', '.join(quoted_all_columns)}, ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash)
                    SELECT {', '.join([f'b."{col}"' for col in allColumns])}, {batchId}, b.ds_surf_all_hash, b.ds_surf_key_hash
                    FROM batch_data b
                    ON CONFLICT (ds_surf_key_hash)
                    DO UPDATE SET
                        {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in allColumns])},
                        ds_surf_batch_id = EXCLUDED.ds_surf_batch_id,
                        ds_surf_all_hash = EXCLUDED.ds_surf_all_hash,
                        ds_surf_key_hash = EXCLUDED.ds_surf_key_hash
                    WHERE {mergeTableName}.ds_surf_all_hash != EXCLUDED.ds_surf_all_hash
                    """

                    print(f"DEBUG: Executing batch upsert for offset {offset}")
                    connection.execute(text(batch_upsert_sql))

                # Handle deletions once after all inserts/updates for this dataset
                delete_sql = f"""
                DELETE FROM {mergeTableName} m
                WHERE NOT EXISTS (
                    SELECT 1 FROM {stagingTableName} s
                    WHERE s.ds_surf_batch_id = {batchId}
                    AND s.ds_surf_key_hash = m.ds_surf_key_hash
                )
                """
                print(f"DEBUG: Executing deletion query for dataset {datasetToMergeName}")
                delete_result = connection.execute(text(delete_sql))
                dataset_deleted = delete_result.rowcount
                total_deleted += dataset_deleted

                # Get final metrics for this dataset more efficiently
                metrics_sql = f"""
                SELECT
                    COUNT(*) FILTER (WHERE ds_surf_batch_id = {batchId}) as inserted_count,
                    COUNT(*) FILTER (WHERE ds_surf_batch_id != {batchId} AND ds_surf_key_hash IN (
                        SELECT ds_surf_key_hash FROM {stagingTableName} WHERE ds_surf_batch_id = {batchId}
                    )) as updated_count
                FROM {mergeTableName}
                WHERE ds_surf_key_hash IN (
                    SELECT ds_surf_key_hash FROM {stagingTableName} WHERE ds_surf_batch_id = {batchId}
                )
                """

                metrics_result = connection.execute(text(metrics_sql))
                metrics_row = metrics_result.fetchone()
                dataset_inserted = metrics_row[0] if metrics_row[0] else 0
                dataset_updated = metrics_row[1] if metrics_row[1] else 0

                total_inserted += dataset_inserted
                total_updated += dataset_updated

                print(f"DEBUG: Dataset {datasetToMergeName} - Inserted: {dataset_inserted}, Updated: {dataset_updated}, Deleted: {dataset_deleted}")

            # Now update the batch status to merged within the existing transaction
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        print(f"DEBUG: Total merge results - Inserted: {total_inserted}, Updated: {total_updated}, Deleted: {total_deleted}")
        return total_inserted, total_updated, total_deleted

    def _mergeStagingToMergeWithMerge(self, mergeEngine: Engine, batchId: int, key: str, batch_size: int = 10000) -> tuple[int, int, int]:
        """Merge staging data into merge table using PostgreSQL MERGE with DELETE support (PostgreSQL 15+).
        This approach handles INSERT, UPDATE, and DELETE operations in a single MERGE statement."""
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
                stagingTableName: str = self.getStagingTableNameForDataset(dataset)
                mergeTableName: str = self.getMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                # Get all column names
                allColumns: List[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = [f'"{col}"' for col in allColumns]

                # Get total count for processing
                count_result = connection.execute(text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE ds_surf_batch_id = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                print(f"DEBUG: Processing {total_records} records for dataset {datasetToMergeName} using MERGE with DELETE")

                # Create a staging view that includes records to be deleted
                # We need to identify records that exist in merge but not in staging (deletions)
                staging_with_deletes_sql = f"""
                WITH staging_records AS (
                    SELECT ds_surf_key_hash FROM {stagingTableName} WHERE ds_surf_batch_id = {batchId}
                ),
                merge_records AS (
                    SELECT ds_surf_key_hash FROM {mergeTableName}
                ),
                records_to_delete AS (
                    SELECT m.ds_surf_key_hash
                    FROM merge_records m
                    WHERE NOT EXISTS (
                        SELECT 1 FROM staging_records s WHERE s.ds_surf_key_hash = m.ds_surf_key_hash
                    )
                )
                SELECT
                    s.*,
                    'INSERT' as operation
                FROM {stagingTableName} s
                WHERE s.ds_surf_batch_id = {batchId}
                UNION ALL
                SELECT
                    m.*,
                    'DELETE' as operation
                FROM {mergeTableName} m
                INNER JOIN records_to_delete d ON m.ds_surf_key_hash = d.ds_surf_key_hash
                """

                # Create temporary staging table with operation column
                temp_staging_table = f"temp_staging_{batchId}_{datasetToMergeName.replace('-', '_')}"
                create_temp_sql = f"""
                CREATE TEMP TABLE {temp_staging_table} AS {staging_with_deletes_sql}
                """
                connection.execute(text(create_temp_sql))

                # Now use MERGE with DELETE support
                merge_sql = f"""
                MERGE INTO {mergeTableName} m
                USING {temp_staging_table} s
                ON m.ds_surf_key_hash = s.ds_surf_key_hash
                WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
                WHEN MATCHED AND s.operation = 'INSERT' AND m.ds_surf_all_hash != s.ds_surf_all_hash THEN
                    UPDATE SET
                        {', '.join([f'"{col}" = s."{col}"' for col in allColumns])},
                        ds_surf_batch_id = {batchId},
                        ds_surf_all_hash = s.ds_surf_all_hash,
                        ds_surf_key_hash = s.ds_surf_key_hash
                WHEN NOT MATCHED AND s.operation = 'INSERT' THEN
                    INSERT ({', '.join(quoted_all_columns)}, ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash)
                    VALUES ({', '.join([f's."{col}"' for col in allColumns])}, {batchId}, s.ds_surf_all_hash, s.ds_surf_key_hash)
                """

                print(f"DEBUG: Executing MERGE with DELETE for dataset {datasetToMergeName}")
                connection.execute(text(merge_sql))

                # Get metrics from the MERGE operation
                # Note: PostgreSQL doesn't return row counts from MERGE, so we need to calculate them
                metrics_sql = f"""
                SELECT
                    COUNT(*) FILTER (WHERE ds_surf_batch_id = {batchId}) as inserted_count,
                    COUNT(*) FILTER (WHERE ds_surf_batch_id != {batchId} AND ds_surf_key_hash IN (
                        SELECT ds_surf_key_hash FROM {stagingTableName} WHERE ds_surf_batch_id = {batchId}
                    )) as updated_count
                FROM {mergeTableName}
                WHERE ds_surf_key_hash IN (
                    SELECT ds_surf_key_hash FROM {stagingTableName} WHERE ds_surf_batch_id = {batchId}
                )
                """

                metrics_result = connection.execute(text(metrics_sql))
                metrics_row = metrics_result.fetchone()
                dataset_inserted = metrics_row[0] if metrics_row[0] else 0
                dataset_updated = metrics_row[1] if metrics_row[1] else 0

                # For deletions, count records that were in merge but not in staging
                deleted_count_sql = f"""
                SELECT COUNT(*) FROM {mergeTableName} m
                WHERE m.ds_surf_key_hash NOT IN (
                    SELECT ds_surf_key_hash FROM {stagingTableName} WHERE ds_surf_batch_id = {batchId}
                )
                AND m.ds_surf_batch_id != {batchId}
                """
                deleted_result = connection.execute(text(deleted_count_sql))
                dataset_deleted = deleted_result.fetchone()[0]

                total_inserted += dataset_inserted
                total_updated += dataset_updated
                total_deleted += dataset_deleted

                print(f"DEBUG: Dataset {datasetToMergeName} - Inserted: {dataset_inserted}, Updated: {dataset_updated}, Deleted: {dataset_deleted}")

            # Now update the batch status to merged within the existing transaction
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        print(f"DEBUG: Total merge results - Inserted: {total_inserted}, Updated: {total_updated}, Deleted: {total_deleted}")
        return total_inserted, total_updated, total_deleted

    def checkBatchStatus(self, connection: Connection, key: str, batchId: int) -> Optional[str]:
        """Check the current batch status for a given key. Returns the status or None if no batch exists."""
        result = connection.execute(text(f"""
            SELECT bm."batch_status"
            FROM {self.getBatchMetricsTableName()} bm
            WHERE bm."key" = '{key}' AND bm."batch_id" = {batchId}
        """))
        row = result.fetchone()
        return row[0] if row else None

    def executeBatch(self, sourceEngine: Engine, mergeEngine: Engine, key: str) -> JobStatus:
        with mergeEngine.begin() as connection:
            batchId: int = self.getCurrentBatchId(connection, key)
            currentStatus = self.checkBatchStatus(connection, key, batchId)

        if currentStatus is None:
            # No batch exists, start a new one
            batchId = self.startBatch(mergeEngine)
            print(f"Started new batch {batchId} for {key}")
            # Batch is started, continue with ingestion
            with mergeEngine.begin() as connection:
                batchId = self.getCurrentBatchId(connection, key)
            print(f"Continuing batch {batchId} for {key} (status: {currentStatus})")
            self.ingestNextBatchToStaging(sourceEngine, mergeEngine, key, batchId)
            return JobStatus.KEEP_WORKING

        elif currentStatus == BatchStatus.STARTED.value:
            # Batch is started, continue with ingestion
            print(f"Continuing batch {batchId} for {key} (status: {currentStatus})")
            self.ingestNextBatchToStaging(sourceEngine, mergeEngine, key, batchId)
            return JobStatus.KEEP_WORKING

        elif currentStatus == BatchStatus.INGESTED.value:
            # Batch is ingested, continue with merge
            print(f"Continuing batch {batchId} for {key} (status: {currentStatus})")
            self.mergeStagingToMerge(mergeEngine, batchId, key)
            # Batch is committed, job is done for this run
            return JobStatus.DONE

        elif currentStatus == BatchStatus.COMMITTED.value:
            # Batch is already committed, job is done for this run
            print(f"Batch for {key} is already committed, start new batch")
            self.startBatch(mergeEngine)
            print(f"Started new batch {batchId} for {key}")
            return JobStatus.KEEP_WORKING

        elif currentStatus == BatchStatus.FAILED.value:
            # Batch failed, we're done
            print(f"Batch for {key} failed")
            return JobStatus.DONE
        return JobStatus.ERROR

    def run(self) -> JobStatus:
        # First, get a connection to the source database
        cmd: SQLSnapshotIngestion = cast(SQLSnapshotIngestion, self.store.cmd)
        assert cmd.credential is not None

        # Now, get a connection to the merge database
        mergeUser, mergePassword = self.credStore.getAsUserPassword(self.dp.postgresCredential)
        mergeEngine: Engine = self.createEngine(self.dp.mergeStore, mergeUser, mergePassword)

        # Now, get an Engine for the source database
        sourceUser, sourcePassword = self.credStore.getAsUserPassword(cmd.credential)
        assert self.store.cmd.dataContainer is not None
        sourceEngine: Engine = self.createEngine(self.store.cmd.dataContainer, sourceUser, sourcePassword)

        # Make sure the staging and merge tables exist and have the current schema for each dataset
        self.reconcileStagingTableSchemas(mergeEngine, self.store, cmd)
        self.reconcileMergeTableSchemas(mergeEngine, self.store, cmd)

        # Create batch counter and metrics tables if they don't exist
        self.createBatchCounterTable(mergeEngine)
        self.createBatchMetricsTable(mergeEngine)

        # Check current batch status to determine what to do
        if cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET:
            # For single dataset ingestion, process each dataset separately
            for dataset in self.store.datasets.values():
                key = f"{self.store.name}#{dataset.name}"
                currentStatus = self.executeBatch(sourceEngine, mergeEngine, key)
        else:
            # For multi-dataset ingestion, process all datasets in a single batch
            key = self.store.name
            currentStatus = self.executeBatch(sourceEngine, mergeEngine, key)
        return currentStatus

    def getCurrentBatchId(self, connection: Connection, key: str) -> int:
        """Get the current batch ID for a given key, create a new batch counter if one doesn't exist."""
        # First check if the table exists
        from sqlalchemy import inspect
        inspector = inspect(connection.engine)
        table_name = self.getBatchCounterTableName()
        print(f"DEBUG: Checking if table {table_name} exists")
        if not inspector.has_table(table_name):
            print(f"DEBUG: Table {table_name} does not exist, creating it")
            # Create the table
            t = self.getBatchCounterTable()
            t.create(connection.engine)
            print(f"DEBUG: Table {table_name} created")
        else:
            print(f"DEBUG: Table {table_name} already exists")

        # Now query the table
        result = connection.execute(text(f'SELECT "currentBatch" FROM {self.getBatchCounterTableName()} WHERE key = \'{key}\''))
        row = result.fetchone()
        if row is None:
            # Insert a new batch counter
            connection.execute(text(f'INSERT INTO {self.getBatchCounterTableName()} (key, "currentBatch") VALUES (\'{key}\', 1)'))
            return 1
        else:
            return row[0]


def main():
    """Main entry point for the SnapshotMergeJob when run as a command-line tool."""
    parser = argparse.ArgumentParser(description='Run SnapshotMergeJob for a specific ingestion stream')
    parser.add_argument('--platform-name', required=True, help='Name of the platform')
    parser.add_argument('--store-name', required=True, help='Name of the datastore')
    parser.add_argument('--dataset-name', help='Name of the dataset (for single dataset ingestion)')
    parser.add_argument('--operation', default='snapshot-merge', help='Operation to perform')
    parser.add_argument('--git-repo-path', required=True, help='Path to the git repository')

    args = parser.parse_args()

    eco: Optional[Ecosystem] = None
    tree: Optional[ValidationTree] = None
    eco, tree = loadEcosystemFromEcoModule(args.git_repo_path)
    if eco is None or tree is None:
        print("Failed to load ecosystem")
        return -1  # ERROR
    if tree.hasErrors():
        print("Ecosystem model has errors")
        tree.printTree()
        return -1  # ERROR

    if args.operation == "snapshot-merge":
        print(f"Running SnapshotMergeJob for platform: {args.platform_name}, store: {args.store_name}")
        if args.dataset_name:
            print(f"Dataset: {args.dataset_name}")

        dp: Optional[DataPlatform] = eco.getDataPlatform(args.platform_name)
        if dp is None:
            print(f"Unknown platform: {args.platform_name}")
            return -1  # ERROR
        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)
        root: Optional[PlatformPipelineGraph] = graph.roots.get(dp)
        if root is None:
            print(f"Unknown graph for platform: {args.platform_name}")
            return -1  # ERROR
        # Is this datastore being ingested by this platform?
        if args.store_name not in root.storesToIngest:
            print(f"Datastore {args.store_name} is not being ingested by platform: {args.platform_name}")
            return -1  # ERROR

        storeEntry: Optional[DatastoreCacheEntry] = eco.cache_getDatastore(args.store_name)
        if storeEntry is None:
            print(f"Unknown store: {args.store_name}")
            return -1  # ERROR
        store: Datastore = storeEntry.datastore

        if store.cmd is None:
            print(f"Store {args.store_name} has no capture meta data")
            return -1  # ERROR
        else:
            if store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET:
                if args.dataset_name is None:
                    print("Single dataset ingestion requires a dataset name")
                    return -1  # ERROR
            elif store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.MULTI_DATASET:
                if args.dataset_name is not None:
                    print("Multi dataset ingestion does not require a dataset name")
                    return -1  # ERROR

        if store.cmd is None:
            print(f"Store {args.store_name} has no credential")
            return -1  # ERROR

        if args.dataset_name:
            dataset: Optional[Dataset] = store.datasets.get(args.dataset_name)
            if dataset is None:
                print(f"Unknown dataset: {args.dataset_name}")
                return -1  # ERROR

        job: SnapshotMergeJob = SnapshotMergeJob(eco, dp.getCredentialStore(), cast(YellowDataPlatform, dp), store, args.dataset_name)

        jobStatus: JobStatus = job.run()
        if jobStatus == JobStatus.DONE:
            print("Job completed successfully")
            return 0  # DONE
        elif jobStatus == JobStatus.KEEP_WORKING:
            print("Job is still in progress")
            return 1  # KEEP_WORKING
        else:
            print("Job failed")
            return -1  # ERROR
    else:
        print(f"Unknown operation: {args.operation}")
        return -1  # ERROR


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
