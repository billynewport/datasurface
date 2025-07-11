# type: ignore[attr-defined, unknown-member, unknown-argument, unknown-variable, unknown-parameter]
"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Datastore, Ecosystem, CredentialStore, SQLSnapshotIngestion, DataContainer, PostgresDatabase, Dataset, IngestionConsistencyType
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.engine.result import Row
from enum import Enum
from typing import cast, List, Any, Optional
from datasurface.md.governance import EcosystemPipelineGraph, PlatformPipelineGraph
from datasurface.md.lint import ValidationTree
from datasurface.platforms.kubpgstarter.kubpgstarter import KubernetesPGStarterDataPlatform
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable, createOrUpdateTable
import argparse
import sys
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md import DataPlatform
import json


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
    STARTED = "started"
    INGESTING = "ingesting"
    MERGING = "merging"
    MERGED = "merged"
    COMMITTED = "committed"
    FAILED = "failed"


class JobStatus(Enum):
    """This is the status of a job"""
    KEEP_WORKING = 1  # The job is still in progress, put on queue and continue ASAP
    DONE = 0  # The job is complete, wait for trigger to run batch again
    ERROR = -1  # The job failed, stop the job and don't run again


class BatchState:
    """This is the state of a batch being processed. It provides a list of datasets which need to be
    ingested still and for the dataset currently being ingested, where to start ingestion from in terms
    of an offset in the source table."""

    def __init__(self, datasets_to_go: List[str], current_dataset: str, current_offset: int) -> None:
        self.datasets_to_go: List[str] = datasets_to_go
        self.current_dataset: Optional[str] = current_dataset
        self.current_offset: int = current_offset

    @staticmethod
    def from_json(json_str: str) -> "BatchState":
        return BatchState(**json.loads(json_str))

    def to_json(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__)


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

    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: KubernetesPGStarterDataPlatform, store: Datastore) -> None:
        self.eco: Ecosystem = eco
        self.credStore: CredentialStore = credStore
        self.dp: KubernetesPGStarterDataPlatform = dp
        self.store: Datastore = store

    def createEngine(self, container: DataContainer, userName: str, password: str) -> Engine:
        if isinstance(container, PostgresDatabase):
            return create_engine(  # type: ignore[attr-defined]
                'postgresql://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                    username=userName,
                    password=password,
                    hostName=container.connection.hostName,
                    port=container.connection.port,
                    databaseName=container.databaseName
                )
            )
        else:
            raise Exception(f"Unsupported container type {type(container)}")

    def getTableForPlatform(self, tableName: str) -> str:
        """This returns the table name for the platform"""
        return f"{self.dp.platformName}_{tableName}"

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the staging schema for a dataset"""
        t: Table = datasetToSQLAlchemyTable(dataset, tableName)
        # Add the platform specific columns
        t.append_column(Column(name="ds_surf_batch_id", type_=Integer()))  # type: ignore[attr-defined]
        t.append_column(Column(name="ds_surf_all_hash", type_=String(length=32)))  # type: ignore[attr-defined]
        t.append_column(Column(name="ds_surf_key_hash", type_=String(length=32)))  # type: ignore[attr-defined]
        return t

    def getMergeSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the merge schema for a dataset"""
        t: Table = datasetToSQLAlchemyTable(dataset, tableName)
        # Add the platform specific columns
        # batch_id here represents the batch a record was inserted in to the merge table
        t.append_column(Column(name="ds_surf_batch_id", type_=Integer()))  # type: ignore[attr-defined]
        # The md5 hash of all the columns in the record
        t.append_column(Column(name="ds_surf_all_hash", type_=String(length=32)))  # type: ignore[attr-defined]
        # The md5 hash of the primary key columns or all the columns if there are no primary key columns
        t.append_column(Column(name="ds_surf_key_hash", type_=String(length=32)))  # type: ignore[attr-defined]
        return t

    def getStagingTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the staging table name for a dataset"""
        cmd: SQLSnapshotIngestion = self.store.cmd
        tableName: str = f"{self.store.name}_{dataset.name if dataset.name not in cmd.tableForDataset else cmd.tableForDataset[dataset.name]}"
        return self.getTableForPlatform(tableName + "_staging")

    def getMergeTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the merge table name for a dataset"""
        cmd: SQLSnapshotIngestion = self.store.cmd
        tableName: str = f"{self.store.name}_{dataset.name if dataset.name not in cmd.tableForDataset else cmd.tableForDataset[dataset.name]}"
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

    def getBatchCounterTableName(self) -> str:
        """This returns the name of the batch counter table"""
        return self.getTableForPlatform("batch_counter")

    def getBatchMetricsTableName(self) -> str:
        """This returns the name of the batch metrics table"""
        return self.getTableForPlatform("batch_metrics")

    def getBatchCounterTable(self) -> str:
        """This constructs the sqlalchemy table for the batch counter table"""
        t: Table = Table(self.getBatchCounterTableName(), MetaData(),
                         Column("key", String(length=255), primary_key=True),
                         Column("currentBatch", Integer()))
        return t

    def getBatchMetricsTable(self) -> str:
        """This constructs the sqlalchemy table for the batch metrics table. The key is either the data store name or the
        data store name and the dataset name."""
        t: Table = Table(self.getBatchMetricsTableName(), MetaData(),
                         Column("key", String(length=255), primary_key=True),
                         Column("batch_id", Integer(), primary_key=True),
                         Column("batch_start_time", DateTime()),
                         Column("batch_end_time", DateTime(), nullable=True),
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
        In this case, the current batch is 0.
        """
        result = connection.execute(text(f"SELECT currentBatch FROM {self.getBatchCounterTableName()} WHERE key = '{key}'"))
        counterRow: Optional[Row[Any]] = result.fetchone()
        if counterRow is None:
            currentBatch = 0
        else:
            currentBatch = counterRow[0]

        # Check if the current batch is committed
        result = connection.execute(text(f"SELECT batch_status FROM {self.getBatchMetricsTableName()} WHERE key = '{key}' AND batch_id = {currentBatch}"))
        batchStatusRow: Optional[Row[Any]] = result.fetchone()
        if batchStatusRow is not None:
            batchStatus: str = batchStatusRow[0]
            if batchStatus != BatchStatus.COMMITTED.value:
                raise Exception(f"Batch {currentBatch} is not committed")

        # Increment the batch counter
        newBatch = currentBatch + 1
        connection.execute(text(f"UPDATE {self.getBatchCounterTableName()} SET currentBatch = {newBatch} WHERE key = '{key}'"))

        # Insert a new batch event record with started status
        connection.execute(text(
            f"INSERT INTO {self.getBatchMetricsTableName()} "
            f"(key, batch_id, batch_start_time, batch_status, state) "
            f"VALUES ('{key}', {newBatch}, NOW(), '{BatchStatus.STARTED.value}', '{state.to_json()}')"
        ))

        return newBatch

    def createSingleBatch(self, store: Datastore, dataset: Dataset, connection: Connection) -> int:
        """This creates a single-dataset batch and returns the batch id. The transaction is managed by the caller."""
        key: str = f"{self.store.name}#{dataset.name}"
        state: BatchState = BatchState([], dataset.name, 0)  # Just one dataset to ingest, start at offset 0
        return self.createBatchCommon(connection, key, state)

    def createMultiBatch(self, store: Datastore, connection: Connection) -> int:
        """This create a multi-dataset batch and returns the batch id. The transaction is managed by the caller.
        This current batch must be commited before a new batch can be created.
        Get the current batch for the store. The first time, there will be no record in the batch counter table.
        In this case, the current batch is 0.
        """
        key: str = self.store.name
        allDatasets: List[str] = list(store.datasets.keys())

        # Start with the first dataset and the rest of the datasets to go
        state: BatchState = BatchState(allDatasets[1:], allDatasets[0], 0)  # Start with the first dataset
        return self.createBatchCommon(connection, key, state)

    def startBatch(self, mergeEngine: Engine, store: Datastore, dataset: Optional[Dataset]) -> int:
        """This starts a new batch. If the current batch is not committed, it will raise an exception. A existing batch must be restarted."""
        # Start a new transaction
        newBatchId: int
        with mergeEngine.begin() as connection:
            # Create a new batch
            assert store.cmd is not None
            if store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET:
                assert dataset is not None
                newBatchId = self.createSingleBatch(store, dataset, connection)
            else:
                newBatchId = self.createMultiBatch(store, connection)
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

            update_sql = f"UPDATE {self.getBatchMetricsTableName()} SET {', '.join(update_parts)} WHERE key = '{key}' AND batch_id = {batchId}"
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

        update_sql = f"UPDATE {self.getBatchMetricsTableName()} SET {', '.join(update_parts)} WHERE key = '{key}' AND batch_id = {batchId}"
        connection.execute(text(update_sql))

    def ingestNextBatchToStaging(
            self, sourceEngine: Engine, mergeEngine: Engine, key: str,
            batchId: int, cmd: SQLSnapshotIngestion) -> tuple[int, int, int]:

        state: Optional[BatchState] = None
        # Fetch restart state from batch metrics table
        with mergeEngine.begin() as connection:
            result = connection.execute(text(f"SELECT state FROM {self.getBatchMetricsTableName()} WHERE key = '{key}' AND batch_id = {batchId}"))
            restartState: Optional[Row[Any]] = result.fetchone()
            if restartState is not None:
                restartState: str = restartState[0]
                state = BatchState.from_json(restartState)
            else:
                raise Exception(f"No state found for batch {batchId} for key {key}")

        # Ingest the source records in a single transaction
        with sourceEngine.connect() as sourceConn:
            while state.current_dataset is not None:  # While there is a dataset to ingest
                # Get source table name, Map the dataset name if necessary
                datasetToIngestName: str = state.current_dataset
                dataset: Dataset = self.store.datasets[datasetToIngestName]

                # Get source table name using mapping if necessary
                tableName: str = datasetToIngestName if datasetToIngestName not in cmd.tableForDataset else cmd.tableForDataset[datasetToIngestName]
                sourceTableName: str = tableName

                # Get destination staging table name
                stagingTableName: str = self.getStagingTableNameForDataset(dataset)

                # Get primary key columns
                pkColumns: List[str] = [col.name for col in dataset.schema.columns if col.isPrimaryKey]
                if not pkColumns:
                    # If no primary key, use all columns
                    pkColumns = [col.name for col in dataset.schema.columns]

                # Get all column names
                allColumns: List[str] = [col.name for col in dataset.schema.columns]

                # Build hash expressions for PostgreSQL MD5 function
                # For all columns hash: concatenate all column values
                allColumnsHashExpr = " || ".join([f"COALESCE({col}::text, '')" for col in allColumns])

                # For key columns hash: concatenate only primary key column values
                keyColumnsHashExpr = " || ".join([f"COALESCE({col}::text, '')" for col in pkColumns])

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
                    selectSql = f"""
                    SELECT {', '.join(allColumns)},
                        MD5({allColumnsHashExpr}) as ds_surf_all_hash,
                        MD5({keyColumnsHashExpr}) as ds_surf_key_hash
                    FROM {sourceTableName}
                    LIMIT {batchSize} OFFSET {offset}
                    """
                    result = sourceConn.execute(text(selectSql))
                    rows = result.fetchall()

                    if not rows:
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

                        # Build insert statement with proper PostgreSQL placeholders
                        columns = allColumns + ["ds_surf_batch_id", "ds_surf_all_hash", "ds_surf_key_hash"]
                        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
                        insertSql = f"INSERT INTO {stagingTableName} ({', '.join(columns)}) VALUES ({placeholders})"

                        # Execute batch insert using proper SQLAlchemy batch execution
                        # Use executemany from the underlying DBAPI for true batch efficiency
                        # This is the most efficient way to do batch inserts
                        mergeConn.executemany(text(insertSql), batchValues)
                        numRowsInserted: int = len(batchValues)
                        recordsInserted += numRowsInserted
                        # Write the offset to the state
                        state.current_offset = offset + numRowsInserted
                        # Update the state in the batch metrics table
                        mergeConn.execute(text(
                            f"UPDATE {self.getBatchMetricsTableName()} SET state = '{json.dumps(state)}' WHERE key = '{key}' AND batch_id = {batchId}"))

                    offset = state.current_offset

            # All rows for this dataset have been ingested, move to next dataset.
            if len(state.datasets_to_go) > 0:
                # There are more datasets to ingest, set the state to the next dataset
                state.current_dataset = state.datasets_to_go[0]
                state.current_offset = 0
                state.datasets_to_go = state.datasets_to_go[1:]
                self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.INGESTING, state=state)
            else:
                # No more datasets to ingest, set the state to merging
                state.current_dataset = None
                self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.MERGING, state=state)

        return recordsInserted, 0, totalRecords  # No updates or deletes in snapshot ingestion

    def mergeStagingToMerge(self, mergeEngine: Engine, dataset: Dataset, batchId: int,
                            cmd: SQLSnapshotIngestion, key: str) -> tuple[int, int, int]:
        """Merge staging data into merge table using PostgreSQL MERGE command and ds_surf_key_hash as join key, including deletions. This
        really has to be done in a single transaction no matter what as otherwise, the consumers would see inconsistent data. A milestoned
        version of this job can avoid this issue by used a view which filters for live records in the last committed batch, not the
        current one."""
        # Map the dataset name if necessary
        stagingTableName: str = self.getStagingTableNameForDataset(dataset)
        mergeTableName: str = self.getMergeTableNameForDataset(dataset)

        # Get all column names
        allColumns: List[str] = [col.name for col in dataset.schema.columns]

        with mergeEngine.begin() as connection:
            # Get metrics before merge
            countResult = connection.execute(text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE ds_surf_batch_id = {batchId}"))
            totalRecords = countResult.fetchone()[0]

            # Use ds_surf_key_hash as the join key
            join_key = "ds_surf_key_hash"
            all_columns_list = ", ".join(allColumns)
            all_columns_with_prefix = ", ".join([f"s.{col}" for col in allColumns])

            merge_sql = f"""
            MERGE INTO {mergeTableName} m
            USING {stagingTableName} s
            ON s.{join_key} = m.{join_key}
            WHEN MATCHED AND s.ds_surf_all_hash != m.ds_surf_all_hash THEN
                UPDATE SET
                    {', '.join([f"{col} = s.{col}" for col in allColumns])},
                    ds_surf_batch_id = {batchId},
                    ds_surf_all_hash = s.ds_surf_all_hash,
                    ds_surf_key_hash = s.ds_surf_key_hash
            WHEN NOT MATCHED THEN
                INSERT ({all_columns_list}, ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash)
                VALUES ({all_columns_with_prefix}, {batchId}, s.ds_surf_all_hash, s.ds_surf_key_hash)
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE
            """

            # Execute the MERGE
            connection.execute(text(merge_sql))

            # Get the number of rows affected
            inserted_result = connection.execute(text(
                f"SELECT COUNT(*) FROM {mergeTableName} WHERE ds_surf_batch_id = {batchId}"
            ))
            recordsInserted = inserted_result.fetchone()[0]

            # For updated records, we need to check records that have the same key but different hash
            updated_result = connection.execute(text(f"""
                SELECT COUNT(*) FROM {mergeTableName} m
                INNER JOIN {stagingTableName} s ON s.{join_key} = m.{join_key}
                WHERE s.ds_surf_batch_id = {batchId}
                AND s.ds_surf_all_hash != m.ds_surf_all_hash
                AND m.ds_surf_batch_id != {batchId}
            """))
            recordsUpdated = updated_result.fetchone()[0]

            # Now update the batch status to merged within the existing transaction
            self.markBatchMerged(connection, key, batchId, BatchStatus.MERGED,
                                 recordsInserted, recordsUpdated, 0, totalRecords)

        return recordsInserted, recordsUpdated, totalRecords

    def checkBatchStatus(self, mergeEngine: Engine, key: str) -> Optional[str]:
        """Check the current batch status for a given key. Returns the status or None if no batch exists."""
        with mergeEngine.connect() as connection:
            result = connection.execute(text(f"""
                SELECT bm.batch_status
                FROM {self.getBatchMetricsTableName()} bm
                INNER JOIN {self.getBatchCounterTableName()} bc ON bc.key = bm.key
                WHERE bm.key = '{key}' AND bm.batch_id = bc.currentBatch
            """))
            row = result.fetchone()
            return row[0] if row else None

    def run(self) -> JobStatus:
        # First, get a connection to the source database
        cmd: SQLSnapshotIngestion = cast(SQLSnapshotIngestion, self.store.cmd)
        assert cmd.credential is not None

        # Now, get a connection to the merge database
        mergeUser, mergePassword = self.credStore.getAsUserPassword(self.dp.postgresCredential)
        mergeEngine: Engine = self.createEngine(self.dp.mergeStore, mergeUser, mergePassword)

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
                currentStatus = self.checkBatchStatus(mergeEngine, key)

                if currentStatus is None:
                    # No batch exists, start a new one
                    batchId = self.startBatch(mergeEngine, self.store, dataset)
                    print(f"Started new batch {batchId} for {key}")
                    return JobStatus.KEEP_WORKING

                elif currentStatus == BatchStatus.STARTED.value:
                    # Batch is started, continue with ingestion
                    batchId = self.getCurrentBatchId(mergeEngine, key)
                    print(f"Continuing batch {batchId} for {key} (status: {currentStatus})")
                    return JobStatus.KEEP_WORKING

                elif currentStatus == BatchStatus.INGESTING.value:
                    # Batch is ingested, continue with merge
                    batchId = self.getCurrentBatchId(mergeEngine, key)
                    print(f"Continuing batch {batchId} for {key} (status: {currentStatus})")
                    return JobStatus.KEEP_WORKING

                elif currentStatus == BatchStatus.MERGED.value:
                    # Batch is merged, mark as committed
                    batchId = self.getCurrentBatchId(mergeEngine, key)
                    self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.COMMITTED)
                    print(f"Committed batch {batchId} for {key}")
                    return JobStatus.DONE

                elif currentStatus == BatchStatus.COMMITTED.value:
                    # Batch is already committed, we're done
                    print(f"Batch for {key} is already committed")
                    return JobStatus.DONE

                elif currentStatus == BatchStatus.FAILED.value:
                    # Batch failed, we're done
                    print(f"Batch for {key} failed")
                    return JobStatus.DONE
        else:
            # For multi-dataset ingestion, process all datasets in a single batch
            key = self.store.name
            currentStatus = self.checkBatchStatus(mergeEngine, key)

            if currentStatus is None:
                # No batch exists, start a new one
                batchId = self.startBatch(mergeEngine, self.store, None)
                print(f"Started new multi-dataset batch {batchId} for {key}")
                return JobStatus.KEEP_WORKING

            elif currentStatus == BatchStatus.STARTED.value:
                # Batch is started, continue with ingestion
                batchId = self.getCurrentBatchId(mergeEngine, key)
                print(f"Continuing multi-dataset batch {batchId} for {key} (status: {currentStatus})")
                return JobStatus.KEEP_WORKING

            elif currentStatus == BatchStatus.INGESTED.value:
                # Batch is ingested, continue with merge
                batchId = self.getCurrentBatchId(mergeEngine, key)
                print(f"Continuing multi-dataset batch {batchId} for {key} (status: {currentStatus})")
                return JobStatus.KEEP_WORKING

            elif currentStatus == BatchStatus.MERGED.value:
                # Batch is merged, mark as committed
                batchId = self.getCurrentBatchId(mergeEngine, key)
                self.updateBatchStatusInTx(mergeEngine, key, batchId, BatchStatus.COMMITTED)
                print(f"Committed multi-dataset batch {batchId} for {key}")
                return JobStatus.DONE

            elif currentStatus == BatchStatus.COMMITTED.value:
                # Batch is already committed, we're done
                print(f"Multi-dataset batch for {key} is already committed")
                return JobStatus.DONE

            elif currentStatus == BatchStatus.FAILED.value:
                # Batch failed, we're done
                print(f"Multi-dataset batch for {key} failed")
                return JobStatus.DONE

    def getCurrentBatchId(self, mergeEngine: Engine, key: str) -> int:
        """Get the current batch ID for a given key."""
        with mergeEngine.connect() as connection:
            result = connection.execute(text(f"SELECT currentBatch FROM {self.getBatchCounterTableName()} WHERE key = '{key}'"))
            row = result.fetchone()
            if row is None:
                raise Exception(f"No batch counter found for key {key}")
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

    eco: Ecosystem
    tree: ValidationTree
    eco, tree = loadEcosystemFromEcoModule(args.git_repo_path)
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
        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco, dp.name)
        root: Optional[PlatformPipelineGraph] = graph.roots.get(dp)
        if root is None:
            print(f"Unknown graph for platform: {args.platform_name}")
            return -1  # ERROR
        # Is this datastore being ingested by this platform?
        if root.storesToIngest.get(args.store_name) is None:
            print(f"Datastore {args.store_name} is not being ingested by platform: {args.platform_name}")
            return -1  # ERROR

        store: Optional[Datastore] = eco.getDatastore(args.store_name)
        if store is None:
            print(f"Unknown store: {args.store_name}")
            return -1  # ERROR

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

        if store.cmd.credential is None:
            print(f"Store {args.store_name} has no credential")
            return -1  # ERROR

        if args.dataset_name:
            dataset: Optional[Dataset] = store.datasets.get(args.dataset_name)
            if dataset is None:
                print(f"Unknown dataset: {args.dataset_name}")
                return -1  # ERROR

        job: SnapshotMergeJob = SnapshotMergeJob(eco, dp.credentialStore, dp, store)

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
