"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem, CredentialStore, SQLSnapshotIngestion, Dataset, IngestionConsistencyType
)
from sqlalchemy import Table, MetaData, text
import sqlalchemy
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.schema import Column
from datasurface.md.schema import DDLTable
from sqlalchemy.types import Integer, String, TIMESTAMP
from sqlalchemy.engine.row import Row
from enum import Enum
from typing import cast, List, Any, Optional
from datasurface.md.governance import DatastoreCacheEntry, EcosystemPipelineGraph, PlatformPipelineGraph, SQLIngestion, DataTransformerOutput
from datasurface.md.lint import ValidationTree
from datasurface.platforms.yellow.db_utils import createEngine

from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform, YellowSchemaProjector, YellowMilestoneStrategy, BatchStatus, BatchState
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable, createOrUpdateTable
from datasurface.cmd.platform import getLatestModelAtTimestampedFolder
import argparse
import sys
import json
import hashlib
from abc import ABC, abstractmethod
from datasurface.md.repo import GitHubRepository
import os


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


class JobStatus(Enum):
    """This is the status of a job"""
    KEEP_WORKING = 1  # The job is still in progress, put on queue and continue ASAP
    DONE = 0  # The job is complete, wait for trigger to run batch again
    ERROR = -1  # The job failed, stop the job and don't run again


class JobUtilities(ABC):
    """This class provides utilities for working with jobs in the Yellow platform."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform) -> None:
        self.eco: Ecosystem = eco
        self.dp: YellowDataPlatform = dp
        self.credStore: CredentialStore = credStore
        self.schemaProjector: Optional[YellowSchemaProjector] = cast(YellowSchemaProjector, self.dp.createSchemaProjector(eco))

    def getBatchCounterTableName(self) -> str:
        """This returns the name of the batch counter table"""
        return self.dp.getTableForPlatform("batch_counter")

    def getBatchMetricsTableName(self) -> str:
        """This returns the name of the batch metrics table"""
        return self.dp.getTableForPlatform("batch_metrics")

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

    def getRawBaseTableNameForDataset(self, store: Datastore, dataset: Dataset) -> str:
        """This returns the base table name for a dataset"""
        if isinstance(store.cmd, SQLIngestion):
            return f"{store.name}_{dataset.name if dataset.name not in store.cmd.tableForDataset else store.cmd.tableForDataset[dataset.name]}"
        elif isinstance(store.cmd, DataTransformerOutput):
            return f"{store.name}_{dataset.name}"  # ✅ Simple base name without circular call
        else:
            raise Exception(f"Unknown store command type: {type(store.cmd)}")

    def getRawMergeTableNameForDataset(self, store: Datastore, dataset: Dataset) -> str:
        """This returns the merge table name for a dataset"""
        tableName: str = self.getRawBaseTableNameForDataset(store, dataset)
        return self.dp.getTableForPlatform(tableName + "_merge")

    def getDataTransformerOutputTableNameForDatasetForIngestionOnly(self, store: Datastore, dataset: Dataset) -> str:
        """This returns the DataTransformer output table name for a dataset (dt_ prefix) for ingestion only. Merged tables are stored in the
        normal merge table notation. The dt prefix is ONLY used for the output tables for a DataTransformer when doing the ingestion
        of these output tables. Once the data is ingested for output datastores, the data is merged in to the normal merge tables."""
        tableName: str = self.getRawBaseTableNameForDataset(store, dataset)
        return self.dp.getTableForPlatform(f"dt_{tableName}")


class YellowDatasetUtilities(JobUtilities):
    """This class provides utilities for working with datasets in the Yellow platform."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform, store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp)
        self.store: Datastore = store
        self.datasetName: Optional[str] = datasetName
        self.dataset: Optional[Dataset] = self.store.datasets[datasetName] if datasetName is not None else None

    def checkForSchemaChanges(self, state: BatchState) -> None:
        """Check if any dataset schemas have changed since batch start"""
        for dataset_name in state.all_datasets:
            dataset = self.store.datasets[dataset_name]
            stored_hash = state.schema_versions.get(dataset_name)
            if stored_hash and not self.validateSchemaUnchanged(dataset, stored_hash):
                raise Exception(f"Schema changed for dataset {dataset_name} during batch processing")

    def getKey(self) -> str:
        """This returns the key for the batch"""
        return f"{self.store.name}#{self.dataset.name}" if self.dataset is not None else self.store.name

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the staging schema for a dataset"""
        stagingDS: Dataset = self.schemaProjector.computeSchema(dataset, self.schemaProjector.SCHEMA_TYPE_STAGING)
        t: Table = datasetToSQLAlchemyTable(stagingDS, tableName, sqlalchemy.MetaData())
        return t

    def getMergeSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the merge schema for a dataset"""
        mergeDS: Dataset = self.schemaProjector.computeSchema(dataset, self.schemaProjector.SCHEMA_TYPE_MERGE)
        t: Table = datasetToSQLAlchemyTable(mergeDS, tableName, sqlalchemy.MetaData())
        return t

    def getBaseTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the base table name for a dataset"""
        return self.getRawBaseTableNameForDataset(self.store, dataset)

    def getStagingTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the staging table name for a dataset"""
        tableName: str = self.getBaseTableNameForDataset(dataset)
        return self.dp.getTableForPlatform(tableName + "_staging")

    def getMergeTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the merge table name for a dataset"""
        return self.getRawMergeTableNameForDataset(self.store, dataset)


class Job(YellowDatasetUtilities):
    """This is the base class for all jobs. The batch counter and batch_metric/state tables are likely to be common across batch implementations. The 2
    step process, stage and then merge is also likely to be common. Some may use external staging but then it's just a noop stage with a merge."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: YellowDataPlatform, store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp, store, datasetName)

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

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the staging schema for a dataset"""
        stagingDS: Dataset = self.schemaProjector.computeSchema(dataset, self.schemaProjector.SCHEMA_TYPE_STAGING)
        t: Table = datasetToSQLAlchemyTable(stagingDS, tableName, sqlalchemy.MetaData())
        return t

    def getMergeSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the merge schema for a dataset"""
        mergeDS: Dataset = self.schemaProjector.computeSchema(dataset, self.schemaProjector.SCHEMA_TYPE_MERGE)
        t: Table = datasetToSQLAlchemyTable(mergeDS, tableName, sqlalchemy.MetaData())
        return t

    def createStagingTableIndexes(self, mergeEngine: Engine, tableName: str) -> None:
        """Create performance indexes for staging tables"""
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        indexes = [
            # Primary: batch filtering (used in every query)
            (f"idx_{tableName}_batch_id", f"CREATE INDEX IF NOT EXISTS idx_{tableName}_batch_id ON {tableName} ({sp.BATCH_ID_COLUMN_NAME})"),

            # Secondary: join performance
            (f"idx_{tableName}_key_hash", f"CREATE INDEX IF NOT EXISTS idx_{tableName}_key_hash ON {tableName} ({sp.KEY_HASH_COLUMN_NAME})"),

            # Composite: optimal for most queries that filter by batch AND join by key
            (f"idx_{tableName}_batch_key",
             f"CREATE INDEX IF NOT EXISTS idx_{tableName}_batch_key ON {tableName} ({sp.BATCH_ID_COLUMN_NAME}, {sp.KEY_HASH_COLUMN_NAME})")
        ]

        with mergeEngine.begin() as connection:
            for index_name, index_sql in indexes:
                # Check if index already exists
                check_sql = """
                SELECT COUNT(*) FROM pg_indexes
                WHERE indexname = :index_name AND tablename = :table_name
                """
                result = connection.execute(text(check_sql), {"index_name": index_name, "table_name": tableName})
                index_exists = result.fetchone()[0] > 0

                if not index_exists:
                    try:
                        connection.execute(text(index_sql))
                        print(f"DEBUG: Created index: {index_name}")
                    except Exception as e:
                        print(f"DEBUG: Failed to create index {index_name}: {e}")

    def createMergeTableIndexes(self, mergeEngine: Engine, tableName: str) -> None:
        """Create performance indexes for merge tables"""
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        indexes = [
            # Critical: join performance (exists in all modes)
            (f"idx_{tableName}_key_hash", f"CREATE INDEX IF NOT EXISTS idx_{tableName}_key_hash ON {tableName} ({sp.KEY_HASH_COLUMN_NAME})")
        ]

        # Add forensic-specific indexes for batch milestoned tables
        if self.dp.milestoneStrategy == YellowMilestoneStrategy.BATCH_MILESTONED:
            indexes.extend([
                # Critical: live record filtering (batch_out = 2147483647)
                (f"idx_{tableName}_batch_out", f"CREATE INDEX IF NOT EXISTS idx_{tableName}_batch_out ON {tableName} ({sp.BATCH_OUT_COLUMN_NAME})"),

                # Composite: optimal for live record joins (most common pattern)
                (f"idx_{tableName}_live_records",
                 f"CREATE INDEX IF NOT EXISTS idx_{tableName}_live_records ON {tableName} ({sp.KEY_HASH_COLUMN_NAME}, {sp.BATCH_OUT_COLUMN_NAME})"),

                # For forensic queries: finding recently closed records
                (f"idx_{tableName}_batch_in", f"CREATE INDEX IF NOT EXISTS idx_{tableName}_batch_in ON {tableName} ({sp.BATCH_IN_COLUMN_NAME})"),

                # For forensic history queries
                (f"idx_{tableName}_batch_range",
                 f"CREATE INDEX IF NOT EXISTS idx_{tableName}_batch_range ON {tableName} ({sp.BATCH_IN_COLUMN_NAME}, {sp.BATCH_OUT_COLUMN_NAME})")
            ])
        else:
            # For live-only mode, we can create an index on ds_surf_all_hash for change detection
            indexes.extend([
                # For live-only mode: change detection performance
                (f"idx_{tableName}_all_hash", f"CREATE INDEX IF NOT EXISTS idx_{tableName}_all_hash ON {tableName} ({sp.ALL_HASH_COLUMN_NAME})")
            ])

        with mergeEngine.begin() as connection:
            for index_name, index_sql in indexes:
                # Check if index already exists
                check_sql = """
                SELECT COUNT(*) FROM pg_indexes
                WHERE indexname = :index_name AND tablename = :table_name
                """
                result = connection.execute(text(check_sql), {"index_name": index_name, "table_name": tableName})
                index_exists = result.fetchone()[0] > 0

                if not index_exists:
                    try:
                        connection.execute(text(index_sql))
                        print(f"DEBUG: Created index: {index_name}")
                    except Exception as e:
                        print(f"DEBUG: Failed to create index {index_name}: {e}")

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

    def createOrUpdateForensicTable(self, mergeEngine: Engine, mergeTable: Table, tableName: str) -> None:
        """Create or update a forensic table, handling primary key changes by dropping and recreating if needed"""
        from sqlalchemy import inspect
        inspector = inspect(mergeEngine)

        if not inspector.has_table(tableName):
            # Table doesn't exist, create it
            mergeTable.create(mergeEngine)
            print(f"Created forensic table {tableName}")
            return

        # Table exists, check if primary key needs to be updated
        current_table = Table(tableName, MetaData(), autoload_with=mergeEngine)

        # Get current primary key columns
        current_pk_columns = []
        for col in current_table.columns:
            if col.primary_key:
                current_pk_columns.append(col.name)

        # Get desired primary key columns from the merge table
        desired_pk_columns = []
        for col in mergeTable.columns:
            if col.primary_key:
                desired_pk_columns.append(col.name)

        # Check if primary key needs to be changed
        if set(current_pk_columns) != set(desired_pk_columns):
            print(f"DEBUG: Primary key change detected for {tableName}")
            print(f"DEBUG: Current PK: {current_pk_columns}")
            print(f"DEBUG: Desired PK: {desired_pk_columns}")

            # Drop and recreate the table with new primary key
            with mergeEngine.begin() as connection:
                # Drop the existing table
                connection.execute(text(f"DROP TABLE {tableName} CASCADE"))
                print(f"DEBUG: Dropped table {tableName}")

            # Create the new table
            mergeTable.create(mergeEngine)
            print(f"DEBUG: Recreated table {tableName} with new primary key")
        else:
            # Primary key is the same, use normal update
            createOrUpdateTable(mergeEngine, mergeTable)

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
        key: str = self.getKey()
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
        key: str = self.getKey()
        allDatasets: List[str] = list(store.datasets.keys())

        # Create schema hashes for all datasets
        schema_versions = {}
        for dataset_name in allDatasets:
            dataset = store.datasets[dataset_name]
            schema_versions[dataset_name] = self.getSchemaHash(dataset)

        # Start with the first dataset and the rest of the datasets to go
        state: BatchState = BatchState(all_datasets=allDatasets, schema_versions=schema_versions)  # Start with the first dataset
        return self.createBatchCommon(connection, key, state)

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
            state: BatchState = self.getBatchState(mergeEngine, connection, self.getKey(), newBatchId)
            # Truncate the staging table for each dataset in the batch state
            for datasetName in state.all_datasets:
                dataset: Dataset = self.store.datasets[datasetName]
                stagingTableName: str = self.getStagingTableNameForDataset(dataset)
                connection.execute(text(f"TRUNCATE TABLE {stagingTableName}"))
        return newBatchId

    def getBatchState(self, mergeEngine: Engine, connection: Connection, key: str, batchId: int) -> BatchState:
        """Get the batch status for a given key and batch id"""
        result = connection.execute(text(f'SELECT "state" FROM {self.getBatchMetricsTableName()} WHERE "key" = \'{key}\' AND "batch_id" = {batchId}'))
        row = result.fetchone()

        return BatchState.model_validate_json(row[0]) if row else BatchState(all_datasets=[])

    def checkBatchStatus(self, connection: Connection, key: str, batchId: int) -> Optional[str]:
        """Check the current batch status for a given key. Returns the status or None if no batch exists."""
        result = connection.execute(text(f"""
            SELECT bm."batch_status"
            FROM {self.getBatchMetricsTableName()} bm
            WHERE bm."key" = '{key}' AND bm."batch_id" = {batchId}
        """))
        row = result.fetchone()
        return row[0] if row else None

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

            update_sql = f'UPDATE {self.getBatchMetricsTableName()} SET {", ".join(update_parts)} WHERE "key" = :key AND "batch_id" = :batch_id'
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

        update_sql = f'UPDATE {self.getBatchMetricsTableName()} SET {", ".join(update_parts)} WHERE "key" = \'{key}\' AND "batch_id" = {batchId}'
        connection.execute(text(update_sql))

    @abstractmethod
    def ingestNextBatchToStaging(self, sourceEngine: Engine, mergeEngine: Engine, key: str, batchId: int) -> tuple[int, int, int]:
        """This will ingest the next batch to staging"""
        pass

    @abstractmethod
    def mergeStagingToMerge(self, mergeEngine: Engine, batchId: int, key: str, batch_size: int = 10000) -> tuple[int, int, int]:
        """This will merge the staging table to the merge table"""
        pass

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

    def reconcileStagingTableSchemas(self, mergeEngine: Engine, store: Datastore) -> None:
        """This will make sure the staging table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = self.getStagingTableNameForDataset(dataset)
            stagingTable: Table = self.getStagingSchemaForDataset(dataset, tableName)
            createOrUpdateTable(mergeEngine, stagingTable)
            # Create performance indexes for staging table
            self.createStagingTableIndexes(mergeEngine, tableName)

    def reconcileMergeTableSchemas(self, mergeEngine: Engine, store: Datastore) -> None:
        """This will make sure the merge table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = self.getMergeTableNameForDataset(dataset)
            mergeTable: Table = self.getMergeSchemaForDataset(dataset, tableName)

            # For forensic mode, we need to handle primary key changes
            if self.dp.milestoneStrategy == YellowMilestoneStrategy.BATCH_MILESTONED:
                self.createOrUpdateForensicTable(mergeEngine, mergeTable, tableName)
            else:
                createOrUpdateTable(mergeEngine, mergeTable)
                # Only add unique constraint on key_hash for live-only mode
                # In forensic mode, multiple records can have the same key_hash (different versions)
                self.ensureUniqueConstraintExists(mergeEngine, tableName, "ds_surf_key_hash")

            # Create performance indexes for merge table
            self.createMergeTableIndexes(mergeEngine, tableName)

    def run(self) -> JobStatus:
        # Check if this is a DataTransformer output store
        isDataTransformerOutput = isinstance(self.store.cmd, DataTransformerOutput)

        # Now, get a connection to the merge database
        mergeUser, mergePassword = self.credStore.getAsUserPassword(self.dp.postgresCredential)
        mergeEngine: Engine = createEngine(self.dp.mergeStore, mergeUser, mergePassword)

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
                key = f"{self.store.name}#{dataset.name}"
                currentStatus = self.executeBatch(sourceEngine, mergeEngine, key)
        else:
            # For multi-dataset ingestion, process all datasets in a single batch
            key = self.store.name
            currentStatus = self.executeBatch(sourceEngine, mergeEngine, key)
        return currentStatus

    def getSourceTableName(self, dataset: Dataset) -> str:
        """This returns the source table name for a dataset"""
        if isinstance(self.store.cmd, DataTransformerOutput):
            return self.getDataTransformerOutputTableNameForDatasetForIngestionOnly(self.store, dataset)
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
        print(f"DEBUG: Starting ingestion for batch {batchId}, hasMoreDatasets: {state.hasMoreDatasets()}")
        recordsInserted = 0  # Initialize counter for all datasets
        totalRecords = 0  # Initialize total records counter
        with sourceEngine.connect() as sourceConn:
            while state.hasMoreDatasets():  # While there is a dataset to ingest
                # Get source table name, Map the dataset name if necessary
                datasetToIngestName: str = state.getCurrentDataset()
                dataset: Dataset = self.store.datasets[datasetToIngestName]

                # Get source table name - for DataTransformer output, use dt_ prefixed tables
                if isinstance(self.store.cmd, DataTransformerOutput):
                    sourceTableName: str = self.getDataTransformerOutputTableNameForDatasetForIngestionOnly(self.store, dataset)
                else:
                    # Get source table name using mapping if necessary
                    sourceTableName: str = self.getSourceTableName(dataset)

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
                        MD5({allColumnsHashExpr}) as {self.schemaProjector.ALL_HASH_COLUMN_NAME},
                        MD5({keyColumnsHashExpr}) as {self.schemaProjector.KEY_HASH_COLUMN_NAME}
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
                        quoted_columns = [f'"{col}"' for col in allColumns] + \
                            [
                                f'"{self.schemaProjector.BATCH_ID_COLUMN_NAME}"',
                                f'"{self.schemaProjector.ALL_HASH_COLUMN_NAME}"',
                                f'"{self.schemaProjector.KEY_HASH_COLUMN_NAME}"']
                        placeholders = ", ".join([f":{i}" for i in range(len(quoted_columns))])
                        insertSql = f"INSERT INTO {stagingTableName} ({', '.join(quoted_columns)}) VALUES ({placeholders})"

                        # Execute batch insert using proper SQLAlchemy batch execution
                        # Use executemany for true batch efficiency - single execute call for all rows
                        all_params = [{str(i): val for i, val in enumerate(values)} for values in batchValues]
                        mergeConn.execute(text(insertSql), all_params)
                        numRowsInserted: int = len(batchValues)
                        recordsInserted += numRowsInserted
                        # Write the offset to the state
                        state.current_offset = offset + numRowsInserted
                        # Update the state in the batch metrics table
                        mergeConn.execute(text(
                            f'UPDATE {self.getBatchMetricsTableName()} SET "state" = :state'
                            f' WHERE "key" = :key AND "batch_id" = :batch_id'), {
                                "state": state.model_dump_json(),
                                "key": key,
                                "batch_id": batchId
                            })

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

    def mergeStagingToMerge(self, mergeEngine: Engine, batchId: int, key: str, batch_size: int = 10000) -> tuple[int, int, int]:
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
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        with mergeEngine.begin() as connection:
            state: BatchState = self.getBatchState(mergeEngine, connection, key, batchId)
            self.checkForSchemaChanges(state)

            for datasetToMergeName in state.all_datasets:
                dataset: Dataset = self.store.datasets[datasetToMergeName]
                stagingTableName: str = self.getStagingTableNameForDataset(dataset)
                mergeTableName: str = self.getMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)
                allColumns: list[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = [f'"{col}"' for col in allColumns]

                # PostgreSQL 16 compatible forensic merge operations - split into multiple statements
                # Step 1: Close changed records (equivalent to WHEN MATCHED AND hash differs)
                close_changed_sql = f"""
                UPDATE {mergeTableName} m
                SET {sp.BATCH_OUT_COLUMN_NAME} = {batchId - 1}
                FROM {stagingTableName} s
                WHERE m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                    AND m.{sp.BATCH_OUT_COLUMN_NAME} = {sp.LIVE_RECORD_ID}
                    AND m.{sp.ALL_HASH_COLUMN_NAME} != s.{sp.ALL_HASH_COLUMN_NAME}
                    AND s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
                """

                # Step 2: Close records not in source (equivalent to WHEN NOT MATCHED BY SOURCE)
                close_deleted_sql = f"""
                UPDATE {mergeTableName} m
                SET {sp.BATCH_OUT_COLUMN_NAME} = {batchId - 1}
                WHERE m.{sp.BATCH_OUT_COLUMN_NAME} = {sp.LIVE_RECORD_ID}
                AND NOT EXISTS (
                    SELECT 1 FROM {stagingTableName} s
                    WHERE s.{sp.KEY_HASH_COLUMN_NAME} = m.{sp.KEY_HASH_COLUMN_NAME}
                    AND s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
                )
                """

                # Step 3: Insert new records (equivalent to WHEN NOT MATCHED BY TARGET)
                insert_new_sql = f"""
                INSERT INTO {mergeTableName} (
                    {', '.join(quoted_all_columns)},
                    {sp.BATCH_ID_COLUMN_NAME},
                    {sp.ALL_HASH_COLUMN_NAME},
                    {sp.KEY_HASH_COLUMN_NAME},
                    {sp.BATCH_IN_COLUMN_NAME},
                    {sp.BATCH_OUT_COLUMN_NAME}
                )
                SELECT
                    {', '.join([f's."{col}"' for col in allColumns])},
                    {batchId},
                    s.{sp.ALL_HASH_COLUMN_NAME},
                    s.{sp.KEY_HASH_COLUMN_NAME},
                    {batchId},
                    {sp.LIVE_RECORD_ID}
                FROM {stagingTableName} s
                WHERE s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
                AND NOT EXISTS (
                    SELECT 1 FROM {mergeTableName} m
                    WHERE m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                    AND m.{sp.BATCH_OUT_COLUMN_NAME} = {sp.LIVE_RECORD_ID}
                )
                """

                print(f"DEBUG: Executing PostgreSQL 16 compatible forensic merge for dataset {datasetToMergeName}")
                print("DEBUG: Step 1 - Closing changed records")
                result1 = connection.execute(text(close_changed_sql))
                changed_records = result1.rowcount
                total_updated += changed_records
                print(f"DEBUG: Step 1 - Closed {changed_records} changed records")

                print("DEBUG: Step 2 - Closing deleted records")
                result2 = connection.execute(text(close_deleted_sql))
                deleted_records = result2.rowcount
                total_deleted += deleted_records
                print(f"DEBUG: Step 2 - Closed {deleted_records} deleted records")

                print("DEBUG: Step 3 - Inserting new records")
                result3 = connection.execute(text(insert_new_sql))
                new_records = result3.rowcount
                total_inserted += new_records
                print(f"DEBUG: Step 3 - Inserted {new_records} new records")

                # Insert new versions for changed records (where the old record was just closed)
                insert_changed_sql = f"""
                INSERT INTO {mergeTableName} ({', '.join(quoted_all_columns)}, {sp.BATCH_ID_COLUMN_NAME},
                    {sp.ALL_HASH_COLUMN_NAME}, {sp.KEY_HASH_COLUMN_NAME}, {sp.BATCH_IN_COLUMN_NAME}, {sp.BATCH_OUT_COLUMN_NAME})
                SELECT {', '.join([f's."{col}"' for col in allColumns])}, {batchId}, s.{sp.ALL_HASH_COLUMN_NAME},
                    s.{sp.KEY_HASH_COLUMN_NAME}, {batchId}, {sp.LIVE_RECORD_ID}
                FROM {stagingTableName} s
                WHERE s.{sp.BATCH_ID_COLUMN_NAME} = {batchId}
                AND EXISTS (
                    SELECT 1 FROM {mergeTableName} m
                    WHERE m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                    AND m.{sp.BATCH_OUT_COLUMN_NAME} = {batchId - 1}
                )
                AND NOT EXISTS (
                    SELECT 1 FROM {mergeTableName} m2
                    WHERE m2.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                    AND m2.{sp.BATCH_IN_COLUMN_NAME} = {batchId}
                )
                """
                print("DEBUG: Step 4 - Inserting new versions for changed records")
                result4 = connection.execute(text(insert_changed_sql))
                changed_new_records = result4.rowcount
                total_inserted += changed_new_records
                print(f"DEBUG: Step 4 - Inserted {changed_new_records} new versions for changed records")

                # Count total records processed from staging for this dataset
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                print(f"DEBUG: Dataset {datasetToMergeName} - New: {new_records}, Changed: {changed_records}, "
                      f"Deleted: {deleted_records}, Changed New Versions: {changed_new_records}")

            # Now update the batch status to merged within the existing transaction
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        print(f"DEBUG: Total forensic merge results - Inserted: {total_inserted}, Updated: {total_updated}, Deleted: {total_deleted}")
        return total_inserted, total_updated, total_deleted


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

    def mergeStagingToMerge(self, mergeEngine: Engine, batchId: int, key: str, batch_size: int = 10000) -> tuple[int, int, int]:
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
                stagingTableName: str = self.getStagingTableNameForDataset(dataset)
                mergeTableName: str = self.getMergeTableNameForDataset(dataset)
                schema: DDLTable = cast(DDLTable, dataset.originalSchema)

                # Get all column names
                allColumns: List[str] = [col.name for col in schema.columns.values()]
                quoted_all_columns = [f'"{col}"' for col in allColumns]

                # Get total count for processing
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {self.schemaProjector.BATCH_ID_COLUMN_NAME} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                print(f"DEBUG: Processing {total_records} records for dataset {datasetToMergeName} in batches of {batch_size}")

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

                    print(f"DEBUG: Executing batch upsert for offset {offset}")
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
                print(f"DEBUG: Executing deletion query for dataset {datasetToMergeName}")
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
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

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
                count_result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {stagingTableName} WHERE {sp.BATCH_ID_COLUMN_NAME} = {batchId}"))
                total_records = count_result.fetchone()[0]
                totalRecords += total_records

                print(f"DEBUG: Processing {total_records} records for dataset {datasetToMergeName} using MERGE with DELETE")

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
                temp_staging_table = f"temp_staging_{batchId}_{datasetToMergeName.replace('-', '_')}"
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

                print(f"DEBUG: Executing MERGE with DELETE for dataset {datasetToMergeName}")
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

                print(f"DEBUG: Dataset {datasetToMergeName} - Inserted: {dataset_inserted}, Updated: {dataset_updated}, Deleted: {dataset_deleted}")

            # Now update the batch status to merged within the existing transaction
            self.markBatchMerged(
                connection, key, batchId, BatchStatus.COMMITTED,
                total_inserted, total_updated, total_deleted, totalRecords)

        print(f"DEBUG: Total merge results - Inserted: {total_inserted}, Updated: {total_updated}, Deleted: {total_deleted}")
        return total_inserted, total_updated, total_deleted


def main():
    """Main entry point for the SnapshotMergeJob when run as a command-line tool."""
    parser = argparse.ArgumentParser(description='Run SnapshotMergeJob for a specific ingestion stream')
    parser.add_argument('--platform-name', required=True, help='Name of the platform')
    parser.add_argument('--store-name', required=True, help='Name of the datastore')
    parser.add_argument('--dataset-name', help='Name of the dataset (for single dataset ingestion)')
    parser.add_argument('--operation', default='snapshot-merge', help='Operation to perform')
    parser.add_argument('--git-repo-path', required=True, help='Path to the git repository')
    parser.add_argument('--git-repo-owner', required=True, help='GitHub repository owner (e.g., billynewport)')
    parser.add_argument('--git-repo-name', required=True, help='GitHub repository name (e.g., mvpmodel)')
    parser.add_argument('--git-repo-branch', required=True, help='GitHub repository branch (e.g., main)')

    args = parser.parse_args()

    # Clone the git repository if the directory is empty

    if not os.path.exists(args.git_repo_path) or not os.listdir(args.git_repo_path):
        git_token = os.environ.get('git_TOKEN')
        if not git_token:
            print("ERROR: git_TOKEN environment variable not found")
            return -1

    # Ensure the directory exists
    os.makedirs(args.git_repo_path, exist_ok=True)

    eco: Optional[Ecosystem] = None
    tree: Optional[ValidationTree] = None
    eco, tree = getLatestModelAtTimestampedFolder(
        GitHubRepository(f"{args.git_repo_owner}/{args.git_repo_name}", args.git_repo_branch), args.git_repo_path, doClone=True)
    if tree is not None and tree.hasErrors():
        print("Ecosystem model has errors")
        tree.printTree()
        return -1  # ERROR
    if eco is None or tree is None:
        print("Failed to load ecosystem")
        return -1  # ERROR

    if args.operation == "snapshot-merge":
        print(f"Running {args.operation} for platform: {args.platform_name}, store: {args.store_name}")
        if args.dataset_name:
            print(f"Dataset: {args.dataset_name}")

        dp: Optional[YellowDataPlatform] = cast(YellowDataPlatform, eco.getDataPlatform(args.platform_name))
        if dp is None:
            print(f"Unknown platform: {args.platform_name}")
            return -1  # ERROR
        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)
        root: Optional[PlatformPipelineGraph] = graph.roots.get(dp.name)
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

        # DataTransformer output stores don't need external credentials
        if not isinstance(store.cmd, DataTransformerOutput):
            cmd = cast(SQLSnapshotIngestion, store.cmd)
            if cmd.credential is None:
                print(f"Store {args.store_name} has no credential")
                return -1  # ERROR

        if args.dataset_name:
            dataset: Optional[Dataset] = store.datasets.get(args.dataset_name)
            if dataset is None:
                print(f"Unknown dataset: {args.dataset_name}")
                return -1  # ERROR

        job: Job
        if dp.milestoneStrategy == YellowMilestoneStrategy.LIVE_ONLY:
            job = SnapshotMergeJobLiveOnly(eco, dp.getCredentialStore(), cast(YellowDataPlatform, dp), store, args.dataset_name)
        elif dp.milestoneStrategy == YellowMilestoneStrategy.BATCH_MILESTONED:
            job = SnapshotMergeJobForensic(eco, dp.getCredentialStore(), cast(YellowDataPlatform, dp), store, args.dataset_name)
        else:
            print(f"Unknown milestone strategy: {dp.milestoneStrategy}")
            return -1  # ERROR

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
    try:
        exit_code = main()
        print(f"DATASURFACE_RESULT_CODE={exit_code}")
    except Exception as e:
        print(f"Unhandled exception in main: {e}")
        import traceback
        traceback.print_exc()
        print("DATASURFACE_RESULT_CODE=-1")
        exit_code = -1
    # Always exit with 0 (success) - Airflow will parse the result code from logs
    sys.exit(0)
