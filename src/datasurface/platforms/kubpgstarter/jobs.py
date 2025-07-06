# type: ignore[attr-defined, unknown-member, unknown-argument, unknown-variable, unknown-parameter]
"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Datastore, Ecosystem, CredentialStore, SQLSnapshotIngestion, DataContainer, PostgresDatabase, Dataset, IngestionConsistencyType
from sqlalchemy import create_engine, Table, MetaData, text, inspect
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, String, DateTime
from enum import Enum
from typing import cast, List, Any, Optional
from datasurface.platforms.kubpgstarter.kubpgstarter import KubernetesPGStarterDataPlatform
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
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
"""


def _types_are_compatible(current_type: str, desired_type: str) -> bool:
    """Check if two SQLAlchemy type strings represent compatible types."""
    # Remove whitespace and convert to uppercase for comparison
    current = current_type.strip().upper()
    desired = desired_type.strip().upper()

    # If they're exactly the same, they're compatible
    if current == desired:
        return True

    # Handle common PostgreSQL type equivalencies (but be strict about lengths)
    # INTEGER types are often equivalent
    if current in ['INTEGER', 'INT', 'INT4'] and desired in ['INTEGER', 'INT', 'INT4']:
        return True

    # BOOLEAN types
    if current in ['BOOLEAN', 'BOOL'] and desired in ['BOOLEAN', 'BOOL']:
        return True

    # For VARCHAR and CHAR, we need to be strict about lengths
    # Only consider them compatible if they have the same constraints
    # This means VARCHAR(50) and VARCHAR(200) are NOT compatible
    if current.startswith('VARCHAR(') and desired.startswith('VARCHAR('):
        # Extract the length values and compare them
        current_length = _extract_length_from_type(current)
        desired_length = _extract_length_from_type(desired)
        return current_length == desired_length

    if current.startswith('CHAR(') and desired.startswith('CHAR('):
        # Extract the length values and compare them
        current_length = _extract_length_from_type(current)
        desired_length = _extract_length_from_type(desired)
        return current_length == desired_length

    # For DECIMAL/NUMERIC types, compare precision and scale
    if (current.startswith('DECIMAL(') or current.startswith('NUMERIC(')) and \
       (desired.startswith('DECIMAL(') or desired.startswith('NUMERIC(')):
        current_precision, current_scale = _extract_decimal_params(current)
        desired_precision, desired_scale = _extract_decimal_params(desired)
        return current_precision == desired_precision and current_scale == desired_scale

    # If we can't determine compatibility, assume they're different
    return False


def _extract_length_from_type(type_str: str) -> int:
    """Extract the length parameter from a type string like VARCHAR(50)."""
    try:
        # Find the opening and closing parentheses
        start = type_str.find('(')
        end = type_str.find(')')
        if start != -1 and end != -1 and end > start:
            length_str = type_str[start + 1:end]
            return int(length_str)
        return -1  # No length parameter found
    except (ValueError, IndexError):
        return -1  # Invalid format


def _extract_decimal_params(type_str: str) -> tuple[int, int]:
    """Extract precision and scale from a DECIMAL/NUMERIC type string like DECIMAL(10,2)."""
    try:
        # Find the opening and closing parentheses
        start = type_str.find('(')
        end = type_str.find(')')
        if start != -1 and end != -1 and end > start:
            params_str = type_str[start + 1:end]
            params = params_str.split(',')
            if len(params) == 2:
                precision = int(params[0].strip())
                scale = int(params[1].strip())
                return precision, scale
        return -1, -1  # No parameters found
    except (ValueError, IndexError):
        return -1, -1  # Invalid format


def createOrUpdateTable(engine: Engine, table: Table) -> None:
    """This will create the table if it doesn't exist or update it if it does"""
    # If the table doesn't exist, create it
    inspector = inspect(engine)  # type: ignore[attr-defined]
    if not inspector.has_table(table.name):  # type: ignore[attr-defined]
        table.create(engine)
    # If the table exists, check if it has the correct schema
    else:
        # Get the current schema of the table
        currentSchema: Table = Table(table.name, MetaData(), autoload_with=engine)
        # Find new columns to add
        newColumns: List[Column[Any]] = []
        for column in table.columns:
            col_typed: Column[Any] = column  # Type annotation for clarity
            if col_typed.name not in currentSchema.columns:  # type: ignore[attr-defined]
                newColumns.append(col_typed)

        # Find columns that need type changes
        columnsToAlter: List[Column[Any]] = []
        for column in currentSchema.columns:
            col_typed: Column[Any] = column  # Type annotation for clarity
            if col_typed.name not in table.columns:  # type: ignore[attr-defined]
                continue
            # Compare column types more carefully - convert both to string representation
            current_type_str = str(col_typed.type).upper()  # type: ignore[attr-defined]
            desired_type_str = str(table.columns[col_typed.name].type).upper()  # type: ignore[attr-defined]
            # Only consider it a change if the types are meaningfully different
            if current_type_str != desired_type_str and not _types_are_compatible(current_type_str, desired_type_str):
                columnsToAlter.append(table.columns[col_typed.name])  # type: ignore[attr-defined]

        # Execute all schema changes in a single transaction
        if newColumns or columnsToAlter:
            with engine.begin() as connection:
                # Add new columns (these need to be individual statements)
                for column in newColumns:
                    column_type = str(column.type)  # type: ignore[attr-defined]
                    alter_sql = f"ALTER TABLE {table.name} ADD COLUMN {column.name} {column_type}"  # type: ignore[attr-defined]
                    if column.nullable is False:
                        alter_sql += " NOT NULL"
                    connection.execute(text(alter_sql))

                # Alter existing columns (batch multiple alterations into a single statement)
                if columnsToAlter:
                    alter_parts = []
                    for column in columnsToAlter:
                        alter_parts.append(f"ALTER COLUMN {column.name} TYPE {str(column.type)}")  # type: ignore[attr-defined]

                    # Combine all alterations into a single ALTER TABLE statement
                    alter_sql = f"ALTER TABLE {table.name} " + ", ".join(alter_parts)
                    connection.execute(text(alter_sql))

            if newColumns:
                print(f"Added columns to table {table.name}: {[col.name for col in newColumns]}")  # type: ignore[attr-defined]
            if columnsToAlter:
                print(f"Altered columns in table {table.name}: {[col.name for col in columnsToAlter]}")  # type: ignore[attr-defined]


class BatchStatus(Enum):
    """This is the status of a batch"""
    STARTED = "started"
    INGESTING = "ingesting"
    MERGED = "merged"
    COMMITTED = "committed"
    FAILED = "failed"


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
                         Column("total_records", Integer(), nullable=True))
        return t

    def createBatchCounterTable(self, mergeEngine: Engine) -> None:
        """This creates the batch counter table"""
        t: Table = self.getBatchCounterTable()
        createOrUpdateTable(mergeEngine, t)

    def createBatchMetricsTable(self, mergeEngine: Engine) -> None:
        """This creates the batch metrics table"""
        t: Table = self.getBatchMetricsTable()
        createOrUpdateTable(mergeEngine, t)

    def createBatchCommon(self, connection: Connection, key: str) -> int:
        """This creates a batch and returns the batch id. The transaction is managed by the caller.
        This current batch must be commited before a new batch can be created.
        Get the current batch for the store. The first time, there will be no record in the batch counter table.
        In this case, the current batch is 0.
        """
        result = connection.execute(text(f"SELECT currentBatch FROM {self.getBatchCounterTableName()} WHERE key = '{key}'"))
        if result.fetchone() is None:
            currentBatch = 0
        else:
            currentBatch = result.fetchone()[0]

        # Check if the current batch is committed
        result = connection.execute(text(f"SELECT batch_status FROM {self.getBatchMetricsTableName()} WHERE key = '{key}' AND batch_id = {currentBatch}"))
        if result.fetchone() is not None:
            batchStatus: str = result.fetchone()[0]
            if batchStatus != BatchStatus.COMMITTED.value:
                raise Exception(f"Batch {currentBatch} is not committed")

        # Increment the batch counter
        newBatch = currentBatch + 1
        connection.execute(text(f"UPDATE {self.getBatchCounterTableName()} SET currentBatch = {newBatch} WHERE key = '{key}'"))

        # Insert a new batch event record with started status
        connection.execute(text(
            f"INSERT INTO {self.getBatchMetricsTableName()} "
            f"(key, batch_id, batch_start_time, batch_status) "
            f"VALUES ('{key}', {newBatch}, NOW(), '{BatchStatus.STARTED.value}')"
        ))

        return newBatch

    def createSingleBatch(self, store: Datastore, dataset: Dataset, connection: Connection) -> int:
        """This creates a single-dataset batch and returns the batch id. The transaction is managed by the caller."""
        key: str = f"{self.store.name}#{dataset.name}"
        return self.createBatchCommon(connection, key)

    def createMultiBatch(self, store: Datastore, connection: Connection) -> int:
        """This create a multi-dataset batch and returns the batch id. The transaction is managed by the caller.
        This current batch must be commited before a new batch can be created.
        Get the current batch for the store. The first time, there will be no record in the batch counter table.
        In this case, the current batch is 0.
        """
        key: str = self.store.name
        return self.createBatchCommon(connection, key)

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

    def updateBatchStatus(self, mergeEngine: Engine, key: str, batchId: int, status: BatchStatus,
                          recordsInserted: Optional[int] = None, recordsUpdated: Optional[int] = None,
                          recordsDeleted: Optional[int] = None, totalRecords: Optional[int] = None) -> None:
        """Update the batch status and metrics"""
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

    def calculateHash(self, values: List[Any]) -> str:
        """Calculate MD5 hash of concatenated string values"""
        concatenated = "".join(str(val) if val is not None else "" for val in values)
        return hashlib.md5(concatenated.encode('utf-8')).hexdigest()

    def ingestDatasetToStaging(self, sourceEngine: Engine, mergeEngine: Engine, dataset: Dataset,
                               batchId: int, cmd: SQLSnapshotIngestion) -> tuple[int, int, int]:
        """Ingest a dataset from source to staging table"""
        # Map the dataset name if necessary
        tableName: str = dataset.name if dataset.name not in cmd.tableForDataset else cmd.tableForDataset[dataset.name]
        sourceTableName: str = tableName
        stagingTableName: str = self.getStagingTableNameForDataset(dataset)

        # Get primary key columns
        pkColumns: List[str] = [col.name for col in dataset.schema.columns if col.isPrimaryKey]
        if not pkColumns:
            # If no primary key, use all columns
            pkColumns = [col.name for col in dataset.schema.columns]

        # Get all column names
        allColumns: List[str] = [col.name for col in dataset.schema.columns]

        # Read from source table
        with sourceEngine.connect() as sourceConn:
            # Get total count for metrics
            countResult = sourceConn.execute(text(f"SELECT COUNT(*) FROM {sourceTableName}"))
            totalRecords = countResult.fetchone()[0]

            # Read data in batches to avoid memory issues
            batchSize = 1000
            offset = 0
            recordsInserted = 0

            while True:
                # Read batch from source
                selectSql = f"SELECT * FROM {sourceTableName} LIMIT {batchSize} OFFSET {offset}"
                result = sourceConn.execute(text(selectSql))
                rows = result.fetchall()

                if not rows:
                    break

                # Process each row and insert into staging
                with mergeEngine.begin() as mergeConn:
                    for row in rows:
                        # Convert row to list of values
                        values = list(row)

                        # Calculate hashes
                        allHash = self.calculateHash(values)
                        keyHash = self.calculateHash([values[allColumns.index(col)] for col in pkColumns])

                        # Build insert statement
                        columns = allColumns + ["ds_surf_batch_id", "ds_surf_all_hash", "ds_surf_key_hash"]
                        placeholders = ", ".join(["%s"] * len(columns))
                        insertSql = f"INSERT INTO {stagingTableName} ({', '.join(columns)}) VALUES ({placeholders})"

                        # Execute insert
                        insertValues = values + [batchId, allHash, keyHash]
                        mergeConn.execute(text(insertSql), insertValues)
                        recordsInserted += 1

                offset += batchSize

        return recordsInserted, 0, totalRecords  # No updates or deletes in snapshot ingestion

    def mergeStagingToMerge(self, mergeEngine: Engine, dataset: Dataset, batchId: int,
                            cmd: SQLSnapshotIngestion) -> None:
        """Merge staging data into merge table using PostgreSQL MERGE command and ds_surf_key_hash as join key, including deletions."""
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

    def run(self) -> None:
        # First, get a connection to the source database
        cmd: SQLSnapshotIngestion = cast(SQLSnapshotIngestion, self.store.cmd)
        assert cmd.credential is not None

        sourceUser, sourcePassword = self.credStore.getAsUserPassword(cmd.credential)
        assert cmd.dataContainer is not None
        sourceEngine: Engine = self.createEngine(cmd.dataContainer, sourceUser, sourcePassword)

        # Now, get a connection to the merge database
        mergeUser, mergePassword = self.credStore.getAsUserPassword(self.dp.postgresCredential)
        mergeEngine: Engine = self.createEngine(self.dp.mergeStore, mergeUser, mergePassword)

        # Make sure the staging and merge tables exist and have the current schema for each dataset
        self.reconcileStagingTableSchemas(mergeEngine, self.store, cmd)
        self.reconcileMergeTableSchemas(mergeEngine, self.store, cmd)

        # Create batch counter and metrics tables if they don't exist
        self.createBatchCounterTable(mergeEngine)
        self.createBatchMetricsTable(mergeEngine)

        # Start a new batch
        batchId: int
        if cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET:
            # For single dataset ingestion, process each dataset separately
            for dataset in self.store.datasets.values():
                batchId = self.startBatch(mergeEngine, self.store, dataset)
                key = f"{self.store.name}#{dataset.name}"

# This needs to be restartable. If the job is restarted then it depends on the batch_status
# what needs to be done. We may have ingested but not merged for example. If merge fails
# then we need to restart the merge. Updating batch status to committed needs to be done
# within the same transaction as merge.
# Similarly, status changes to MERG

                try:
                    # Update status to ingesting
                    self.updateBatchStatus(mergeEngine, key, batchId, BatchStatus.INGESTING)

                    # Ingest data to staging
                    recordsInserted, recordsUpdated, totalRecords = self.ingestDatasetToStaging(
                        sourceEngine, mergeEngine, dataset, batchId, cmd
                    )

                    # Update status to merged
                    self.updateBatchStatus(
                        mergeEngine, key, batchId, BatchStatus.MERGED,
                        recordsInserted, recordsUpdated, 0, totalRecords
                    )

                    # Merge staging to merge table
                    finalInserted, finalUpdated, finalTotal = self.mergeStagingToMerge(
                        mergeEngine, dataset, batchId, cmd
                    )

                    # Update status to committed
                    self.updateBatchStatus(
                        mergeEngine, key, batchId, BatchStatus.COMMITTED,
                        finalInserted, finalUpdated, 0, finalTotal
                    )

                except Exception as e:
                    # Update status to failed
                    self.updateBatchStatus(mergeEngine, key, batchId, BatchStatus.FAILED)
                    raise e
        else:
            # For multi-dataset ingestion, process all datasets in a single batch
            batchId = self.startBatch(mergeEngine, self.store, None)
            key = self.store.name

            try:
                # Update status to ingesting
                self.updateBatchStatus(mergeEngine, key, batchId, BatchStatus.INGESTING)

                totalRecordsInserted = 0
                totalRecordsUpdated = 0
                totalRecords = 0

                # Process each dataset
                for dataset in self.store.datasets.values():
                    recordsInserted, recordsUpdated, datasetTotal = self.ingestDatasetToStaging(
                        sourceEngine, mergeEngine, dataset, batchId, cmd
                    )
                    totalRecordsInserted += recordsInserted
                    totalRecordsUpdated += recordsUpdated
                    totalRecords += datasetTotal

                # Update status to merged
                self.updateBatchStatus(
                    mergeEngine, key, batchId, BatchStatus.MERGED,
                    totalRecordsInserted, totalRecordsUpdated, 0, totalRecords
                )

                # Merge all datasets
                finalTotalInserted = 0
                finalTotalUpdated = 0
                for dataset in self.store.datasets.values():
                    finalInserted, finalUpdated, _ = self.mergeStagingToMerge(
                        mergeEngine, dataset, batchId, cmd
                    )
                    finalTotalInserted += finalInserted
                    finalTotalUpdated += finalUpdated

                # Update status to committed
                self.updateBatchStatus(
                    mergeEngine, key, batchId, BatchStatus.COMMITTED,
                    finalTotalInserted, finalTotalUpdated, 0, totalRecords
                )

            except Exception as e:
                # Update status to failed
                self.updateBatchStatus(mergeEngine, key, batchId, BatchStatus.FAILED)
                raise e
