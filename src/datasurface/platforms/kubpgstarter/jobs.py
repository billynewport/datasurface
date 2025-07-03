# type: ignore[attr-defined, unknown-member, unknown-argument, unknown-variable, unknown-parameter]
"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Datastore, Ecosystem, CredentialStore, SQLSnapshotIngestion, DataContainer, PostgresDatabase, Dataset
from sqlalchemy import create_engine, Table, MetaData, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, String
from typing import cast, List, Any
from datasurface.platforms.kubpgstarter.kubpgstarter import KubernetesPGStarterDataPlatform
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable


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

    store: Datastore

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

    def reconcileStagingTableSchemas(self, mergeEngine: Engine, store: Datastore, cmd: SQLSnapshotIngestion) -> None:
        """This will make sure the staging table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = dataset.name if dataset.name not in cmd.tableForDataset else cmd.tableForDataset[dataset.name]
            tableName = tableName + "_staging"
            stagingTable: Table = self.getStagingSchemaForDataset(dataset, tableName)
            createOrUpdateTable(mergeEngine, stagingTable)

    def reconcileMergeTableSchemas(self, mergeEngine: Engine, store: Datastore, cmd: SQLSnapshotIngestion) -> None:
        """This will make sure the merge table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = dataset.name if dataset.name not in cmd.tableForDataset else cmd.tableForDataset[dataset.name]
            tableName = tableName + "_merge"
            mergeTable: Table = self.getMergeSchemaForDataset(dataset, tableName)
            createOrUpdateTable(mergeEngine, mergeTable)

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

        # TODO: Implement the actual snapshot merge logic using sourceEngine and mergeEngine
        # For now, we'll keep the sourceEngine reference to avoid the unused variable warning
        _ = sourceEngine
