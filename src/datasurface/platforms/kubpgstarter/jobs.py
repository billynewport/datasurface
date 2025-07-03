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

    # For VARCHAR and TEXT, we need to be more careful about lengths
    # Only consider them compatible if they have the same constraints
    # This means VARCHAR(50) and VARCHAR(200) are NOT compatible

    # If we can't determine compatibility, assume they're different
    return False


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
                # Add new columns
                for column in newColumns:
                    column_type = str(column.type)  # type: ignore[attr-defined]
                    alter_sql = f"ALTER TABLE {table.name} ADD COLUMN {column.name} {column_type}"  # type: ignore[attr-defined]
                    if column.nullable is False:
                        alter_sql += " NOT NULL"
                    connection.execute(text(alter_sql))

                # Alter existing columns
                for column in columnsToAlter:
                    alter_sql = f"ALTER TABLE {table.name} ALTER COLUMN {column.name} TYPE {str(column.type)}"  # type: ignore[attr-defined]
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

    def reconcileStagingTableSchemas(self, mergeEngine: Engine, store: Datastore, cmd: SQLSnapshotIngestion) -> None:
        """This will make sure the staging table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = dataset.name if dataset.name not in cmd.tableForDataset else cmd.tableForDataset[dataset.name]
            tableName = tableName + "_staging"
            stagingTable: Table = self.getStagingSchemaForDataset(dataset, tableName)
            createOrUpdateTable(mergeEngine, stagingTable)

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

        # Make sure the staging table exists and has the current schema for each dataset
        self.reconcileStagingTableSchemas(mergeEngine, self.store, cmd)

        # TODO: Implement the actual snapshot merge logic using sourceEngine and mergeEngine
        # For now, we'll keep the sourceEngine reference to avoid the unused variable warning
        _ = sourceEngine
