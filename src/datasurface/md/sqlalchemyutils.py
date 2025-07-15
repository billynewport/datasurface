"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import Any, List, Optional, Sequence, TypeVar, Dict
from datasurface.md.types import Boolean, SmallInt, Integer, BigInt, IEEE32, IEEE64, Decimal, Date, Timestamp, Interval, Variant, Char, NChar, \
    VarChar, NVarChar
import sqlalchemy
from sqlalchemy import inspect, MetaData, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.schema import Table, Column, PrimaryKeyConstraint
from sqlalchemy.types import Boolean as SQLBoolean, SmallInteger, Integer as SQLInteger, BigInteger, Float, DECIMAL, Date as SQLDate, \
    TIMESTAMP, Interval as SQLInterval, LargeBinary, CHAR, VARCHAR, TEXT
from datasurface.md import Dataset, Datastore
from datasurface.md import Workspace, DatasetGroup, DatasetSink, DataContainer, PostgresDatabase
from datasurface.md import EcosystemPipelineGraph, DataPlatform
from datasurface.md.schema import DDLColumn, NullableStatus, PrimaryKeyStatus, DDLTable
from datasurface.md.types import DataType
from abc import ABC, abstractmethod


def ddlColumnToSQLAlchemyType(dataType: DDLColumn) -> Column[Any]:
    """Converts a DataType to a SQLAlchemy type"""
    # TODO: Timestamp support of timezones

    t: Any = None

    if isinstance(dataType.type, Boolean):
        t = SQLBoolean()
    elif isinstance(dataType.type, SmallInt):
        t = SmallInteger()
    elif isinstance(dataType.type, Integer):
        t = SQLInteger()
    elif isinstance(dataType.type, BigInt):
        t = BigInteger()
    elif isinstance(dataType.type, IEEE32):
        t = Float()
    elif isinstance(dataType.type, IEEE64):
        t = Float()  # SQLAlchemy 1.4 doesn't have Double, use Float for IEEE64
    elif isinstance(dataType.type, Decimal):
        dec: Decimal = dataType.type
        t = DECIMAL(dec.maxSize, dec.precision)
    elif isinstance(dataType.type, Date):
        t = SQLDate()
    elif isinstance(dataType.type, Timestamp):
        t = TIMESTAMP()
    elif isinstance(dataType.type, Interval):
        t = SQLInterval()
    elif isinstance(dataType.type, Variant):
        var: Variant = dataType.type
        t = LargeBinary(var.maxSize)  # SQLAlchemy 1.4 uses LargeBinary instead of VARBINARY
    elif isinstance(dataType.type, Char):
        ch: Char = dataType.type
        t = CHAR(ch.maxSize, ch.collationString)
    elif isinstance(dataType.type, NChar):
        nch: NChar = dataType.type
        # SQLAlchemy 1.4 doesn't have NCHAR, use CHAR with Unicode support
        t = CHAR(nch.maxSize, collation=nch.collationString)
    elif isinstance(dataType.type, VarChar):
        vc: VarChar = dataType.type
        t = VARCHAR(vc.maxSize, vc.collationString)
    elif isinstance(dataType.type, NVarChar):
        nvc: NVarChar = dataType.type
        # SQLAlchemy 1.4 doesn't have NVARCHAR, use VARCHAR with Unicode support
        t = VARCHAR(nvc.maxSize, collation=nvc.collationString)
    else:
        raise Exception(f"Unknown data type {dataType.name}: {type(dataType.type)}")

    c: Column[Any] = Column(dataType.name, t, nullable=(dataType.nullable == NullableStatus.NULLABLE))
    return c


def datasetToSQLAlchemyTable(dataset: Dataset, tableName: str, metadata: MetaData) -> Table:
    """Converts a DDLTable to a SQLAlchemy Table"""
    if (isinstance(dataset.originalSchema, DDLTable)):
        table: DDLTable = dataset.originalSchema
        columns: List[Column[Any]] = []
        for col in table.columns.values():
            columns.append(ddlColumnToSQLAlchemyType(col))
        if (table.primaryKeyColumns is not None):
            pk: PrimaryKeyConstraint = PrimaryKeyConstraint(*table.primaryKeyColumns.colNames)
            sqTable: Table = Table(tableName, metadata, *columns, pk)
            return sqTable
        else:
            sqTable: Table = Table(tableName, metadata, *columns)
            return sqTable
    else:
        raise Exception("Unknown schema type")


def datasetToSQLAlchemyView(dataset: Dataset, viewName: str, underlyingTable: str, metadata: MetaData) -> str:
    """Converts a Dataset to a SQL CREATE OR REPLACE VIEW statement that maps to an underlying table.

    The view will have a one-to-one mapping to columns in the underlying table that have the same names.
    Returns the SQL CREATE OR REPLACE VIEW statement as a string.
    """
    if (isinstance(dataset.originalSchema, DDLTable)):
        table: DDLTable = dataset.originalSchema
        columnNames: List[str] = []
        for col in table.columns.values():
            columnNames.append(col.name)

        # Create the view SQL statement using CREATE OR REPLACE VIEW
        columnsClause: str = ", ".join(columnNames)
        viewSql: str = f"CREATE OR REPLACE VIEW {viewName} AS SELECT {columnsClause} FROM {underlyingTable}"
        return viewSql
    else:
        raise Exception("Unknown schema type")


_T = TypeVar('_T')


def getValueOrThrow(val: Optional[_T]) -> _T:
    """Converts an optional literal to an literal or throws an exception if the value is None"""
    if (val is None):
        raise Exception("Value is None")
    return val


def convertSQLAlchemyTableToDataset(table: Table) -> Dataset:
    """Converts a SQLAlchemy Table to a Dataset"""
    columns: List[DDLColumn] = []

    for al_col in table.columns.values():  # type: ignore[attr-defined]
        colType: Any = al_col.type  # type: ignore[attr-defined]
        newType: Optional[DataType] = None
        if isinstance(colType, SQLBoolean):
            newType = Boolean()
        elif isinstance(colType, SmallInteger):
            newType = SmallInt()
        elif isinstance(colType, SQLInteger):
            newType = Integer()
        elif isinstance(colType, BigInteger):
            newType = BigInt()
        elif isinstance(colType, Float):
            newType = IEEE32()
        elif isinstance(colType, DECIMAL):
            s_dec: DECIMAL = colType
            newType = Decimal(getValueOrThrow(s_dec.precision), getValueOrThrow(s_dec.scale))
        elif isinstance(colType, SQLDate):
            newType = Date()
        elif isinstance(colType, TIMESTAMP):
            newType = Timestamp()
        elif isinstance(colType, SQLInterval):
            newType = Interval()
        elif isinstance(colType, LargeBinary):
            var_b: LargeBinary = colType
            newType = Variant(var_b.length)
        elif isinstance(colType, CHAR):
            ch_col: CHAR = colType
            newType = Char(getValueOrThrow(ch_col.length), ch_col.collation)
        elif isinstance(colType, VARCHAR):
            newType = VarChar(getValueOrThrow(colType.length), colType.collation)
        elif isinstance(colType, TEXT):
            newType = VarChar(None, colType.collation)
        if (newType):
            n: NullableStatus = NullableStatus.NOT_NULLABLE
            if (getValueOrThrow(al_col.nullable)):  # type: ignore[attr-defined]
                n = NullableStatus.NULLABLE
            pk: PrimaryKeyStatus = PrimaryKeyStatus.NOT_PK
            if (getValueOrThrow(al_col.primary_key)):  # type: ignore[attr-defined]
                pk = PrimaryKeyStatus.PK

            columns.append(DDLColumn(al_col.name, newType, n, pk))  # type: ignore[attr-defined]
        else:
            raise Exception(f"Unknown data type {al_col.name}: {colType}")  # type: ignore[attr-defined]

    t: DDLTable = DDLTable(*columns)

    primaryKeyColumns: List[DDLColumn] = []
    for constraint in table.constraints:
        if (isinstance(constraint, PrimaryKeyConstraint)):
            for pkCol in constraint.columns:
                col: Optional[DDLColumn] = t.getColumnByName(pkCol.name)
                if (col):
                    primaryKeyColumns.append(col)
                    col.primaryKey = PrimaryKeyStatus.PK
                else:
                    raise Exception(f"Unknown pk column {pkCol.name}")
    if (len(primaryKeyColumns) == 0):
        for column in columns:
            if (column.primaryKey == PrimaryKeyStatus.PK):
                primaryKeyColumns.append(column)

    rc: Dataset = Dataset(table.name, t)
    return rc


def convertSQLAlchemyTableSetToDatastore(name: str, tables: Sequence[Table]) -> Datastore:
    """Converts a list of SQLAlchemy Tables to a Datastore"""
    datasets: List[Dataset] = []
    for table in tables:
        datasets.append(convertSQLAlchemyTableToDataset(table))
    return Datastore(name, *datasets)


def convertDDLTableToPythonClass(name: str, table: DDLTable) -> str:
    """Converts a DDLTable to a Python class"""
    classStr: str = f"class {name}:\n"
    for col in table.columns.values():
        classStr += f"    {col.name}: \n"
    return classStr


"""
We need 2 major features here. The first is the vanilla ability to create/maintain tables and views against the model schema. The schema
needs to be modified also for extra columns required. MD5 hash columns for primary keys and all columns, milestoning columns if needed and
so on. Names of attributes/datasets/datastores may need to be modified to be compatible with the naming rules of the underlying
database, length limits, character restrictions, reserved words."""


class DatasetMapping:
    """This class is used to map a dataset to a DataContainer"""
    def __init__(self, dataset: Dataset) -> None:
        self.dataset: Dataset = dataset
        self.tableNames: List[str] = []
        self.viewNames: List[str] = []
        self.attributes: Dict[str, str] = {}


class DataPlatformDatasetSinkArtifactIterator(ABC):
    """This class is used to iterate over the artifacts for a DataPlatform/DatasetSink pair. The expected pattern is a single
    table to hold the data. Each consumer will get a specific named view pointing at the table. The view name will
    include the ${workspaceName}_{$dsgName}_${storeName}_${datasetName}_${postfix}. This will be a long name
    so mangling may be required. The name also needs to be unique across the DataContainer (i.e. the database or filesystem)
    Certain characters may not be allowed, there may be length restrictions. It's very DataContainer specific"""

    def __init__(self, dp: DataPlatform, dc: DataContainer, w: Workspace, dsg: DatasetGroup) -> None:
        self.dp: DataPlatform = dp
        self.dc: DataContainer = dc
        self.w: Workspace = w
        self.dsg: DatasetGroup = dsg

    @abstractmethod
    def getArtifacts(self, store: Datastore, datasets: List[Dataset]) -> tuple[List[str], List[str]]:
        """This really needs to return a list of tables and view names. It also needs to return a list of attribute names
        and type strings for each dataset."""
        pass


class SQLAlchemyDataContainerReconciler:
    """This class is used to reconcile a Workspace against the database that it has been assigned to. The Workspace has an
    associated DataContainer which should be a container type which SQLAlchemy supports. The container must
    be a physical container, not a logical one. Reconciling means creating and maintaining schemas in the DataContainer
    to match the model schemas.
    A Datacontainer will have multiple Workspaces assigned to it. Each Workspace has one or more Datasetgroups and each Datasetgroup is assigned to a
    single DataPlatform selected by the data platform chooser. We need to create raw tables in which to contain data supplied by DataPlatforms. We need views
    defined referencing these tables which are used by users of the Workspace. So, we need to gather all Workspaces assigned to a DataContainer.
    We need to get the list of DataPlatform instances to support those workspaces. We need the list of raw tables (one per dataset) used by each DataPlatform.
    We create those tables using a naming convention combining dataplatform instance and datastore/dataset names. We need to create a
    single view for each Datasetgroup
    """

    def createEngine(self, container: DataContainer, userName: str, password: str) -> Engine:
        if isinstance(container, PostgresDatabase):
            return sqlalchemy.create_engine(  # type: ignore[attr-defined]
                'postgresql://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                    username=userName,
                    password=password,
                    hostName=container.hostPortPair.hostName,
                    port=container.hostPortPair.port,
                    databaseName=container.databaseName
                    )
            )
        else:
            raise Exception(f"Unsupported container type {type(container)}")

    def __init__(self, graph: EcosystemPipelineGraph, container: DataContainer, userName: str, password: str) -> None:
        """This really needs an intention graph to work out what we are doing"""
        self.engine: Engine = self.createEngine(container, userName, password)
        self.graph: EcosystemPipelineGraph = graph

    def reconcileDatasetSink(
            self,
            conn: Connection,
            dp: DataPlatform,
            dc: DataContainer,
            workspace: Workspace, dsg: DatasetGroup, store: Datastore, sink: DatasetSink) -> None:
        """This will create or alter to make current all SQL objects used by a DataPlatform
        for this DatasetSink"""
        pass

    def reconcileBeforeExport(
            self,
            artifacts: DataPlatformDatasetSinkArtifactIterator,
            dp: DataPlatform,
            dc: DataContainer,
            workspace: Workspace,
            dsg: DatasetGroup,
            store: Datastore, datasets: List[Dataset]) -> None:
        """DataPlatforms can use this to make sure all tables/views for this dsg are up to date before applying
         deltas to the tables. """

        with self.engine.connect() as conn:
            # For each DatasetSink in the DSG, reconcile it
            for sink in dsg.sinks.values():
                # Each DataPlatform will have a set of Tables/Views for a DatasetSink
                # These are named using the naming convention of the DataPlatform
                self.reconcileDatasetSink(conn, dp, dc, workspace, dsg, store, sink)

    def reconcileAllKnownDataContainers(self) -> None:
        """This will iterate over all sqlalchemy supported data containers and reconcile the
        table and view schemas for them."""
        pass


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


def createOrUpdateTable(engine: Engine, table: Table) -> bool:
    """This will create the table if it doesn't exist or update it if it does. Returns True if created or altered, False if no changes were needed."""
    inspector = inspect(engine)  # type: ignore[attr-defined]
    if not inspector.has_table(table.name):  # type: ignore[attr-defined]
        table.create(engine)
        print(f"Created table {table.name}")
        return True
    else:
        currentSchema: Table = Table(table.name, MetaData(), autoload_with=engine)
        newColumns: List[Column[Any]] = []
        for column in table.columns:
            col_typed: Column[Any] = column  # Type annotation for clarity
            if col_typed.name not in currentSchema.columns:  # type: ignore[attr-defined]
                newColumns.append(col_typed)
        columnsToAlter: List[Column[Any]] = []
        for column in currentSchema.columns:
            col_typed: Column[Any] = column  # Type annotation for clarity
            if col_typed.name not in table.columns:  # type: ignore[attr-defined]
                continue
            current_type_str = str(col_typed.type).upper()  # type: ignore[attr-defined]
            desired_type_str = str(table.columns[col_typed.name].type).upper()  # type: ignore[attr-defined]
            if current_type_str != desired_type_str and not _types_are_compatible(current_type_str, desired_type_str):
                columnsToAlter.append(table.columns[col_typed.name])  # type: ignore[attr-defined]
        if newColumns or columnsToAlter:
            with engine.begin() as connection:
                for column in newColumns:
                    column_type = str(column.type)  # type: ignore[attr-defined]
                    alter_sql = f"ALTER TABLE {table.name} ADD COLUMN {column.name} {column_type}"  # type: ignore[attr-defined]
                    if column.nullable is False:
                        alter_sql += " NOT NULL"
                    connection.execute(text(alter_sql))
                if columnsToAlter:
                    alter_parts = []
                    for column in columnsToAlter:
                        alter_parts.append(f"ALTER COLUMN {column.name} TYPE {str(column.type)}")  # type: ignore[attr-defined]
                    alter_sql = f"ALTER TABLE {table.name} " + ", ".join(alter_parts)
                    connection.execute(text(alter_sql))
            if newColumns:
                print(f"Added columns to table {table.name}: {[col.name for col in newColumns]}")  # type: ignore[attr-defined]
            if columnsToAlter:
                print(f"Altered columns in table {table.name}: {[col.name for col in columnsToAlter]}")  # type: ignore[attr-defined]
            return True
        else:
            return False


def createOrUpdateView(engine: Engine, dataset: Dataset, viewName: str, underlyingTable: str) -> bool:
    """This will create the view if it doesn't exist or update it if it does to match the current dataset schema

    Returns:
        bool: True if the view was created or modified, False if no changes were needed
    """
    inspector = inspect(engine)  # type: ignore[attr-defined]

    # Generate the new view SQL using CREATE OR REPLACE VIEW
    newViewSql: str = datasetToSQLAlchemyView(dataset, viewName, underlyingTable, MetaData())

    # Check if view exists
    if inspector.has_table(viewName):  # type: ignore[attr-defined]
        # View exists, check if it needs to be updated
        try:
            # Get the current view definition
            with engine.connect() as connection:
                result = connection.execute(text(f"SELECT pg_get_viewdef('{viewName}', true) as view_def"))
                currentViewDef = result.fetchone()[0]

                # Normalize the view definitions for comparison
                # Remove whitespace, newlines, and convert to uppercase for comparison
                normalizedCurrent = ' '.join(currentViewDef.replace('\n', ' ').replace(';', '').split()).upper().strip()

                # Extract just the SELECT part from the new view SQL (after CREATE OR REPLACE VIEW ... AS)
                selectStart = newViewSql.upper().find('SELECT')
                if selectStart != -1:
                    normalizedNewSelect = ' '.join(newViewSql[selectStart:].replace('\n', ' ').split()).upper().strip()
                else:
                    normalizedNewSelect = ' '.join(newViewSql.replace('\n', ' ').split()).upper().strip()

                # Debug output for troubleshooting
                print(f"DEBUG: Current view def: {repr(normalizedCurrent)}")
                print(f"DEBUG: New view def: {repr(normalizedNewSelect)}")

                # Check if the SELECT part matches
                if normalizedCurrent == normalizedNewSelect:
                    # No changes needed
                    print(f"View {viewName} already matches current schema")
                    return False
                else:
                    # View needs to be updated
                    with engine.begin() as connection:
                        connection.execute(text(newViewSql))
                    print(f"Updated view {viewName} to match current schema")
                    return True

        except Exception as e:
            # If we can't get the current view definition, assume it needs updating
            print(f"Could not compare view definitions for {viewName}, updating anyway: {e}")
            with engine.begin() as connection:
                connection.execute(text(newViewSql))
            print(f"Updated view {viewName} to match current schema")
            return True
    else:
        # View doesn't exist, create it
        with engine.begin() as connection:
            connection.execute(text(newViewSql))
        print(f"Created view {viewName} with current schema")
        return True


def reconcileViewSchemas(engine: Engine, store: Datastore, viewNameMapper, underlyingTableMapper) -> dict[str, bool]:
    """This will make sure the views exist and have the current schema for each dataset

    Args:
        engine: SQLAlchemy engine for the database
        store: Datastore containing the datasets
        viewNameMapper: Function that takes a dataset and returns the view name
        underlyingTableMapper: Function that takes a dataset and returns the underlying table name

    Returns:
        dict[str, bool]: Dictionary mapping view names to whether they were changed (True) or not (False)
    """
    changedViews: dict[str, bool] = {}

    for dataset in store.datasets.values():
        # Map the dataset to view name and underlying table name
        viewName: str = viewNameMapper(dataset)
        underlyingTable: str = underlyingTableMapper(dataset)

        # Create or update the view
        wasChanged: bool = createOrUpdateView(engine, dataset, viewName, underlyingTable)
        changedViews[viewName] = wasChanged

    return changedViews
