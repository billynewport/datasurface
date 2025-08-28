"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import Any, List, Optional, Sequence, TypeVar, Dict
from datasurface.md.types import Boolean, SmallInt, Integer, BigInt, IEEE32, IEEE64, Decimal, Date, Timestamp, Interval, Variant, Char, NChar, \
    VarChar, NVarChar, Geography, GeometryType, SpatialReferenceSystem
import sqlalchemy
from sqlalchemy import MetaData, text, quoted_name
from sqlalchemy.inspection import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.schema import Table, Column, PrimaryKeyConstraint
from sqlalchemy.types import Boolean as SQLBoolean, SmallInteger, Integer as SQLInteger, BigInteger, Float, DECIMAL, Date as SQLDate, \
    TIMESTAMP, DATETIME, Interval as SQLInterval, LargeBinary, CHAR, VARCHAR, TEXT, Double
from datasurface.md import Dataset, Datastore
from datasurface.md import Workspace, DatasetGroup, DatasetSink, DataContainer, PostgresDatabase
from datasurface.md import EcosystemPipelineGraph, DataPlatform
from datasurface.md.schema import DDLColumn, NullableStatus, PrimaryKeyStatus, DDLTable
from datasurface.md.types import DataType
from abc import ABC, abstractmethod
from geoalchemy2 import Geography as GA2Geography, Geometry as GA2Geometry
from sqlalchemy.engine.url import URL
import logging
from sqlalchemy.engine.reflection import Inspector

# Standard Python logger - works everywhere
logger = logging.getLogger(__name__)


def ddlColumnToSQLAlchemyType(dataType: DDLColumn, engine: Optional[Any] = None) -> Column[Any]:
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
        t = Double()
    elif isinstance(dataType.type, Decimal):
        dec: Decimal = dataType.type
        t = DECIMAL(dec.maxSize, dec.precision)
    elif isinstance(dataType.type, Date):
        t = SQLDate()
    elif isinstance(dataType.type, Timestamp):
        # Use database-specific datetime type - this is critical for cross-database compatibility
        if engine is not None and hasattr(engine, 'dialect'):
            dialect_name = str(engine.dialect.name).lower()
            if 'mssql' in dialect_name:
                # SQL Server: use DATETIME instead of TIMESTAMP (which is binary row version)
                t = DATETIME()
            elif 'mysql' in dialect_name:
                # MySQL: use DATETIME for timestamp semantics
                t = DATETIME()
            elif 'oracle' in dialect_name:
                # Oracle: use TIMESTAMP
                t = TIMESTAMP()
            elif 'db2' in dialect_name:
                # DB2: use TIMESTAMP
                t = TIMESTAMP()
            else:
                # PostgreSQL and others: use TIMESTAMP
                t = TIMESTAMP()
        else:
            # Default to TIMESTAMP when engine is not available
            t = TIMESTAMP()
    elif isinstance(dataType.type, Interval):
        t = SQLInterval()
    elif isinstance(dataType.type, Variant):
        var: Variant = dataType.type
        # Use Snowflake VARIANT when targeting Snowflake
        if engine is not None and hasattr(engine, 'dialect') and 'snowflake' in str(engine.dialect.name).lower():
            try:
                from snowflake.sqlalchemy import VARIANT as SF_VARIANT  # type: ignore
                t = SF_VARIANT()
            except Exception:
                # Fallback if snowflake.sqlalchemy is not installed
                t = LargeBinary(var.maxSize)
        else:
            t = LargeBinary(var.maxSize)
    elif isinstance(dataType.type, Char):
        ch: Char = dataType.type
        t = CHAR(ch.maxSize, ch.collationString)
    elif isinstance(dataType.type, NChar):
        nch: NChar = dataType.type
        t = CHAR(nch.maxSize, nch.collationString)
    elif isinstance(dataType.type, VarChar):
        vc: VarChar = dataType.type
        t = VARCHAR(vc.maxSize, vc.collationString)
    elif isinstance(dataType.type, NVarChar):
        nvc: NVarChar = dataType.type
        t = VARCHAR(nvc.maxSize, nvc.collationString)
    elif isinstance(dataType.type, Geography):
        geo: Geography = dataType.type
        # Prefer native spatial types on Snowflake, otherwise use GeoAlchemy2 Geography
        if engine is not None and hasattr(engine, 'dialect') and 'snowflake' in str(engine.dialect.name).lower():
            try:
                from snowflake.sqlalchemy import GEOGRAPHY as SF_GEOGRAPHY  # type: ignore
                t = SF_GEOGRAPHY()
            except Exception:
                # Fallback to generic VARCHAR if native type not available
                t = VARCHAR(None)
        else:
            geometry_type = geo.geometryType.value if geo.geometryType else 'GEOMETRY'
            t = GA2Geography(geometry_type=geometry_type, srid=geo.srs.srid)
    else:
        raise Exception(f"Unknown data type {dataType.name}: {type(dataType.type)}")

    c: Column[Any] = Column(quoted_name(dataType.name, quote=True), t, nullable=(dataType.nullable == NullableStatus.NULLABLE))
    return c


def datasetToSQLAlchemyTable(dataset: Dataset, tableName: str, metadata: MetaData, engine: Optional[Any] = None) -> Table:
    """Converts a DDLTable to a SQLAlchemy Table"""
    if (isinstance(dataset.originalSchema, DDLTable)):
        table: DDLTable = dataset.originalSchema
        columns: List[Column[Any]] = []
        for col in table.columns.values():
            columns.append(ddlColumnToSQLAlchemyType(col, engine))
        if (table.primaryKeyColumns is not None):
            pk: PrimaryKeyConstraint = PrimaryKeyConstraint(*table.primaryKeyColumns.colNames)
            sqTable: Table = Table(tableName, metadata, *columns, pk)
            return sqTable
        else:
            sqTable: Table = Table(tableName, metadata, *columns)
            return sqTable
    else:
        raise Exception("Unknown schema type")


def datasetToSQLAlchemyView(dataset: Dataset, viewName: str, underlyingTable: str, metadata: MetaData, where_clause: Optional[str] = None) -> str:
    """Converts a Dataset to a SQL CREATE OR REPLACE VIEW statement that maps to an underlying table.

    The view will have a one-to-one mapping to columns in the underlying table that have the same names.
    Returns the SQL CREATE OR REPLACE VIEW statement as a string.

    Args:
        dataset: The dataset defining the schema
        viewName: Name of the view to create
        underlyingTable: Name of the underlying table
        metadata: SQLAlchemy metadata (unused but kept for compatibility)
        where_clause: Optional WHERE clause to filter the view (e.g., "batch_out = 2147483647")
    """
    if (isinstance(dataset.originalSchema, DDLTable)):
        table: DDLTable = dataset.originalSchema
        columnNames: List[str] = []
        for col in table.columns.values():
            columnNames.append(quoted_name(col.name, quote=True))

        # Create the view SQL statement using CREATE OR REPLACE VIEW
        columnsClause: str = ", ".join(columnNames)
        viewSql: str = f"CREATE OR REPLACE VIEW {viewName} AS SELECT {columnsClause} FROM {underlyingTable}"

        # Add WHERE clause if provided
        if where_clause:
            viewSql += f" WHERE {where_clause}"

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
        elif isinstance(colType, DATETIME):
            # DATETIME is also a timestamp in our model
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
        elif isinstance(colType, GA2Geography):
            # Convert GeoAlchemy2 Geography back to DataSurface Geography
            geometry_type = None
            if hasattr(colType, 'geometry_type') and colType.geometry_type != 'GEOMETRY':
                geometry_type = GeometryType(colType.geometry_type)
            srs = SpatialReferenceSystem(colType.srid if hasattr(colType, 'srid') and colType.srid != -1 else 4326)
            newType = Geography(srs=srs, geometryType=geometry_type)
        elif isinstance(colType, GA2Geometry):
            # Treat Geometry as Geography for now (could be extended later)
            geometry_type = None
            if hasattr(colType, 'geometry_type') and colType.geometry_type != 'GEOMETRY':
                geometry_type = GeometryType(colType.geometry_type)
            srs = SpatialReferenceSystem(colType.srid if hasattr(colType, 'srid') and colType.srid != -1 else 4326)
            newType = Geography(srs=srs, geometryType=geometry_type)

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
            url = URL.create(
                "postgresql+psycopg2",
                username=userName,
                password=password,
                host=container.hostPortPair.hostName,
                port=container.hostPortPair.port,
                database=container.databaseName,
            )
            return sqlalchemy.create_engine(url)
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

    # TIMESTAMP/DATETIME equivalencies - these represent the same semantic meaning
    # across different databases but use different SQL types
    timestamp_types = ['TIMESTAMP', 'DATETIME', 'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE']
    if current in timestamp_types and desired in timestamp_types:
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


def createOrUpdateTable(connection: Connection, inspector: Inspector, table: Table, createOnly: bool = False) -> bool:
    """Create the table if it doesn't exist; otherwise add/alter columns per dialect.

    Returns True if created or altered, False if no changes were needed.
    """
    import time
    start_time = time.time()

    logger.info(f"⏱️  Starting createOrUpdateTable for {table.name}")

    # Time the has_table check
    has_table_start = time.time()
    table_exists = inspector.has_table(table.name)  # type: ignore[attr-defined]
    has_table_time = time.time() - has_table_start
    logger.info(f"⏱️  has_table() check took {has_table_time:.3f}s for {table.name} (exists: {table_exists})")

    if not table_exists:
        create_start = time.time()
        table.create(connection)
        create_time = time.time() - create_start
        total_time = time.time() - start_time
        logger.info(f"⏱️  table.create() took {create_time:.3f}s for {table.name}")
        logger.info(f"⏱️  TOTAL createOrUpdateTable took {total_time:.3f}s for {table.name}")
        return True

    if createOnly:
        logger.info(f"⏱️  createOnly is True, skipping schema fingerprint check for {table.name}")
        return True

    # PERFORMANCE OPTIMIZATION: Use fast schema fingerprint for SQL Server and Snowflake before expensive schema autoload
    dialect_name = str(connection.dialect.name).lower()
    is_mssql = 'mssql' in dialect_name or 'sqlserver' in dialect_name
    is_snowflake = 'snowflake' in dialect_name

    if is_mssql:
        # Create comprehensive but fast schema fingerprint
        fingerprint_start = time.time()
        result = connection.execute(text(f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table.name}'
                ORDER BY ORDINAL_POSITION
            """))
        existing_schema = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in result]

        # Generate expected schema fingerprint from table definition
        expected_schema = []
        for column in table.columns:
            col_name = column.name
            col_type = str(column.type).lower()

            # Extract type details for comparison
            if hasattr(column.type, 'length') and getattr(column.type, 'length', None):
                max_length = getattr(column.type, 'length', None)
            else:
                max_length = None

            nullable = column.nullable
            expected_schema.append((col_name, col_type, max_length, None, None, nullable))

        fingerprint_time = time.time() - fingerprint_start
        logger.info(f"⏱️  Schema fingerprint check took {fingerprint_time:.3f}s for {table.name}")

        # Compare schemas (simplified comparison for key differences)
        schemas_match = (len(existing_schema) == len(expected_schema))
        if schemas_match:
            for i, (existing_col, expected_col) in enumerate(zip(existing_schema, expected_schema)):
                # Compare: name, type, length (key properties that affect compatibility)
                if (existing_col[0].lower() != expected_col[0].lower() or
                        existing_col[2] != expected_col[2]):  # different name or length
                    schemas_match = False
                    break

        if schemas_match:
            total_time = time.time() - start_time
            logger.info(f"⏱️  TOTAL createOrUpdateTable took {total_time:.3f}s for {table.name} (schema fingerprint matches, skipped full schema check)")
            return False  # No changes needed
        else:
            logger.info("⏱️  Schema fingerprint mismatch detected, proceeding with full schema validation")
            logger.info(f"⏱️  Existing: {existing_schema}")
            logger.info(f"⏱️  Expected: {expected_schema}")

    elif is_snowflake:
        # Create comprehensive but fast schema fingerprint for Snowflake
        fingerprint_start = time.time()
        # Snowflake stores table names in uppercase by default
        result = connection.execute(text(f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = UPPER('{table.name}')
                ORDER BY ORDINAL_POSITION
            """))
        existing_schema = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in result]

        # Generate expected schema fingerprint from table definition
        expected_schema = []
        for column in table.columns:
            col_name = column.name
            col_type = str(column.type).lower()

            # Extract type details for comparison
            if hasattr(column.type, 'length') and getattr(column.type, 'length', None):
                max_length = getattr(column.type, 'length', None)
            else:
                max_length = None

            nullable = column.nullable
            expected_schema.append((col_name, col_type, max_length, None, None, nullable))

        fingerprint_time = time.time() - fingerprint_start
        logger.info(f"⏱️  Schema fingerprint check took {fingerprint_time:.3f}s for {table.name}")

        # Compare schemas (simplified comparison for key differences)
        schemas_match = (len(existing_schema) == len(expected_schema))
        if schemas_match:
            for i, (existing_col, expected_col) in enumerate(zip(existing_schema, expected_schema)):
                # Compare: name, type, length (key properties that affect compatibility)
                if (existing_col[0].lower() != expected_col[0].lower() or
                        existing_col[2] != expected_col[2]):  # different name or length
                    schemas_match = False
                    break

        if schemas_match:
            total_time = time.time() - start_time
            logger.info(f"⏱️  TOTAL createOrUpdateTable took {total_time:.3f}s for {table.name} (schema fingerprint matches, skipped full schema check)")
            return False  # No changes needed
        else:
            logger.info("⏱️  Schema fingerprint mismatch detected, proceeding with full schema validation")
            logger.info(f"⏱️  Existing: {existing_schema}")
            logger.info(f"⏱️  Expected: {expected_schema}")

    # Time the schema loading
    schema_start = time.time()
    currentSchema: Table = Table(table.name, MetaData(), autoload_with=connection)
    schema_time = time.time() - schema_start
    logger.info(f"⏱️  Schema autoload took {schema_time:.3f}s for {table.name}")

    # Determine dialect
    dialect_name = str(connection.dialect.name).lower()
    is_postgres = 'postgres' in dialect_name
    is_mssql = 'mssql' in dialect_name or 'sqlserver' in dialect_name

    # Identify new and altered columns
    newColumns: List[Column[Any]] = []
    for column in table.columns:
        col_typed: Column[Any] = column
        if col_typed.name not in currentSchema.columns:  # type: ignore[attr-defined]
            newColumns.append(col_typed)

    columnsToAlter: List[Column[Any]] = []
    for column in currentSchema.columns:
        col_typed: Column[Any] = column
        if col_typed.name not in table.columns:  # type: ignore[attr-defined]
            continue
        current_type_str = str(col_typed.type).upper()  # type: ignore[attr-defined]
        desired_type_str = str(table.columns[col_typed.name].type).upper()  # type: ignore[attr-defined]
        if current_type_str != desired_type_str and not _types_are_compatible(current_type_str, desired_type_str):
            columnsToAlter.append(table.columns[col_typed.name])  # type: ignore[attr-defined]

    if not newColumns and not columnsToAlter:
        return False

    # Add new columns
    for column in newColumns:
        column_type = str(column.type)  # type: ignore[attr-defined]
        if is_postgres:
            add_sql = f"ALTER TABLE {table.name} ADD COLUMN {column.name} {column_type}"
        elif is_mssql:
            add_sql = f"ALTER TABLE {table.name} ADD {column.name} {column_type}"
        else:
            # Fallback to ANSI-ish syntax without COLUMN keyword
            add_sql = f"ALTER TABLE {table.name} ADD {column.name} {column_type}"
        if column.nullable is False:
            add_sql += " NOT NULL"
        connection.execute(text(add_sql))

    # Alter existing columns' types when necessary
    if columnsToAlter:
        if is_postgres:
            alter_parts = [f"ALTER COLUMN {c.name} TYPE {str(c.type)}" for c in columnsToAlter]  # type: ignore[attr-defined]
            alter_sql = f"ALTER TABLE {table.name} " + ", ".join(alter_parts)
            connection.execute(text(alter_sql))
        elif is_mssql:
            # SQL Server requires separate ALTERs and uses no TYPE keyword
            for c in columnsToAlter:
                alter_sql = f"ALTER TABLE {table.name} ALTER COLUMN {c.name} {str(c.type)}"  # type: ignore[attr-defined]
                connection.execute(text(alter_sql))
        else:
            # Best-effort generic path: attempt separate ALTERs
            for c in columnsToAlter:
                alter_sql = f"ALTER TABLE {table.name} ALTER COLUMN {c.name} {str(c.type)}"  # type: ignore[attr-defined]
                connection.execute(text(alter_sql))

    if newColumns:
        logger.info("Added columns to table %s: %s", table.name, [col.name for col in newColumns])  # type: ignore[attr-defined]
    if columnsToAlter:
        logger.info("Altered columns in table %s: %s", table.name, [col.name for col in columnsToAlter])  # type: ignore[attr-defined]

    total_time = time.time() - start_time
    logger.info(f"⏱️  TOTAL createOrUpdateTable took {total_time:.3f}s for {table.name}")
    return True


def createOrUpdateView(connection: Connection, inspector: Inspector, dataset: Dataset,
                       viewName: str, underlyingTable: str, where_clause: Optional[str] = None) -> bool:
    """Create or update a view to match the dataset schema across databases.

    For PostgreSQL: uses CREATE OR REPLACE VIEW and pg_get_viewdef for comparison.
    For SQL Server: uses CREATE OR ALTER VIEW and sys.sql_modules for comparison.
    """
    dialect_name = str(connection.dialect.name).lower()
    is_postgres = 'postgres' in dialect_name
    is_mssql = 'mssql' in dialect_name or 'sqlserver' in dialect_name

    # Build SELECT list from dataset schema
    if isinstance(dataset.originalSchema, DDLTable):
        table: DDLTable = dataset.originalSchema
        columnNames: List[str] = [col.name for col in table.columns.values()]
        select_clause = f"SELECT {', '.join(columnNames)} FROM {underlyingTable}"
        if where_clause:
            select_clause += f" WHERE {where_clause}"
    else:
        raise Exception("Unknown schema type")

    # Generate dialect-specific CREATE statement
    if is_postgres:
        newViewSql: str = f"CREATE OR REPLACE VIEW {viewName} AS {select_clause}"
    elif is_mssql:
        newViewSql = f"CREATE OR ALTER VIEW {viewName} AS {select_clause}"
    else:
        # Fallback to ANSI-ish CREATE OR REPLACE (supported by several engines)
        newViewSql = f"CREATE OR REPLACE VIEW {viewName} AS {select_clause}"

    # Check if view exists
    exists: bool = viewName in inspector.get_view_names()
    if exists:  # type: ignore[attr-defined]
        # View exists; compare current definition to decide if update is needed
        try:
            if is_postgres:
                result = connection.execute(text(f"SELECT pg_get_viewdef('{viewName}', true) as view_def"))
                currentViewDef = result.fetchone()[0]
            elif is_mssql:
                # Prefer sys.sql_modules for full definition
                result = connection.execute(
                    text(
                        "SELECT sm.definition FROM sys.sql_modules sm "
                        "JOIN sys.views v ON sm.object_id = v.object_id "
                        "WHERE v.name = :vname"
                    ),
                    {"vname": viewName},
                )
                row = result.fetchone()
                currentViewDef = row[0] if row else None
            else:
                currentViewDef = None

            if currentViewDef is None:
                # If we cannot retrieve definition, update defensively
                connection.execute(text(newViewSql))
                logger.info("Updated view %s to match current schema", viewName)
                return True

            # Normalize SELECT parts and compare
            normalizedCurrent = ' '.join(str(currentViewDef).replace('\n', ' ').replace(';', '').split()).upper().strip()
            selectStart = newViewSql.upper().find('SELECT')
            if selectStart != -1:
                normalizedNewSelect = ' '.join(newViewSql[selectStart:].replace('\n', ' ').split()).upper().strip()
            else:
                normalizedNewSelect = ' '.join(newViewSql.replace('\n', ' ').split()).upper().strip()

            if normalizedCurrent == normalizedNewSelect:
                logger.info("View %s already matches current schema", viewName)
                return False

            connection.execute(text(newViewSql))
            logger.info("Updated view %s to match current schema", viewName)
            return True
        except Exception as e:
            logger.warning("Could not compare view definitions for %s, updating anyway: %s", viewName, e)
            connection.execute(text(newViewSql))
            logger.info("Updated view %s to match current schema", viewName)
            return True

    # View doesn't exist; create it
    connection.execute(text(newViewSql))
    logger.info("Created view %s with current schema", viewName)
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
    with engine.begin() as connection:
        inspector = inspect(connection)  # type: ignore[attr-defined]
        for dataset in store.datasets.values():
            # Map the dataset to view name and underlying table name
            viewName: str = viewNameMapper(dataset)
            underlyingTable: str = underlyingTableMapper(dataset)

            # Create or update the view
            wasChanged: bool = createOrUpdateView(connection, inspector, dataset, viewName, underlyingTable)
            changedViews[viewName] = wasChanged

    return changedViews
