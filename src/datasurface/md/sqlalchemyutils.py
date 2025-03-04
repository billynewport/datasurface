"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import decimal
from typing import Any, List, Optional, Sequence, TypeVar
from datasurface.md import Boolean, SmallInt, Integer, BigInt, IEEE32, IEEE64, Decimal, Date, Timestamp, Interval, Variant, Char, NChar, \
    VarChar, NVarChar, DDLColumn
import sqlalchemy
from datasurface.md import Dataset, DDLTable, DataType, Datastore
from datasurface.md import NullableStatus, PrimaryKeyStatus, Workspace, DatasetGroup, DatasetSink, DataContainer, PostgresDatabase
from datasurface.md import Credential, UserPasswordCredential, EcosystemPipelineGraph


def ddlColumnToSQLAlchemyType(dataType: DDLColumn) -> sqlalchemy.Column[Any]:
    """Converts a DataType to a SQLAlchemy type"""
    # TODO: Timestamp support of timezones

    t: Any = None

    if isinstance(dataType, Boolean):
        t = sqlalchemy.Boolean()
    elif isinstance(dataType, SmallInt):
        t = sqlalchemy.SmallInteger()
    elif isinstance(dataType, Integer):
        t = sqlalchemy.Integer()
    elif isinstance(dataType, BigInt):
        t = sqlalchemy.BigInteger()
    elif isinstance(dataType, IEEE32):
        t = sqlalchemy.Float()
    elif isinstance(dataType, IEEE64):
        t = sqlalchemy.Double()
    elif isinstance(dataType, Decimal):
        dec: Decimal = dataType
        t = sqlalchemy.DECIMAL(dec.maxSize, dec.precision)
    elif isinstance(dataType, Date):
        t = sqlalchemy.Date()
    elif isinstance(dataType, Timestamp):
        t = sqlalchemy.TIMESTAMP()
    elif isinstance(dataType, Interval):
        t = sqlalchemy.Interval()
    elif isinstance(dataType, Variant):
        var: Variant = dataType
        t = sqlalchemy.VARBINARY(var.maxSize)
    elif isinstance(dataType, Char):
        ch: Char = dataType
        t = sqlalchemy.CHAR(ch.maxSize, ch.collationString)
    elif isinstance(dataType, NChar):
        nch: NChar = dataType
        t = sqlalchemy.NCHAR(nch.maxSize, collation=nch.collationString)
    elif isinstance(dataType, VarChar):
        vc: VarChar = dataType
        t = sqlalchemy.VARCHAR(vc.maxSize, vc.collationString)
    elif isinstance(dataType, NVarChar):
        nvc: NVarChar = dataType
        t = sqlalchemy.NVARCHAR(nvc.maxSize, collation=nvc.collationString)
    else:
        raise Exception(f"Unknown data type {dataType.name}")

    c: sqlalchemy.Column[Any] = sqlalchemy.Column(dataType.name, t, nullable=(dataType.nullable == NullableStatus.NULLABLE))
    return c


def datasetToSQLAlchemyTable(dataset: Dataset) -> sqlalchemy.Table:
    """Converts a DDLTable to a SQLAlchemy Table"""
    if (isinstance(dataset.originalSchema, DDLTable)):
        table: DDLTable = dataset.originalSchema
        columns: List[sqlalchemy.Column[Any]] = []
        for col in table.columns.values():
            columns.append(ddlColumnToSQLAlchemyType(col))
        if (table.primaryKeyColumns is not None):
            pk: sqlalchemy.PrimaryKeyConstraint = sqlalchemy.PrimaryKeyConstraint(*table.primaryKeyColumns.colNames)
            sqTable: sqlalchemy.Table = sqlalchemy.Table(dataset.name, sqlalchemy.MetaData(), *columns, pk)
            return sqTable
        else:
            sqTable: sqlalchemy.Table = sqlalchemy.Table(dataset.name, sqlalchemy.MetaData(), *columns)
            return sqTable
    else:
        raise Exception("Unknown schema type")


_T = TypeVar('_T')


def getValueOrThrow(val: Optional[_T]) -> _T:
    """Converts an optional literal to an literal or throws an exception if the value is None"""
    if (val is None):
        raise Exception("Value is None")
    return val


def convertSQLAlchemyTableToDataset(table: sqlalchemy.Table) -> Dataset:
    """Converts a SQLAlchemy Table to a Dataset"""
    columns: List[DDLColumn] = []

    for al_col in table.columns.values():
        colType: Any = al_col.type
        newType: Optional[DataType] = None
        if isinstance(colType, sqlalchemy.Boolean):
            newType = Boolean()
        elif isinstance(colType, sqlalchemy.SmallInteger):
            newType = SmallInt()
        elif isinstance(colType, sqlalchemy.Integer):
            newType = Integer()
        elif isinstance(colType, sqlalchemy.BigInteger):
            newType = BigInt()
        elif isinstance(colType, sqlalchemy.Float):
            newType = IEEE32()
        elif isinstance(colType, sqlalchemy.Double):
            newType = IEEE64()
        elif isinstance(colType, sqlalchemy.DECIMAL):
            s_dec: sqlalchemy.DECIMAL[decimal.Decimal] = colType
            newType = Decimal(getValueOrThrow(s_dec.precision), getValueOrThrow(s_dec.scale))
        elif isinstance(colType, sqlalchemy.Date):
            newType = Date()
        elif isinstance(colType, sqlalchemy.TIMESTAMP):
            newType = Timestamp()
        elif isinstance(colType, sqlalchemy.Interval):
            newType = Interval()
        elif isinstance(colType, sqlalchemy.LargeBinary):
            var_b: sqlalchemy.LargeBinary = colType
            newType = Variant(var_b.length)
        elif isinstance(colType, sqlalchemy.CHAR):
            ch_col: sqlalchemy.CHAR = colType
            newType = Char(getValueOrThrow(ch_col.length), ch_col.collation)
        elif isinstance(colType, sqlalchemy.NCHAR):
            newType = NChar(getValueOrThrow(colType.length), colType.collation)
        elif isinstance(colType, sqlalchemy.VARCHAR):
            newType = VarChar(getValueOrThrow(colType.length), colType.collation)
        elif isinstance(colType, sqlalchemy.TEXT):
            newType = VarChar(None, colType.collation)
        elif isinstance(colType, sqlalchemy.NVARCHAR):
            newType = NVarChar(getValueOrThrow(colType.length), colType.collation)
        if (newType):
            n: NullableStatus = NullableStatus.NOT_NULLABLE
            if (getValueOrThrow(al_col.nullable)):
                n = NullableStatus.NULLABLE
            pk: PrimaryKeyStatus = PrimaryKeyStatus.NOT_PK
            if (getValueOrThrow(al_col.primary_key)):
                pk = PrimaryKeyStatus.PK

            columns.append(DDLColumn(al_col.name, newType, n, pk))
        else:
            raise Exception(f"Unknown data type {al_col.name}: {colType}")

    t: DDLTable = DDLTable(*columns)

    primaryKeyColumns: List[DDLColumn] = []
    for constraint in table.constraints:
        if (isinstance(constraint, sqlalchemy.PrimaryKeyConstraint)):
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


def convertSQLAlchemyTableSetToDatastore(name: str, tables: Sequence[sqlalchemy.Table]) -> Datastore:
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


class SQLAlchemyDataContainerReconciler:
    """This class is used to reconcile a Workspace against the database that it has been assigned to. The Workspace has an
    associated DataContainer which should be a container type which SQLAlchemy supports. The container must
    be a physical container, not a logical one. Reconciling means creating and maintaining schemas in the DataContainer
    to match the model schemas.
    A Datacontainer will have multiple Workspaces assigned to it. Each Workspace has one or more Datasetgroups and each Datasetgroup is assigned to a
    single DataPlatform selected by the data platform chooser. We need to create raw tables in which to contain data supplied by DataPlatforms. We need views
    defined referencing these tables which are used by users of the Workspace. So, we need to gather all Workspaces assigned to a DataContainer.
    We need to get the list of DataPlatform instances to support those workspaces. We need the list of raw tables (one per dataset) used by each DataPlatform.
    We create those tables using a naming convention combining dataplatform instance and datastore/dataset names. We need to create a single view for each Datasetgroup
    """

    def createEngine(self, container: DataContainer, creds: Credential) -> sqlalchemy.Engine:
        if isinstance(creds, UserPasswordCredential):
            if isinstance(container, PostgresDatabase):
                return sqlalchemy.create_engine(
                    'postgresql://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                        username=creds.username,
                        password=creds.password,
                        hostName=container.connection.hostName,
                        port=container.connection.port,
                        databaseName=container.databaseName
                        )
                )
            else:
                raise Exception(f"Unsupported container type {type(container)}")
        else:
            raise Exception(f"Unsupported credential type {type(creds)}")

    def __init__(self, graph: EcosystemPipelineGraph, creds: Credential) -> None:
        """This really needs an intention graph to work out what we are doing"""
        self.workspace: Workspace = workspace
        self.engine: sqlalchemy.Engine = self.createEngine(workspace.container, creds)
        self.graph: EcosystemPipelineGraph = graph

    def reconcileAllKnownDataContainers(self) -> None:
        """This will iterate over all sqlalchemy supported data containers and reconcile the
        table and view schemas for them."""
        
        
