import decimal
from typing import Any, List, Optional, Sequence, TypeVar
from datasurface.md import Boolean, SmallInt, Integer, BigInt, IEEE32, IEEE64, Decimal, Date, Timestamp, Interval, Variant, Char, NChar, \
    VarChar, NVarChar, DDLColumn
import sqlalchemy
from datasurface.md import Dataset, DDLTable, DataType, Datastore
from datasurface.md.Schema import NullableStatus, PrimaryKeyStatus


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

    c: sqlalchemy.Column[Any] = sqlalchemy.Column(dataType.name, t, primary_key=(dataType.primaryKey == PrimaryKeyStatus.PK),
                                                  nullable=(dataType.nullable == NullableStatus.NULLABLE))
    return c


def datasetToSQLAlchemyTable(dataset: Dataset) -> sqlalchemy.Table:
    """Converts a DDLTable to a SQLAlchemy Table"""
    if (isinstance(dataset.originalSchema, DDLTable)):
        table: DDLTable = dataset.originalSchema
        columns: List[sqlalchemy.Column[Any]] = []
        for col in table.columns.values():
            columns.append(ddlColumnToSQLAlchemyType(col))
        return sqlalchemy.Table(dataset.name, sqlalchemy.MetaData(), *columns)
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
