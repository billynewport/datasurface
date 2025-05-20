"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from enum import Enum
from typing import Any, Optional, Union, List, cast
from datasurface.md.documentation import Documentation, Documentable
from datasurface.md.json import JSONable
from datasurface.md.lint import UserDSLObject, ValidationTree, ValidationProblem, ProblemSeverity, ANSI_SQL_NamedObject
from datasurface.md.types import DataType
from datasurface.md.utils import is_valid_sql_identifier
from datasurface.md.policy import DataClassification, DataClassificationPolicy
from abc import abstractmethod
from collections import OrderedDict


class NullableStatus(Enum):
    """Specifies whether a column is nullable"""
    NOT_NULLABLE = 0
    """This column cannot store null values"""
    NULLABLE = 1
    """This column can store null values"""


class PrimaryKeyStatus(Enum):
    """Specifies whether a column is part of the primary key"""
    NOT_PK = 0
    """Not part of a primary key"""
    PK = 1
    """Part of the primary key"""


DEFAULT_primaryKey: PrimaryKeyStatus = PrimaryKeyStatus.NOT_PK

DEFAULT_nullable: NullableStatus = NullableStatus.NULLABLE


class DDLColumn(ANSI_SQL_NamedObject, Documentable, JSONable):
    """This is an individual attribute within a DDLTable schema"""
    def __init__(self, name: str, dataType: DataType, *args: Union[NullableStatus, DataClassification, PrimaryKeyStatus, Documentation]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name, "DDLColumn", 1)
        Documentable.__init__(self, None)
        JSONable.__init__(self)
        self.type: DataType = dataType
        self.primaryKey: PrimaryKeyStatus = DEFAULT_primaryKey
        self.classification: Optional[list[DataClassification]] = None
        self.nullable: NullableStatus = DEFAULT_nullable
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "name": self.name,
            "type": self.type.to_json(),
            "nullable": self.nullable.value,
            "primaryKey": self.primaryKey.value,
            "classification": [c.to_json() for c in self.classification] if self.classification else None,
            "doc": self.documentation.to_json() if self.documentation else None,
        }

    def add(self, *args: Union[NullableStatus, DataClassification, PrimaryKeyStatus, Documentation]) -> None:
        for arg in args:
            if (isinstance(arg, NullableStatus)):
                self.nullable = arg
            elif (isinstance(arg, DataClassification)):
                if (self.classification is None):
                    self.classification = list()
                self.classification.append(arg)
            elif (isinstance(arg, PrimaryKeyStatus)):
                self.primaryKey = arg
            else:
                self.documentation = arg

    def __eq__(self, o: object) -> bool:
        if (type(o) is not DDLColumn):
            return False
        return super().__eq__(o) and self.type == o.type and self.primaryKey == o.primaryKey and self.nullable == o.nullable and \
            self.classification == o.classification

    def checkForBackwardsCompatibility(self, other: object, vTree: ValidationTree) -> bool:
        """Returns true if this column is backwards compatible with the other column"""
        # TODO Add support to changing the column data type to a compatible type
        super().checkForBackwardsCompatibility(other, vTree)
        if not isinstance(other, DDLColumn):
            vTree.addProblem(f"Cannot compare {self.__class__.__name__} with {other.__class__.__name__}")
            return False

        self.type.isBackwardsCompatibleWith(other.type, vTree)
        if (self.nullable != other.nullable):
            vTree.addProblem(f"Nullable status for {self.name} changed from {self.nullable} to {other.nullable}")
        if (self.classification != other.classification):
            vTree.addProblem(f"Data classification for {self.name} changed from {self.classification} to {other.classification}")
        return not vTree.hasErrors()

    def lint(self, tree: ValidationTree) -> None:
        super().nameLint(tree)
        self.type.lint(tree)
        if (self.documentation):
            self.documentation.lint(tree.addSubTree(self.documentation))
        if (self.primaryKey == PrimaryKeyStatus.PK and self.nullable == NullableStatus.NULLABLE):
            tree.addProblem(f"Primary key column {self.name} cannot be nullable")

    def __str__(self) -> str:
        return f"DDLColumn({self.name})"


class AttributeList(UserDSLObject, JSONable):
    """A list of column names."""
    def __init__(self, colNames: list[str]) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.colNames: List[str] = []
        for col in colNames:
            self.colNames.append(col)

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "colNames": self.colNames}

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AttributeList) and self.colNames == other.colNames

    def lint(self, tree: ValidationTree) -> None:
        for col in self.colNames:
            if not is_valid_sql_identifier(col):
                tree.addProblem(f"Column name {col} is not a valid ANSI SQL identifier")

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.colNames})"


class PrimaryKeyList(AttributeList):
    """A list of columns to be used as the primary key"""
    def __init__(self, colNames: list[str]) -> None:
        super().__init__(colNames)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, PrimaryKeyList)


class PartitionKeyList(AttributeList):
    """A list of column names used for partitioning ingested data"""
    def __init__(self, colNames: list[str]) -> None:
        super().__init__(colNames)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, PartitionKeyList)


class NotBackwardsCompatible(ValidationProblem):
    """This is a validation problem that indicates that the schema is not backwards compatible"""
    def __init__(self, problem: str) -> None:
        super().__init__(problem, ProblemSeverity.ERROR)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, NotBackwardsCompatible)

    def __hash__(self) -> int:
        return hash(str(self))


class Schema(Documentable, UserDSLObject, JSONable):
    """This is a basic schema in the system. It has base meta attributes common for all schemas and core methods for all schemas"""
    def __init__(self) -> None:
        Documentable.__init__(self, None)
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.primaryKeyColumns: Optional[PrimaryKeyList] = None
        self.ingestionPartitionColumns: Optional[PartitionKeyList] = None
        """How should this dataset be partitioned for ingestion and storage"""

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"primaryKeyColumns": self.primaryKeyColumns.to_json() if self.primaryKeyColumns else None})
        rc.update({"ingestionPartitionColumns": self.ingestionPartitionColumns.to_json() if self.ingestionPartitionColumns else None})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, Schema)):
            return self.primaryKeyColumns == other.primaryKeyColumns and self.ingestionPartitionColumns == other.ingestionPartitionColumns and \
                self.documentation == other.documentation
        else:
            return False

    @abstractmethod
    def getHubSchema(self) -> 'Schema':
        """Returns the hub schema for this schema"""
        pass

    @abstractmethod
    def checkForBackwardsCompatibility(self, other: 'Schema', vTree: ValidationTree) -> bool:
        """Returns true if this schema is backward compatible with the other schema"""
        # Primary keys cannot change
        if (self.primaryKeyColumns != other.primaryKeyColumns):
            vTree.addRaw(NotBackwardsCompatible(f"Primary key columns cannot change from {self.primaryKeyColumns} to {other.primaryKeyColumns}"))
        # Partitioning cannot change
        if (self.ingestionPartitionColumns != other.ingestionPartitionColumns):
            vTree.addRaw(NotBackwardsCompatible(f"Partitioning cannot change from {self.ingestionPartitionColumns} to {other.ingestionPartitionColumns}"))
        if self.documentation:
            self.documentation.lint(vTree.addSubTree(self.documentation))
        return not vTree.hasErrors()

    @abstractmethod
    def checkClassificationsAreOnly(self, verifier: DataClassificationPolicy) -> bool:
        """Returns true if all columns in this schema have the specified classification"""
        pass

    @abstractmethod
    def hasDataClassifications(self) -> bool:
        """Returns True if the schema has any data classification specified"""
        pass

    @abstractmethod
    def lint(self, tree: ValidationTree) -> None:
        """This method performs linting on this schema"""
        if (self.primaryKeyColumns):
            self.primaryKeyColumns.lint(tree.addSubTree(self.primaryKeyColumns))

        if (self.ingestionPartitionColumns):
            self.ingestionPartitionColumns.lint(tree.addSubTree(self.ingestionPartitionColumns))


class DDLTable(Schema):
    """Table definition"""

    def __init__(self, *args: Union[DDLColumn, PrimaryKeyList, PartitionKeyList, Documentation]) -> None:
        super().__init__()
        self.columns: dict[str, DDLColumn] = OrderedDict[str, DDLColumn]()
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"columns": {k: v.to_json() for k, v in self.columns.items()}})
        return rc

    def hasDataClassifications(self) -> bool:
        for col in self.columns.values():
            if (col.classification):
                return True
        return False

    def checkClassificationsAreOnly(self, verifier: DataClassificationPolicy) -> bool:
        """Check all columns comply with the verifier"""
        for col in self.columns.values():
            if col.classification:
                for dc in col.classification:
                    if not verifier.isCompatible(dc):
                        return False
        return True

    def add(self, *args: Union[DDLColumn, PrimaryKeyList, PartitionKeyList, Documentation]):
        """Add a column or primary key list to the table"""
        for c in args:
            if (isinstance(c, DDLColumn)):
                if (self.columns.get(c.name) is not None):
                    raise Exception(f"Duplicate column {c.name}")
                self.columns[c.name] = c
            elif (isinstance(c, PrimaryKeyList)):
                self.primaryKeyColumns = c
            elif (isinstance(c, PartitionKeyList)):
                self.ingestionPartitionColumns = c
            else:
                self.documentation = c
        self.calculateKeys()

    def calculateKeys(self):
        """If a primarykey list is specified then set each column pk correspondingly otherwise construct a primary key list from the pk flag"""
        if (self.primaryKeyColumns):
            for col in self.columns.values():
                col.primaryKey = PrimaryKeyStatus.NOT_PK
            for col in self.primaryKeyColumns.colNames:
                self.columns[col].primaryKey = PrimaryKeyStatus.PK
        else:
            keyNameList: List[str] = []
            for col in self.columns.values():
                if (col.primaryKey == PrimaryKeyStatus.PK):
                    keyNameList.append(col.name)
            self.primaryKeyColumns = PrimaryKeyList(keyNameList)

    def getHubSchema(self) -> 'Schema':
        """Returns the hub schema for this schema"""
        return self

    def getColumnByName(self, name: str) -> Optional[DDLColumn]:
        """Returns a column by name"""
        col: DDLColumn = self.columns[name]
        return col

    def __eq__(self, o: object) -> bool:
        if (not super().__eq__(o)):
            return False
        if (type(o) is not DDLTable):
            return False
        return self.columns == o.columns and self.columns == o.columns

    def checkForBackwardsCompatibility(self, other: 'Schema', vTree: ValidationTree) -> bool:
        """Returns true if this schema is backward compatible with the other schema"""
        super().checkForBackwardsCompatibility(other, vTree)
        if vTree.checkTypeMatches(other, DDLTable):
            currentDDL: DDLTable = cast(DDLTable, other)
            # New tables must contain all old columns
            for col in currentDDL.columns.values():
                if (col.name not in self.columns):
                    vTree.addRaw(NotBackwardsCompatible(f"Column {col.name} is missing from the new schema"))
            # Existing columns must be compatible
            for col in self.columns.values():
                cTree: ValidationTree = vTree.addSubTree(col)
                newCol: Optional[DDLColumn] = currentDDL.columns.get(col.name)
                if (newCol):
                    newCol.checkForBackwardsCompatibility(col, cTree)

            # Now check additional columns
            newColumnNames: set[str] = set(self.columns.keys())
            currColNames: set[str] = set(currentDDL.columns.keys())
            additionalColumns: set[str] = newColumnNames.difference(currColNames)
            for colName in additionalColumns:
                col: DDLColumn = self.columns[colName]
                # Additional columns cannot be primary keys
                if (col.primaryKey == PrimaryKeyStatus.PK):
                    vTree.addRaw(NotBackwardsCompatible(f"Column {col.name} cannot be a new primary key column"))
                # Additional columns must be nullable
                if col.nullable == NullableStatus.NOT_NULLABLE:
                    vTree.addRaw(NotBackwardsCompatible(f"Column {col.name} must be nullable"))
        return not vTree.hasErrors()

    def lint(self, tree: ValidationTree) -> None:
        """This method performs linting on this schema"""
        super().lint(tree)
        if self.primaryKeyColumns:
            pkTree: ValidationTree = tree.addSubTree(self.primaryKeyColumns)
            self.primaryKeyColumns.lint(pkTree)
            for colName in self.primaryKeyColumns.colNames:
                if (colName not in self.columns):
                    pkTree.addRaw(NotBackwardsCompatible(f"Primary key column {colName} is not in the column list"))
                else:
                    col: DDLColumn = self.columns[colName]
                    if (col.primaryKey != PrimaryKeyStatus.PK):
                        tree.addRaw(NotBackwardsCompatible(f"Column {colName} should be marked primary key column"))
                    if (col.nullable == NullableStatus.NULLABLE):
                        tree.addRaw(NotBackwardsCompatible(f"Primary key column {colName} cannot be nullable"))
            for col in self.columns.values():
                colTree: ValidationTree = tree.addSubTree(col)
                col.lint(colTree)
                if col.primaryKey == PrimaryKeyStatus.PK and col.name not in self.primaryKeyColumns.colNames:
                    colTree.addRaw(NotBackwardsCompatible(f"Column {col.name} is marked as primary key but is not in the primary key list"))
        else:
            tree.addProblem("Table must have a primary key list")

        # If partitioning columns are specified then they must exist and be non nullable
        if self.ingestionPartitionColumns:
            for colName in self.ingestionPartitionColumns.colNames:
                if (colName not in self.columns):
                    tree.addRaw(NotBackwardsCompatible(f"Partitioning column {colName} is not in the column list"))
                else:
                    col: DDLColumn = self.columns[colName]
                    if (col.nullable == NullableStatus.NULLABLE):
                        tree.addRaw(NotBackwardsCompatible(f"Partitioning column {colName} cannot be nullable"))

    def __str__(self) -> str:
        return "DDLTable()"
