"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from enum import Enum
from typing import Any, Optional, Union, List, cast
from datasurface.md.documentation import Documentation, Documentable
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
    """The column can contain NULL values"""


class PrimaryKeyStatus(Enum):
    """Specifies whether a column is part of the primary key"""
    NOT_PK = 0
    """Not part of a primary key"""
    PK = 1
    """Part of the primary key"""


# Default values for DDLColumn attributes
DEFAULT_nullable = NullableStatus.NULLABLE
DEFAULT_primaryKey = PrimaryKeyStatus.NOT_PK


class DDLColumn(ANSI_SQL_NamedObject, Documentable):
    """This is an individual attribute within a DDLTable schema"""
    def __init__(self,
                 name: str,
                 data_type: DataType,
                 *args: Union[NullableStatus, PrimaryKeyStatus, DataClassification, Documentation],
                 nullable: Optional[NullableStatus] = None,
                 primary_key: Optional[PrimaryKeyStatus] = None,
                 documentation: Optional[Documentation] = None,
                 classifications: Optional[list[DataClassification]] = None) -> None:
        ANSI_SQL_NamedObject.__init__(self, name, "DDLColumn", 1)
        Documentable.__init__(self, documentation)

        # Handle backward compatibility: if *args are provided, parse them the old way
        if args:
            # Legacy mode: parse *args
            parsed_nullable: Optional[NullableStatus] = nullable
            parsed_primary_key: Optional[PrimaryKeyStatus] = primary_key
            parsed_documentation: Optional[Documentation] = documentation
            parsed_classifications: list[DataClassification] = list(classifications) if classifications else []

            for arg in args:
                if isinstance(arg, NullableStatus):
                    parsed_nullable = arg
                elif isinstance(arg, DataClassification):
                    parsed_classifications.append(arg)
                elif isinstance(arg, PrimaryKeyStatus):
                    parsed_primary_key = arg
                else:
                    # Remaining argument should be Documentation
                    parsed_documentation = arg

            # Use parsed values
            self.type: DataType = data_type
            self.nullable: NullableStatus = parsed_nullable if parsed_nullable is not None else DEFAULT_nullable
            self.primaryKey: PrimaryKeyStatus = parsed_primary_key if parsed_primary_key is not None else DEFAULT_primaryKey
            self.classification: Optional[list[DataClassification]] = parsed_classifications if parsed_classifications else None
            if parsed_documentation:
                self.documentation = parsed_documentation
        else:
            # New mode: use named parameters directly (faster!)
            self.type: DataType = data_type
            self.nullable: NullableStatus = nullable if nullable is not None else DEFAULT_nullable
            self.primaryKey: PrimaryKeyStatus = primary_key if primary_key is not None else DEFAULT_primaryKey
            self.classification: Optional[list[DataClassification]] = classifications

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

    @classmethod
    def create_legacy(cls, name: str, data_type: DataType, *args: Union[NullableStatus, DataClassification, PrimaryKeyStatus, Documentation]) -> 'DDLColumn':
        """Legacy factory method for backward compatibility with old *args pattern.
        Use this temporarily during migration, then switch to named parameters for better performance."""
        nullable: Optional[NullableStatus] = None
        primary_key: Optional[PrimaryKeyStatus] = None
        documentation: Optional[Documentation] = None
        classifications: list[DataClassification] = []

        for arg in args:
            if isinstance(arg, NullableStatus):
                nullable = arg
            elif isinstance(arg, DataClassification):
                classifications.append(arg)
            elif isinstance(arg, PrimaryKeyStatus):
                primary_key = arg
            else:
                # Remaining argument should be Documentation
                documentation = arg

        return cls(
            name=name,
            data_type=data_type,
            nullable=nullable,
            primary_key=primary_key,
            documentation=documentation,
            classifications=classifications if classifications else None
        )

    def __eq__(self, o: object) -> bool:
        if (type(o) is not DDLColumn):
            return False
        return ANSI_SQL_NamedObject.__eq__(self, o) and Documentable.__eq__(self, o) and self.type == o.type and \
            self.primaryKey == o.primaryKey and self.nullable == o.nullable and self.classification == o.classification

    def checkForBackwardsCompatibility(self, other: object, vTree: ValidationTree) -> bool:
        """Returns true if this column is backwards compatible with the other column"""
        # TODO Add support to changing the column data type to a compatible type
        super().checkForBackwardsCompatibility(other, vTree)
        if not isinstance(other, DDLColumn):
            vTree.addProblem(f"Cannot compare {self.__class__.__name__} with {other.__class__.__name__}")
            return False

        self.type.checkIfBackwardsCompatibleWith(other.type, vTree)
        if (self.nullable != other.nullable):
            vTree.addProblem(f"Nullable status for {self.name} changed from {self.nullable} to {other.nullable}")
        if (self.classification != other.classification):
            vTree.addProblem(f"Data classification for {self.name} changed from {self.classification} to {other.classification}")
        if (self.primaryKey != other.primaryKey):
            vTree.addProblem(f"Primary key status for {self.name} changed from {self.primaryKey} to {other.primaryKey}")
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


class AttributeList(UserDSLObject):
    """A list of column names."""
    def __init__(self, colNames: list[str]) -> None:
        UserDSLObject.__init__(self)
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


class Schema(Documentable):
    """This is a basic schema in the system. It has base meta attributes common for all schemas and core methods for all schemas"""
    def __init__(self) -> None:
        Documentable.__init__(self, None)
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

    def __init__(self,
                 *args: Union[DDLColumn, PrimaryKeyList, PartitionKeyList, Documentation],
                 columns: Optional[list[DDLColumn]] = None,
                 primary_key_list: Optional[PrimaryKeyList] = None,
                 partition_key_list: Optional[PartitionKeyList] = None,
                 documentation: Optional[Documentation] = None) -> None:
        super().__init__()
        self.columns: dict[str, DDLColumn] = OrderedDict[str, DDLColumn]()

        # Handle backward compatibility: if *args are provided, parse them the old way
        if args:
            # Legacy mode: parse *args (slower but compatible)
            parsed_columns: list[DDLColumn] = list(columns) if columns else []
            parsed_primary_key_list: Optional[PrimaryKeyList] = primary_key_list
            parsed_partition_key_list: Optional[PartitionKeyList] = partition_key_list
            parsed_documentation: Optional[Documentation] = documentation

            for arg in args:
                if isinstance(arg, DDLColumn):
                    parsed_columns.append(arg)
                elif isinstance(arg, PrimaryKeyList):
                    parsed_primary_key_list = arg
                elif isinstance(arg, PartitionKeyList):
                    parsed_partition_key_list = arg
                else:
                    # Remaining argument should be Documentation
                    parsed_documentation = arg

            # Use parsed values
            if parsed_columns:
                for col in parsed_columns:
                    if self.columns.get(col.name) is not None:
                        raise Exception(f"Duplicate column {col.name}")
                    self.columns[col.name] = col

            if parsed_primary_key_list:
                self.primaryKeyColumns = parsed_primary_key_list

            if parsed_partition_key_list:
                self.ingestionPartitionColumns = parsed_partition_key_list

            if parsed_documentation:
                self.documentation = parsed_documentation
        else:
            # New mode: use named parameters directly (faster!)
            if columns:
                for col in columns:
                    if self.columns.get(col.name) is not None:
                        raise Exception(f"Duplicate column {col.name}")
                    self.columns[col.name] = col

            if primary_key_list:
                self.primaryKeyColumns = primary_key_list

            if partition_key_list:
                self.ingestionPartitionColumns = partition_key_list

            if documentation:
                self.documentation = documentation

        self.calculateKeys()

    @classmethod
    def create_legacy(cls, *args: Union[DDLColumn, PrimaryKeyList, PartitionKeyList, Documentation]) -> 'DDLTable':
        """Legacy factory method for backward compatibility with old *args pattern.
        Use this temporarily during migration, then switch to named parameters for better performance."""
        columns: list[DDLColumn] = []
        primary_key_list: Optional[PrimaryKeyList] = None
        partition_key_list: Optional[PartitionKeyList] = None
        documentation: Optional[Documentation] = None

        for arg in args:
            if isinstance(arg, DDLColumn):
                columns.append(arg)
            elif isinstance(arg, PrimaryKeyList):
                primary_key_list = arg
            elif isinstance(arg, PartitionKeyList):
                partition_key_list = arg
            else:
                # Remaining argument should be Documentation
                documentation = arg

        return cls(
            columns=columns if columns else None,
            primary_key_list=primary_key_list,
            partition_key_list=partition_key_list,
            documentation=documentation
        )

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
        return self.columns == o.columns and self.columns == o.columns and self.primaryKeyColumns == o.primaryKeyColumns and \
            self.ingestionPartitionColumns == o.ingestionPartitionColumns

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

    def add(self, column: DDLColumn) -> None:
        """Add a column to the table. Provided for backward compatibility."""
        if self.columns.get(column.name) is not None:
            raise Exception(f"Duplicate column {column.name}")
        self.columns[column.name] = column
        self.calculateKeys()
