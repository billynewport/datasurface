from typing import Optional, cast
from datasurface.md import Schema, PrimaryKeyList, PartitionKeyList

from avro.schema import Schema as AvSchema
from avro.schema import parse

from avro.schema import RecordSchema, Field, PrimitiveSchema

from datasurface.md.Lint import ValidationTree
from datasurface.md.Policy import DataClassification, DataClassificationPolicy


class AvroSchema(Schema):
    """This allows an avro Schema to be used with a daataset. Primary and partition key column names must be top level
    attribute names"""
    def __init__(self, json_schema: str, classification: Optional[list[DataClassification]] = None,
                 pkCols: Optional[PrimaryKeyList] = None, partCols: Optional[PartitionKeyList] = None):
        super().__init__()
        self.schema: AvSchema = parse(json_schema)
        self.classification: Optional[list[DataClassification]] = classification
        self.primaryKeyColumns = pkCols
        self.ingestionPartitionColumns = partCols

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AvroSchema) and self.schema == o.schema and self.classification == o.classification

    def hasDataClassifications(self) -> bool:
        return self.classification is not None

# TODO This means avro schemas cannot be modified, need a python is backwards compatiblility checker
    def isBackwardsCompatibleWith(self, other: Schema, vTree: ValidationTree) -> bool:
        rc: bool = super().isBackwardsCompatibleWith(other, vTree)
        if (isinstance(other, AvroSchema) and self.schema == other.schema):
            return rc
        else:
            vTree.addProblem("Avro schemas are not equal")
            rc = False
        return rc

    def getHubSchema(self) -> 'Schema':
        return self

    def checkClassificationsAreOnly(self, verifier: DataClassificationPolicy) -> bool:
        # Avro schemas dont allow attribute level classification. There is only schema level.
        if (self.classification):
            for dc in self.classification:
                if not verifier.isCompatible(dc):
                    return False
            return True
        else:
            return True

    def checkColumnsArePrimitiveTypes(self, cols: list[str], tree: ValidationTree):
        """Check all columns exist in the top level record and are primitive types"""
        for col in cols:
            rec: RecordSchema = cast(RecordSchema, self.schema)
            try:
                attribute: Field = cast(Field, rec.fields_dict[col])  # type: ignore
                if (not isinstance(attribute.type, PrimitiveSchema)):  # type: ignore
                    tree.addProblem(f"Column {col} is not a primitive type")
            except KeyError:
                tree.addProblem(f"Unknown column {col}")

    def lint(self, tree: ValidationTree) -> None:
        if (self.primaryKeyColumns):
            self.checkColumnsArePrimitiveTypes(self.primaryKeyColumns.colNames, tree.createChild("PrimaryKeys"))
        if (self.ingestionPartitionColumns):
            self.checkColumnsArePrimitiveTypes(self.ingestionPartitionColumns.colNames, tree.createChild("PartitionKeys"))
