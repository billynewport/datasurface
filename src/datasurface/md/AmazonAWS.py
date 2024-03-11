from typing import Any, Optional, Type
from datasurface.md.AvroSchema import AvroSchema as AvSchema
from datasurface.md.Documentation import Documentation

from datasurface.md.Governance import CloudVendor, DataContainer, SchemaProjector, DataPlatform, Dataset, Ecosystem, InfrastructureLocation, \
    ObjectStorage
from datasurface.md.Lint import ValidationTree
from datasurface.md.Schema import IEEE16, IEEE32, IEEE64, BigInt, Boolean, DDLColumn, DDLTable, DataType, Date, Decimal, Integer, NVarChar, \
    NullableStatus, PrimaryKeyStatus, Schema, SmallInt, Timestamp, TinyInt, VarChar, Variant


class AmazonAWSDataPlatform(DataPlatform):
    def __init__(self, name: str, doc: Documentation):
        super().__init__(name, doc)

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        rc: set[CloudVendor] = set()
        rc.add(CloudVendor.AWS)
        return rc

    def __hash__(self) -> int:
        return hash(self.name)

    def _str__(self) -> str:
        return f"AmazonAWSDataPlatform({self.name})"

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, AmazonAWSDataPlatform)

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return dc.areLocationsOwnedByTheseVendors(eco, {CloudVendor.AWS})

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass

    def getInternalDataContainers(self) -> set[DataContainer]:
        # TODO: Implement this method
        return set()


class AmazonAWSS3Bucket(ObjectStorage):
    def __init__(self, name: str, loc: InfrastructureLocation, endPointURI: Optional[str], bucketName: str, prefix: Optional[str]):
        super().__init__(name, loc, endPointURI, bucketName, prefix)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSS3Bucket)

    def __hash__(self) -> int:
        return hash(self.name)


class AmazonAWSKinesis(DataContainer):
    def __init__(self, name: str, loc: InfrastructureLocation, endPointURI: Optional[str]):
        super().__init__(name, loc)
        self.endPointURI: Optional[str] = endPointURI

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSKinesis) and \
            self.endPointURI == o.endPointURI

    def __hash__(self) -> int:
        return hash(self.name)

    def projectDatasetSchema(self, dataset: 'Dataset') -> SchemaProjector:
        return super().projectDatasetSchema(dataset)


class AmazonAWSDynamoDB(DataContainer):
    def __init__(self, name: str, loc: InfrastructureLocation, endPointURI: Optional[str]):
        super().__init__(name, loc)
        self.endPointURI: Optional[str] = endPointURI

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSDynamoDB) and \
            self.endPointURI == o.endPointURI

    def __hash__(self) -> int:
        return hash(self.name)

    def projectDatasetSchema(self, dataset: 'Dataset') -> SchemaProjector:
        return super().projectDatasetSchema(dataset)


class AmazonAWSSQS(DataContainer):
    def __init__(self, name: str, loc: InfrastructureLocation, queueURL: Optional[str]):
        super().__init__(name, loc)
        self.queueURL: Optional[str] = queueURL

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSSQS) and \
            self.queueURL == o.queueURL

    def __hash__(self) -> int:
        return hash(self.name)

    def projectDatasetSchema(self, dataset: 'Dataset') -> SchemaProjector:
        return super().projectDatasetSchema(dataset)


class GlueDDLSchemaMapper(SchemaProjector):
    def __init__(self, dataset: Dataset):
        super().__init__(dataset)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, GlueDDLSchemaMapper)

    def convertDDLTable(self, table: DDLTable) -> DDLTable:
        glueTable: DDLTable = DDLTable()
        for col in table.columns.values():
            glueType: DataType = col.type
            glueCol: DDLColumn = DDLColumn(col.name, glueType, col.nullable, col.primaryKey)
            if (col.classification):
                for dc in col.classification:
                    glueCol.add(dc)
            glueTable.add(glueCol)
        return glueTable

    def convertAvroTable(self, table: AvSchema) -> AvSchema:
        pass

    def computeSchema(self) -> Optional[Schema]:
        if (self.dataset.originalSchema is None):
            return self.dataset.originalSchema
        if (isinstance(self.dataset.originalSchema, DDLTable)):
            return self.convertDDLTable(self.dataset.originalSchema)
        elif (isinstance(self.dataset.originalSchema, AvSchema)):
            return self.dataset.originalSchema


class GlueTable:
    def __init__(self, table: Dataset):
        self.srcTable: Dataset = table
        self.type_mapping: dict[Type[DataType], str] = {
            TinyInt: "byte",
            SmallInt: "short",
            Integer: "int",
            BigInt: "long",
            # AWS Glue just has unicode, so any string is a string
            NVarChar: "string",
            VarChar: "string",

            IEEE16: "float",
            IEEE32: "float",
            IEEE64: "double",
            Boolean: "boolean",
            Date: "date",
            Timestamp: "timestamp",
            Decimal: "decimal",
            Variant: "binary",
        }

    def convertToAWSType(self, dataType: DataType) -> str:
        if (type(dataType) in self.type_mapping):
            return self.type_mapping[type(dataType)]
        raise Exception(f"Unsupported type: {dataType}")

    def generate_glue_schema(self) -> Optional[dict[str, Any]]:
        if (self.srcTable.originalSchema is None):
            return None

        glue_schema: dict[str, Any] = {
            'Name': self.srcTable.name,
            'StorageDescriptor': {
                # Add more properties as needed, such as Location, InputFormat, OutputFormat, etc.
                # Columns
            },
            # Add more properties as needed, such as PartitionKeys, TableType, Parameters, etc.
        }
        # Add columns if schema is a DDLTable, no need for columns if its avro
        if (isinstance(self.srcTable.originalSchema, DDLTable)):
            schema: DDLTable = self.srcTable.originalSchema
            glue_schema['StorageDescriptor']['Columns'] = [
                    {
                        'Name': col.name,
                        'Type': self.convertToAWSType(col.type),
                        'Parameters': {
                            'nullable': str(col.nullable == NullableStatus.NULLABLE),
                            'primaryKey': str(col.primaryKey == PrimaryKeyStatus.PK)
                        }
                    }
                    for col in schema.columns.values()
                ]
        return glue_schema
