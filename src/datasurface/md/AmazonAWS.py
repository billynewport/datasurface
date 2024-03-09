from typing import Any, Optional
from datasurface.md import Documentation
from datasurface.md.Governance import CloudVendor, DataContainer, DataPlatform, Dataset, Ecosystem, InfrastructureLocation, \
    ObjectStorage
from datasurface.md.Lint import ValidationTree
from datasurface.md.Schema import IEEE32, IEEE64, BigInt, Boolean, DDLTable, DataType, Date, Decimal, Integer, NVarChar, \
    NullableStatus, PrimaryKeyStatus, SmallInt, Timestamp, TinyInt, VarChar, Variant


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
        return dc.isUsingVendorsOnly(eco, {CloudVendor.AWS})

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


class AmazonAWSDynamoDB(DataContainer):
    def __init__(self, name: str, loc: InfrastructureLocation, endPointURI: Optional[str]):
        super().__init__(name, loc)
        self.endPointURI: Optional[str] = endPointURI

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSDynamoDB) and \
            self.endPointURI == o.endPointURI

    def __hash__(self) -> int:
        return hash(self.name)


class AmazonAWSSQS(DataContainer):
    def __init__(self, name: str, loc: InfrastructureLocation, queueURL: Optional[str]):
        super().__init__(name, loc)
        self.queueURL: Optional[str] = queueURL

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSSQS) and \
            self.queueURL == o.queueURL

    def __hash__(self) -> int:
        return hash(self.name)


class GlueTable:
    def __init__(self, table: Dataset):
        self.srcTable: Dataset = table

    def convertToAWSType(self, type: DataType) -> str:
        if (isinstance(type, TinyInt)):
            return "byte"
        if (isinstance(type, SmallInt)):
            return "short"
        if (isinstance(type, Integer)):
            return "int"
        if (isinstance(type, BigInt)):
            return "long"
        # AWS Glue just has unicode, so any string is a string
        if (isinstance(type, NVarChar) or isinstance(type, VarChar)):
            return "string"
        if (isinstance(type, IEEE32)):
            return "float"
        if (isinstance(type, IEEE64)):
            return "double"
        if (isinstance(type, Boolean)):
            return "boolean"
        if (isinstance(type, Date)):
            return "date"
        if (isinstance(type, Timestamp)):
            return "timestamp"
        if (isinstance(type, Decimal)):
            return "decimal"
        if (isinstance(type, Variant)):
            return "binary"
        raise Exception(f"Unsupported type: {type}")

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
