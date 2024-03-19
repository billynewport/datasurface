import re
from typing import Any, Optional, Type
from datasurface.md.AvroSchema import AvroSchema as AvSchema
from datasurface.md.Documentation import Documentation

from datasurface.md.Governance import CaseSensitiveEnum, CloudVendor, Credential, DataContainer, \
    DataContainerNamingMapper, DataPlatformExecutor, DatasetGroup, \
    Datastore, SQLDatabase, SchemaProjector, DataPlatform, Dataset, Ecosystem, InfrastructureLocation, \
    ObjectStorage, Workspace

from datasurface.md.Lint import NameHasBadSynthax, ValidationTree
from datasurface.md.Schema import IEEE16, IEEE32, IEEE64, BigInt, Boolean, DDLColumn, DDLTable, DataType, Date, Decimal, Integer, NVarChar, \
    NullableStatus, PrimaryKeyStatus, Schema, SmallInt, Timestamp, TinyInt, VarChar, Variant


class AmazonAWSDataPlatform(DataPlatform):
    def __init__(self, name: str, doc: Documentation, executor: DataPlatformExecutor):
        super().__init__(name, doc, executor)

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


class AWSAuroraCluster(SQLDatabase):
    """This is a link to an existing Aurora database. It identifies the cluster using its
    clusterId and the database using its databaseName. The location is the location/region of the cluster. We can using Terraform
    retrieve the hostname and port for this database."""
    def __init__(self, name: str, loc: InfrastructureLocation, clusterId: str, databaseName: str):
        super().__init__(name, loc, databaseName)
        self.clusterId: str = clusterId

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AWSAuroraCluster) and \
            self.clusterId == o.clusterId


class AWSSecret(Credential):
    """This represents a secret stored in AWS Secret Manager. It is used to store sensitive information such as passwords, tokens, etc."""
    def __init__(self, secretName: str, loc: InfrastructureLocation):
        super().__init__()
        self.secretName: str = secretName
        self.loc: InfrastructureLocation = loc

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AWSSecret) and \
            self.secretName == o.secretName and self.loc == o.loc

    def __hash__(self) -> int:
        return hash(self.secretName)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        # Check secretName has only alphanumeric characters and /_+=.@- using a regular expression
        pattern = re.compile('^[a-zA-Z0-9/_+=.@-]*$')
        if not pattern.match(self.secretName):
            tree.addRaw(NameHasBadSynthax(f"Secret name {self.secretName} contains invalid characters. Only alphanumeric characters and /_+=.@- are allowed."))


class AWSGlueNamingMapper(DataContainerNamingMapper):
    """This is the identifier mapping adapter for AWS Glue. It truncates identifiers to 255 characters and
    as they are case insensitive, it uppercases all identifiers."""

    def __init__(self):
        super().__init__(255, CaseSensitiveEnum.CASE_INSENSITIVE, None)

    def mapRawDatasetName(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        return super().mapRawDatasetName(w, dsg, store, ds)

    def mapRawDatasetView(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        return super().mapRawDatasetView(w, dsg, store, ds)

    def mapAttributeName(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset', attributeName: str) -> str:
        return super().mapAttributeName(w, dsg, store, ds, attributeName)


class AmazonAWSS3Bucket(ObjectStorage):
    def __init__(self, name: str, loc: InfrastructureLocation, endPointURI: Optional[str], bucketName: str, prefix: Optional[str]):
        super().__init__(name, loc, endPointURI, bucketName, prefix)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSS3Bucket)

    def __hash__(self) -> int:
        return hash(self.name)

    def getNamingAdapter(self) -> DataContainerNamingMapper:
        return AWSGlueNamingMapper()

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)


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

    def getNamingAdapter(self) -> DataContainerNamingMapper:
        return AWSGlueNamingMapper()


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

    def getNamingAdapter(self) -> DataContainerNamingMapper:
        return AWSGlueNamingMapper()


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

    def getNamingAdapter(self) -> DataContainerNamingMapper:
        return AWSGlueNamingMapper()


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


class AWSDMSIceBergDataPlatform(AmazonAWSDataPlatform):
    """This data platform ingests data using AWS DMS in to an S3 based staging area before maintaining an Iceberg based AWS Glue data lake.
    This class is used for declare the instance parameters for this platform. The actual platform will load the ecosystem model and then
    initialize itself using the instance variables of this class."""
    def __init__(
            self,
            name: str,
            doc: Documentation,
            executor: DataPlatformExecutor,
            vpcId: str,
            iacCredential: AWSSecret,
            region: InfrastructureLocation,
            stagingBucket: AmazonAWSS3Bucket,
            dataBucket: AmazonAWSS3Bucket,
            catalogDatabaseName: str,
            stagingIAMRole: str,
            dataIAMRole: str,
            awsGlueIAMRole: str):
        super().__init__(name, doc, executor)
        self.vpcId: str = vpcId
        self.iacCredential: Credential = iacCredential
        self.region: InfrastructureLocation = region
        self.stagingBucket: AmazonAWSS3Bucket = stagingBucket
        self.dataBucket: AmazonAWSS3Bucket = dataBucket
        self.catalogDatabaseName: str = catalogDatabaseName
        self.stagingIAMRole: str = stagingIAMRole
        self.dataIAMRole: str = dataIAMRole
        self.awsGlueIAMRole: str = awsGlueIAMRole

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, AWSDMSIceBergDataPlatform)

    def _str__(self) -> str:
        return f"AWSDMSIceBergDataPlatform({self.name})"

    def getInternalDataContainers(self) -> set[DataContainer]:
        return {self.stagingBucket, self.dataBucket}

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)
        self.stagingBucket.lint(eco, tree)
        self.dataBucket.lint(eco, tree)
        self.iacCredential.lint(eco, tree)
        if (not self.stagingBucket.areAllLocationsInLocations({self.region})):
            tree.addRaw(NameHasBadSynthax(f"Staging bucket location {self.stagingBucket} does not match region {self.region}"))
        if (not self.dataBucket.areAllLocationsInLocations({self.region})):
            tree.addRaw(NameHasBadSynthax(f"Data bucket location {self.dataBucket} does not match region {self.region}"))
