"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import re
from typing import Any, Optional, Type, cast

from jinja2 import Environment, PackageLoader, Template, select_autoescape

from datasurface.md import AvroSchema as AvSchema
from datasurface.md import Documentation, PlainTextDocumentation
from urllib import parse

from datasurface.md import CaseSensitiveEnum, CloudVendor, Credential, DataContainer, CredentialStore, \
    DataContainerNamingMapper, DataPlatformExecutor, DataTransformerNode, DatasetGroup, \
    Datastore, DatastoreCacheEntry, ExportNode, FileBasedFragmentManager, IaCDataPlatformRenderer, IaCDataPlatformRendererShim, \
    IngestionMetadata, IngestionMultiNode, IngestionSingleNode, PipelineNode, PlatformPipelineGraph, SQLDatabase, SchemaProjector, \
    DataPlatform, Dataset, Ecosystem, InfrastructureLocation, \
    ObjectStorage, TriggerNode, UnsupportedDataContainer, Workspace, defaultPipelineNodeFileName, DataPlatformGraphHandler

from datasurface.md import NameHasBadSynthax, ValidationTree
from datasurface.md import IEEE16, IEEE32, IEEE64, BigInt, Boolean, DDLColumn, DDLTable, \
    DataType, Date, Decimal, Integer, NVarChar, \
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

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, AmazonAWSDataPlatform)

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return dc.areLocationsOwnedByTheseVendors(eco, {CloudVendor.AWS})

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)

    def getInternalDataContainers(self) -> set[DataContainer]:
        # TODO: Implement this method
        return set()

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        return IaCDataPlatformRendererShim(self.executor, graph)


class AWSAuroraDatabase(SQLDatabase):
    """This is a link to an existing Aurora database. It identifies the cluster using its
    endpointName and the database using its databaseName. The location is the location/region of the cluster. We can using Terraform
    retrieve the hostname and port for this database."""
    def __init__(self, name: str, locs: set[InfrastructureLocation], endpointName: str, databaseName: str):
        super().__init__(name, locs, databaseName)
        self.endpointName: str = endpointName

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AWSAuroraDatabase) and \
            self.endpointName == o.endpointName

    def __str__(self) -> str:
        return f"AWSAuroraDatabase({self.name}, {self.endpointName})"

    def __hash__(self) -> int:
        return hash(self.name)


def is_valid_aws_name(name: str) -> bool:
    # Check name has only alphanumeric characters and /_+=.@- using a regular expression
    pattern = re.compile('^[a-zA-Z0-9/_+=.@-]*$')
    return pattern.match(name) is not None


def is_valid_s3_bucket_name(name: str) -> bool:
    # Bucket names must be a series of one or more labels, separated
    # by a single period (.). Each label must start and end with a lowercase letter or a number.
    # Bucket names must contain only lowercase letters, numbers, dots (.), and hyphens (-).
    # Each label in the bucket name must be between 1 and 63 characters long.
    # Bucket names cannot be formatted as an IP address (for example, 192.168.5.4).
    # Bucket names cannot begin with a hyphen (-) or contain two consecutive hyphens (--).
    # Bucket names cannot end with a hyphen (-) or contain a dot followed by a hyphen (. -) or a hyphen followed by a dot (- .).

    # Check length
    if len(name) < 1 or len(name) > 255:
        return False

    # Check if name is formatted as an IP address
    if re.match(r'^\d+\.\d+\.\d+\.\d+$', name):
        return False

    # Check each label
    labels = name.split('.')
    for label in labels:
        # Check length
        if len(label) < 1 or len(label) > 63:
            return False
        # Check start and end characters
        if not re.match(r'^[a-z0-9].*[a-z0-9]$', label):
            return False
        # Check allowed characters and consecutive hyphens
        if re.match(r'^[a-z0-9-]*$', label) is None or '--' in label:
            return False

    return True


class AWSSecretManager(CredentialStore):
    """This represents the AWS Secret manager for a region. It's used to reference credentials stored in that region."""
    def __init__(self, name: str, locs: set[InfrastructureLocation]):
        super().__init__(name, locs)
        self.secrets: dict[str, Credential] = {}

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AWSSecretManager)

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)

    def getCredential(self, credName: str) -> Credential:
        """Get a credential from the store. If it does not exist, create it."""
        if credName not in self.secrets:
            cred: Credential = AWSSecret(self, credName)
            self.secrets[credName] = cred
        return super().getCredential(credName)


class AWSSecret(Credential):
    """This represents a secret stored in AWS Secret Manager. It is used to store sensitive information such as passwords, tokens, etc."""
    def __init__(self, manager: AWSSecretManager, secretName: str):
        super().__init__()
        self.manager: AWSSecretManager = manager
        self.secretName: str = secretName

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AWSSecret) and \
            self.secretName == o.secretName and self.manager == o.manager

    def __hash__(self) -> int:
        return hash(self.secretName)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        # Check secretName has only alphanumeric characters and /_+=.@- using a regular expression
        if not is_valid_aws_name(self.secretName):
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
    def __init__(self, name: str, locs: set[InfrastructureLocation], endPointURI: Optional[str], bucketName: str, prefix: Optional[str]):
        super().__init__(name, locs, endPointURI, bucketName, prefix)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSS3Bucket)

    def __hash__(self) -> int:
        return hash(self.name)

    def getNamingAdapter(self) -> DataContainerNamingMapper:
        return AWSGlueNamingMapper()

    def is_valid_s3_bucket_prefix(self, prefix: str) -> bool:
        if len(prefix) > 1024:
            return False
        if prefix != parse.unquote(parse.quote(prefix)):
            return False
        return True    # Validate the bucket name

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)
        if not is_valid_s3_bucket_name(self.bucketName):
            tree.addRaw(NameHasBadSynthax(f"S3 bucket name {self.bucketName} is invalid."))
        if self.prefix and not self.is_valid_s3_bucket_prefix(self.prefix):
            tree.addRaw(NameHasBadSynthax(f"S3 bucket prefix {self.prefix} is invalid."))


class AmazonAWSKinesis(DataContainer):
    def __init__(self, name: str, locs: set[InfrastructureLocation], endPointURI: Optional[str]):
        super().__init__(name, locs)
        self.endPointURI: Optional[str] = endPointURI

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSKinesis) and \
            self.endPointURI == o.endPointURI

    def __hash__(self) -> int:
        return hash(self.name)

    def projectDatasetSchema(self, dataset: 'Dataset') -> Optional[SchemaProjector]:
        return super().projectDatasetSchema(dataset)

    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
        return AWSGlueNamingMapper()


class AmazonAWSDynamoDB(DataContainer):
    def __init__(self, name: str, locs: set[InfrastructureLocation], endPointURI: Optional[str]):
        super().__init__(name, locs)
        self.endPointURI: Optional[str] = endPointURI

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSDynamoDB) and \
            self.endPointURI == o.endPointURI

    def __hash__(self) -> int:
        return hash(self.name)

    def projectDatasetSchema(self, dataset: 'Dataset') -> Optional[SchemaProjector]:
        return super().projectDatasetSchema(dataset)

    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
        return AWSGlueNamingMapper()


class AmazonAWSSQS(DataContainer):
    def __init__(self, name: str, locs: set[InfrastructureLocation], queueURL: Optional[str]):
        super().__init__(name, locs)
        self.queueURL: Optional[str] = queueURL

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSSQS) and \
            self.queueURL == o.queueURL

    def __hash__(self) -> int:
        return hash(self.name)

    def projectDatasetSchema(self, dataset: 'Dataset') -> Optional[SchemaProjector]:
        return super().projectDatasetSchema(dataset)

    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
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

    def convertAvroTable(self, table: AvSchema) -> AvSchema:  # type: ignore
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

    def generateTerraformGlueTable(self) -> list[str]:
        ddl: DDLTable = cast(DDLTable, self.srcTable.originalSchema)
        colList: list[str] = []

        for col in ddl.columns.values():
            colList.append(f'{col.name}:{self.convertToAWSType(col.type)}')

        return colList


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
            regions: set[InfrastructureLocation],
            stagingBucket: AmazonAWSS3Bucket,
            dataBucket: AmazonAWSS3Bucket,
            codeBucket: AmazonAWSS3Bucket,
            catalogDatabaseName: str,
            stagingIAMRole: str,
            dataIAMRole: str,
            awsGlueIAMRole: str):
        super().__init__(name, doc, executor)
        self.vpcId: str = vpcId
        self.iacCredential: Credential = iacCredential
        self.regions: set[InfrastructureLocation] = regions
        self.stagingBucket: AmazonAWSS3Bucket = stagingBucket
        self.dataBucket: AmazonAWSS3Bucket = dataBucket
        self.catalogDatabaseName: str = catalogDatabaseName
        self.stagingIAMRole: str = stagingIAMRole
        self.codeBucket: AmazonAWSS3Bucket = codeBucket
        self.dataIAMRole: str = dataIAMRole
        self.awsGlueIAMRole: str = awsGlueIAMRole

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, AWSDMSIceBergDataPlatform) and \
            self.vpcId == other.vpcId and self.iacCredential == other.iacCredential and \
            self.regions == other.regions and self.stagingBucket == other.stagingBucket and \
            self.dataBucket == other.dataBucket and self.catalogDatabaseName == other.catalogDatabaseName and \
            self.stagingIAMRole == other.stagingIAMRole and self.dataIAMRole == other.dataIAMRole and \
            self.awsGlueIAMRole == other.awsGlueIAMRole and self.codeBucket == other.codeBucket

    def _str__(self) -> str:
        return f"AWSDMSIceBergDataPlatform({self.name})"

    def getInternalDataContainers(self) -> set[DataContainer]:
        return {self.stagingBucket, self.dataBucket}

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)
        self.stagingBucket.lint(eco, tree)
        self.dataBucket.lint(eco, tree)
        self.iacCredential.lint(eco, tree)
        if (not self.stagingBucket.areAllLocationsInLocations(self.regions)):
            tree.addRaw(NameHasBadSynthax(f"Staging bucket location {self.stagingBucket} does not match region {self.regions}"))
        if (not self.dataBucket.areAllLocationsInLocations(self.regions)):
            tree.addRaw(NameHasBadSynthax(f"Data bucket location {self.dataBucket} does not match region {self.regions}"))
        if (not self.codeBucket.areAllLocationsInLocations(self.regions)):
            tree.addRaw(NameHasBadSynthax(f"Code bucket location {self.codeBucket} does not match region {self.regions}"))
        if not is_valid_aws_name(self.stagingIAMRole):
            tree.addRaw(NameHasBadSynthax(
                (f"Staging IAM role {self.stagingIAMRole} contains invalid characters."
                 f" Only alphanumeric characters and /_+=.@- are allowed.")))
        if not is_valid_aws_name(self.dataIAMRole):
            tree.addRaw(NameHasBadSynthax(
                (f"Data IAM role {self.dataIAMRole} contains invalid characters. Only "
                 f"alphanumeric characters and /_+=.@- are allowed.")))

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        return AWSDMSTerraformIaC(self.executor, graph)


def terraFormGetPipelineNodeFileName(node: PipelineNode) -> str:
    """This function returns the Terraform file name for a given pipeline node. It is used to generate Terraform files for the pipeline nodes."""
    return defaultPipelineNodeFileName(node) + ".tf"


class AWSDMSTerraformIaC(IaCDataPlatformRenderer):
    """This class renders the Terraform IaC for the AWS DMS Iceberg Data Platform. It is used to
    generate the Terraform IaC for the platform"""
    def __init__(self, executor: DataPlatformExecutor, graph: PlatformPipelineGraph):
        super().__init__(executor, graph)
        self.supportedIngestContainers: set[Type[DataContainer]] = {AWSAuroraDatabase}
        self.env: Environment = Environment(
            loader=PackageLoader('datasurface.platforms.aws.templates', 'jinja'),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, AWSDMSTerraformIaC)

    def renderIngestionSingle(self, ingestNode: IngestionSingleNode) -> str:
        return ""

    def createTemplate(self, name: str) -> Template:
        template: Template = self.env.get_template(name, None)
        return template

    def generateTableSchema(self, store: Datastore) -> dict[str, Any]:

        tables: dict[str, list[str]] = {}

        for ds in store.datasets.values():
            if ds.originalSchema is not None:
                gt: GlueTable = GlueTable(ds)
                tables[ds.name] = gt.generateTerraformGlueTable()

        return tables

    def renderIngestionMulti(self, ingestNode: IngestionMultiNode) -> str:

        awsp: AWSDMSIceBergDataPlatform = cast(AWSDMSIceBergDataPlatform, self.graph.platform)
        dc: Optional[DataContainer] = self.getDataContainerForDatastore(ingestNode.storeName)
        store: Datastore = self.graph.eco.cache_getDatastoreOrThrow(ingestNode.storeName).datastore

        if store.cmd is None:
            raise Exception(f"Datastore {ingestNode.storeName} does not have a CaptureMetadata.")
        if (not isinstance(store.cmd, IngestionMetadata)):
            raise Exception(f"Datastore {ingestNode.storeName} does not have a IngestionMetadata.")
        if dc is None:
            raise Exception(f"Datastore {ingestNode.storeName} does not have a CaptureMetadata.")
        aurora: AWSAuroraDatabase = cast(AWSAuroraDatabase, dc)

        # Nodes do not have enough information
        # Reorg templates in folders that make sense

        template: Template = self.createTemplate('aurora_dms_ingest.jinja2')

        code: str = ""
        for loc in awsp.regions:
            data: dict[str, Any] = {}
            data["platformName"] = self.graph.platform.name
            data["sourceName"] = ingestNode.storeName
            data["aws_region"] = loc.name
            data["sourceClusterName"] = aurora.endpointName
            data["glueDatabaseName"] = awsp.catalogDatabaseName
            secret: AWSSecret = cast(AWSSecret, awsp.iacCredential)
            data["sourceSecretCredentials"] = secret.secretName
            data["tables"] = self.generateTableSchema(store)

            code = code + template.render(data)
        return code

    def renderExport(self, exportNode: ExportNode) -> str:
        return ""

    def renderTrigger(self, triggerNode: TriggerNode) -> str:
        return ""

    def renderDataTransformer(self, dtNode: DataTransformerNode) -> str:
        return ""

    def checkDatastore(self, eco: Ecosystem, storeName: str, tree: ValidationTree):
        storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(storeName)
        if (storeEntry.datastore.cmd is None):
            tree.addProblem(f"Datastore {storeName} does not have a CaptureMetadata.")
        else:
            dc: Optional[DataContainer] = storeEntry.datastore.cmd.dataContainer
            if dc is not None and not self.isDataContainerSupported(dc, self.supportedIngestContainers):
                tree.addRaw(UnsupportedDataContainer(dc))

    def lintIngestionSingleNode(self, eco: Ecosystem, node: IngestionSingleNode, tree: ValidationTree) -> None:
        self.checkDatastore(eco, node.storeName, tree)

    def lintIngestionMultiNode(self, eco: Ecosystem, node: IngestionMultiNode, tree: ValidationTree) -> None:
        self.checkDatastore(eco, node.storeName, tree)

    def lintExportNode(self, eco: Ecosystem, node: ExportNode, tree: ValidationTree) -> None:
        pass

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass

    def lintTriggerNode(self, eco: Ecosystem, node: TriggerNode, tree: ValidationTree) -> None:
        pass

    def lintDataTransformerNode(self, eco: Ecosystem, node: DataTransformerNode, tree: ValidationTree) -> None:
        pass

    def getInternalDataContainers(self) -> set[DataContainer]:
        raise NotImplementedError()


class AWSDMSTerraformFileFragmentManager(FileBasedFragmentManager):
    """This adds a preRender step to the standard file frag manager. It leverages
    jinja to read the static module files and create them in the render output folder. It uses
    jinja because jinja handles all the getting the static file content regardless of how
    the python is packaged etc. It is a simple way to get the static files into the render output folder."""
    def __init__(self, platform: AWSDMSIceBergDataPlatform, render: AWSDMSTerraformIaC):
        super().__init__(platform.name, PlainTextDocumentation("FragMgr for "), terraFormGetPipelineNodeFileName)
        self.renderMgr: AWSDMSTerraformIaC = render
        self.platform: AWSDMSIceBergDataPlatform = platform

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, AWSDMSTerraformFileFragmentManager) \
            and self.renderMgr == __value.renderMgr

    def preRender(self):
        """This adds the terraform modules used by the generated IaC. It is called before the render method."""

        # Create the glue table module
        data: dict[str, Any] = {}
        data["platformName"] = self.platform.name
        gtTemplate: Template = self.renderMgr.createTemplate('glue_table_module.jinja2')
        self.addStaticFile("module/glue_table", "main.tf", gtTemplate.render(data))

        # Create the dms ingest module
        data: dict[str, Any] = {}
        data["platformName"] = self.platform.name
        ingestTemplate: Template = self.renderMgr.createTemplate('dms_ingest_module.jinja2')
        self.addStaticFile("module/dms_ingest", "main.tf", ingestTemplate.render(data))

        # Provider tf
        data = {}
#        data["aws_region"] = self.platform.region.name
        ingestTemplate: Template = self.renderMgr.createTemplate('provider.jinja2')
        self.addStaticFile("", "provider.tf", ingestTemplate.render(data))

        # main.tf
        data = {}
        data: dict[str, Any] = {}
        data["platformName"] = self.platform.name
#        data["aws_region"] = self.platform.region.name
        data["aws_vpc_id"] = self.platform.vpcId
        mainTemplate: Template = self.renderMgr.createTemplate('main.jinja2')
        self.addStaticFile("", "main.tf", mainTemplate.render(data))

        return super().preRender()

    def postRender(self):
        return super().postRender()
