"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from enum import Enum
from datasurface.md import Documentation
from datasurface.md.Governance import DataContainer, DataContainerNamingMapper, Dataset, DatasetGroup, Datastore, Workspace
from ...md.Governance import CaseSensitiveEnum, CloudVendor, Credential, DataPlatform, DataPlatformExecutor, EncryptionSystem, Ecosystem, \
    HostPortSQLDatabase, IaCDataPlatformRenderer, IaCDataPlatformRendererShim, InfrastructureLocation, PlatformPipelineGraph
from ...md.Lint import NameHasBadSynthax, ValidationTree
from ...md.utils import is_valid_azure_key_vault_name


class AzureVaultObjectType(Enum):
    HSM_KEYS = 1
    SOFTWARE_KEYS = 2
    SECRETS = 3
    CERTIFICATES = 4
    STORAGE_ACCOUNT_KEYS = 5


class AzureKeyVaultCredential(Credential):
    """This allows a secret to be read from Azure Key Vault. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, keyVaultName: str, objectName: str) -> None:
        super().__init__()
        self.keyVaultName: str = keyVaultName
        self.objectName: str = objectName
        self.objectType: AzureVaultObjectType = AzureVaultObjectType.SECRETS

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is AzureKeyVaultCredential and self.keyVaultName == __value.keyVaultName and \
            self.objectName == __value.objectName

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        if (not is_valid_azure_key_vault_name(self.keyVaultName)):
            tree.addRaw(NameHasBadSynthax(f"Azure Key Vault name <{self.keyVaultName}> needs to match [a-z0-9]{3,24}"))

    def __str__(self) -> str:
        return f"AzureKeyVaultCredential({self.keyVaultName}/{self.objectName})"

    def getURL(self) -> str:
        return f"https://{self.keyVaultName}.vault.azure.net/{self.objectType}/{self.objectName}"


class AzureKeyVault(EncryptionSystem):
    pass


class AzureDataplatform(DataPlatform):
    """This platform manages pipelines for resources within Azure"""
    def __init__(self, name: str, doc: Documentation, executor: DataPlatformExecutor, platformCredential: AzureKeyVaultCredential):
        super().__init__(name, doc, executor)
        self.platformCredential = platformCredential

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        cTree = tree.addSubTree(self)
        self.platformCredential.lint(eco, cTree)

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        rc: set[CloudVendor] = set()
        rc.add(CloudVendor.AZURE)
        return rc

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, AzureDataplatform)

    def _str__(self) -> str:
        return f"AzureDataPlatform({self.name})"

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return dc.areLocationsOwnedByTheseVendors(eco, {CloudVendor.AZURE})

    def getInternalDataContainers(self) -> set[DataContainer]:
        # TODO: Implement this method
        return set()

    def createIaCRender(self, graph: 'PlatformPipelineGraph') -> 'IaCDataPlatformRenderer':
        return IaCDataPlatformRendererShim(self.executor, graph)


class AzureBatchDataPlatform(AzureDataplatform):
    def __init__(self, name: str, doc: Documentation, executor: DataPlatformExecutor, platformCredential: AzureKeyVaultCredential):
        super().__init__(name, doc, executor, platformCredential)


# SQL Server Naming Rules
# If an identifier contains a space or a special symbol, the identifier must be enclosed in back quotes.
# A valid name is a string of no more than 128 characters, of which the first character must not be a space.
# Valid names can't include control characters or the following special
# characters: `, |, #, *, ?, [, ], ., !, or $.
# Don't use the reserved words listed in the SQL grammar in Appendix C of the ODBC
# Programmer's Reference (or the shorthand form of these reserved words) as identifiers
# (that is, table or column names), unless you surround the word in back quotes (`).


class SQLServerNamingMapper(DataContainerNamingMapper):
    """This is a naming adapter for SQL Server. It truncates names to 128 characters and
    encloses them in back quotes in case they contain spaces or special symbols. Enclosing them
    in quotes also allows for reserved words to be used as identifiers."""

    def __init__(self):
        super().__init__(128, CaseSensitiveEnum.CASE_INSENSITIVE, "`")

    def mapRawDatasetName(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        return super().mapRawDatasetName(w, dsg, store, ds)

    def mapRawDatasetView(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        return super().mapRawDatasetView(w, dsg, store, ds)

    def mapAttributeName(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset', attributeName: str) -> str:
        return super().mapAttributeName(w, dsg, store, ds, attributeName)


class AzureSQLDatabase(HostPortSQLDatabase):
    """This is an Azure SQL Database resource. """
    def __init__(self, name: str, hostName: str, port: int, databaseName: str, loc: InfrastructureLocation):
        super().__init__(name, loc, hostName, port, databaseName)

    def __str__(self) -> str:
        return f"AzureDatabaseResource({self.name})"

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: Ecosystem, tree: ValidationTree) -> None:
        super().lint(eco, tree)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AzureSQLDatabase)

    def getNamingAdapter(self) -> DataContainerNamingMapper:
        return SQLServerNamingMapper()
