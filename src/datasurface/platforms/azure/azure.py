"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from enum import Enum
from datasurface.md import Documentation, CredentialStore
from datasurface.md import DataContainer, DataContainerNamingMapper, Dataset, DatasetGroup, Datastore, Workspace
from datasurface.md import CaseSensitiveEnum, CloudVendor, Credential, DataPlatform, DataPlatformExecutor, Ecosystem, \
    HostPortSQLDatabase, IaCDataPlatformRendererShim, InfrastructureLocation, PlatformPipelineGraph, \
    DataPlatformGraphHandler
from datasurface.md import NameHasBadSynthax, ValidationTree, HostPortPair
from datasurface.md import is_valid_azure_key_vault_name


class AzureVaultObjectType(Enum):
    HSM_KEYS = 1
    SOFTWARE_KEYS = 2
    SECRETS = 3
    CERTIFICATES = 4
    STORAGE_ACCOUNT_KEYS = 5


class AzureKeyVault(CredentialStore):
    """This is a credential store for Azure Key Vault. It is a simple wrapper around the
    AzureKeyVaultCredential class"""
    def __init__(self, name: str, locs: set[InfrastructureLocation], vaultName: str, type: AzureVaultObjectType) -> None:
        super().__init__(name, locs)
        self.vaultName: str = vaultName
        self.type: AzureVaultObjectType = type

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and \
            isinstance(other, AzureKeyVault) and \
            self.vaultName == other.vaultName and \
            self.type == other.type

    def __str__(self) -> str:
        return f"AzureKeyVault({self.name}:{self.vaultName}:{self.type})"

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        if (not is_valid_azure_key_vault_name(self.vaultName)):
            tree.addRaw(NameHasBadSynthax(f"Azure Key Vault name <{self.vaultName}> needs to match [a-z0-9]{3, 24}"))

    def getCredential(self, credName: str) -> Credential:
        return AzureKeyVaultCredential(self, credName)


class AzureKeyVaultCredential(Credential):
    """This allows a secret to be read from Azure Key Vault. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, vault: AzureKeyVault, objectName: str) -> None:
        super().__init__()
        self.keyVault: AzureKeyVault = vault
        self.objectName: str = objectName

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, AzureKeyVaultCredential) and self.keyVault == other.keyVault and \
            self.objectName == other.objectName

    def __hash__(self) -> int:
        return hash(self.keyVault) + hash(self.objectName)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)

    def __str__(self) -> str:
        return f"AzureKeyVaultCredential({self.keyVault.name}:{self.objectName})"


class AzureDataplatform(DataPlatform):
    """This platform manages pipelines for resources within Azure"""
    def __init__(self, name: str, doc: Documentation, executor: DataPlatformExecutor, platformCredential: Credential):
        super().__init__(name, doc, executor)
        self.platformCredential: Credential = platformCredential

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        cTree = tree.addSubTree(self)
        self.platformCredential.lint(eco, cTree)

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        rc: set[CloudVendor] = set()
        rc.add(CloudVendor.AZURE)
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, AzureDataplatform)

    def _str__(self) -> str:
        return f"AzureDataPlatform({self.name})"

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return dc.areLocationsOwnedByTheseVendors(eco, {CloudVendor.AZURE})

    def getInternalDataContainers(self) -> set[DataContainer]:
        # TODO: Implement this method
        return set()

    def createGraphHandler(self, graph: 'PlatformPipelineGraph') -> 'DataPlatformGraphHandler':
        return IaCDataPlatformRendererShim(self.executor, graph)


class AzureBatchDataPlatform(AzureDataplatform):
    def __init__(self, name: str, doc: Documentation, executor: DataPlatformExecutor, platformCredential: Credential):
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
    def __init__(self, name: str, hostPort: HostPortPair, databaseName: str, locs: set[InfrastructureLocation]):
        super().__init__(name, locs, hostPort, databaseName)

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
