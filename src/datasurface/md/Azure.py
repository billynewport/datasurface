from enum import Enum
from datasurface.md import Documentation
from datasurface.md.Governance import DataContainer, InfrastructureVendor
from .Governance import CloudVendor, Credential, DataPlatform, EncryptionSystem, Ecosystem, GovernanceZone, HostPortSQLDatabase, InfrastructureLocation, Team
from .Lint import ValidationTree
from .utils import is_valid_azure_key_vault_name


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
            tree.addProblem(f"Azure Key Vault name <{self.keyVaultName}> is invalid")

    def __str__(self) -> str:
        return f"AzureKeyVaultCredential({self.keyVaultName}/{self.objectName})"

    def getURL(self) -> str:
        return f"https://{self.keyVaultName}.vault.azure.net/{self.objectType}/{self.objectName}"


class AzureKeyVault(EncryptionSystem):
    pass


class AzureDataplatform(DataPlatform):
    """This platform manages pipelines for resources within Azure"""
    def __init__(self, name: str, doc: Documentation, platformCredential: AzureKeyVaultCredential):
        super().__init__(name, doc)
        self.platformCredential = platformCredential

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        cTree = tree.createChild(self)
        self.platformCredential.lint(eco, cTree)

    def getSupportedVendors(self, eco: Ecosystem) -> set[InfrastructureVendor]:
        rc: set[InfrastructureVendor] = set()
        iv: InfrastructureVendor = eco.getVendorOrThrow("Azure")
        rc.add(iv)
        return rc

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, AzureDataplatform)

    def _str__(self) -> str:
        return f"AzureDataPlatform({self.name})"

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return dc.isUsingVendorsOnly(eco, {CloudVendor.AZURE})

    def getInternalDataContainers(self) -> set[DataContainer]:
        # TODO: Implement this method
        return set()


class AzureBatchDataPlatform(AzureDataplatform):
    def __init__(self, name: str, doc: Documentation, platformCredential: AzureKeyVaultCredential):
        super().__init__(name, doc, platformCredential)


class AzureSQLDatabase(HostPortSQLDatabase):
    """This is an Azure SQL Database resource. """
    def __init__(self, name: str, hostName: str, port: int, databaseName: str, loc: InfrastructureLocation):
        super().__init__(name, loc, hostName, port, databaseName)

    def __str__(self) -> str:
        return f"AzureDatabaseResource({self.name})"

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: Ecosystem, gz: GovernanceZone, t: Team, tree: ValidationTree) -> None:
        super().lint(eco, gz, t, tree)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AzureSQLDatabase)
