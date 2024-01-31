from .Governance import Credential, DataPlatform, EncryptionSystem, Ecosystem
from .Lint import ValidationTree
from .utils import is_valid_azure_key_vault_name


    
class AzureKeyVaultCredential(Credential):
    """This allows a secret to be read from Azure Key Vault. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, keyVaultName : str, objectName : str) -> None:
        super().__init__()
        self.keyVaultName : str = keyVaultName
        self.objectName : str = objectName
        self.objectType : str = "secrets"

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is AzureKeyVaultCredential and self.keyVaultName == __value.keyVaultName and self.objectName == __value.objectName
    
    def lint(self, eco : 'Ecosystem', tree : ValidationTree) -> None:
        super().lint(eco, tree)
        if(not is_valid_azure_key_vault_name(self.keyVaultName)):
            tree.addProblem(f"Azure Key Vault name <{self.keyVaultName}> is invalid")

    def __str__(self) -> str:
        return f"AzureKeyVaultCredential({self.keyVaultName}/{self.objectName})"
    
    def getURL(self) -> str:
        return f"https://{self.keyVaultName}.vault.azure.net/{self.objectType}/{self.objectName}"


class AzureKeyVault(EncryptionSystem):
    pass

class AzureDataplatform(DataPlatform):
    """This platform manages pipelines for resources within Azure"""
    def __init__(self, name : str, platformCredential : AzureKeyVaultCredential):
        self.platformCredential = platformCredential

    def lint(self, eco : 'Ecosystem', tree : ValidationTree):
        cTree = tree.createChild(self)
        self.platformCredential.lint(eco, cTree)

class AzureDataPlatform(DataPlatform):
    def __init__(self):
        pass