from .Governance import Credential, EncryptionSystem, Ecosystem, GovernanceZone, Team
from .Lint import ValidationTree
from .utils import is_valid_azure_key_vault_name


class AzureKeyVaultCredential(Credential):
    """This allows a secret to be read from Azure Key Vault. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, keyVaultName : str, secretName : str) -> None:
        super().__init__()
        self.keyVaultName : str = keyVaultName
        self.secretName : str = secretName

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is AzureKeyVaultCredential and self.keyVaultName == __value.keyVaultName and self.secretName == __value.secretName
    
    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', tree : ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        super().lint(eco, gz, t, tree)
        if(not is_valid_azure_key_vault_name(self.keyVaultName)):
            tree.addProblem("Azure Key Vault name is invalid")

    def __str__(self) -> str:
        return f"AzureKeyVaultCredential({self.keyVaultName})"


class AzureKeyVault(EncryptionSystem):
    pass
