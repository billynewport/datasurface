from datasurface.md.Governance import Credential, EncryptionSystem

class AzureKeyVaultCredential(Credential):
    """This allows a secret to be read from Azure Key Vault. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, keyVaultName : str, secretName : str) -> None:
        super().__init__()
        self.keyVaultName : str = keyVaultName
        self.secretName : str = secretName

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is AzureKeyVaultCredential and self.keyVaultName == __value.keyVaultName and self.secretName == __value.secretName

class AzureKeyVault(EncryptionSystem):
