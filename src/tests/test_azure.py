import unittest
from unittest.mock import patch
from datasurface.platforms.azure.azure import AzureKeyVault, AzureVaultObjectType
from datasurface.md import LocationKey, ValidationTree, Ecosystem, GitHubRepository


def always_true(name: str) -> bool:
    return True


class TestAzureKeyVault(unittest.TestCase):
    def setUp(self):
        self.locs: set[LocationKey] = set()

    def test_equality(self):
        # Create two objects with same properties.
        akv1 = AzureKeyVault("store1", self.locs, "vault123", AzureVaultObjectType.SECRETS)
        akv2 = AzureKeyVault("store1", self.locs, "vault123", AzureVaultObjectType.SECRETS)
        self.assertEqual(akv1, akv2)

        # Different vaultName
        akv3 = AzureKeyVault("store1", self.locs, "vaultABC", AzureVaultObjectType.SECRETS)
        self.assertNotEqual(akv1, akv3)

        # Different type
        akv4 = AzureKeyVault("store1", self.locs, "vault123", AzureVaultObjectType.HSM_KEYS)
        self.assertNotEqual(akv1, akv4)

    def test_str(self):
        akv = AzureKeyVault("storeX", self.locs, "myvault", AzureVaultObjectType.SECRETS)
        expected = f"AzureKeyVault(storeX:myvault:{AzureVaultObjectType.SECRETS})"
        self.assertEqual(str(akv), expected)

    def test_hash(self):
        akv = AzureKeyVault("storeY", self.locs, "vault456", AzureVaultObjectType.SECRETS)
        # __hash__ is defined as hash(self.name)
        self.assertEqual(hash(akv), hash("storeY"))

    def test_lint_valid(self):
        # Patch to simulate a valid vault name by always returning True.
        with patch('datasurface.platforms.azure.azure.is_valid_azure_key_vault_name', always_true):
            akv = AzureKeyVault("storeValid", self.locs, "validvault", AzureVaultObjectType.SECRETS)
            tree = ValidationTree(akv)
            eco: Ecosystem = Ecosystem("eco", GitHubRepository("repo", "Branch"))
            akv.lint(eco, tree)
            # No error should be added if name is valid.
            self.assertFalse(tree.hasErrors())


if __name__ == '__main__':
    unittest.main()
