"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.md import StorageRequirement, ValidationTree
from datasurface.platforms.yellow.yellow_dp import Component


class TestStorageRequirement(unittest.TestCase):

    def test_kubernetes_format_binary_units(self):
        """Test Kubernetes format conversion for binary units (get 'i' suffix)"""
        binary_units = ['G', 'T', 'P', 'E', 'Z', 'Y']
        for unit in binary_units:
            with self.subTest(unit=unit):
                self.assertEqual(Component.storageToKubernetesFormat(StorageRequirement(f"5{unit}")), f"5{unit}i")
                self.assertEqual(Component.storageToKubernetesFormat(StorageRequirement(f"5{unit.lower()}")), f"5{unit.lower()}i")

    def test_kubernetes_format_non_binary_units(self):
        """Test Kubernetes format conversion for non-binary units (no 'i' suffix)"""
        non_binary_units = ['M', 'K', 'B', 'R']
        for unit in non_binary_units:
            with self.subTest(unit=unit):
                self.assertEqual(Component.storageToKubernetesFormat(StorageRequirement(f"5{unit}")), f"5{unit}")
                self.assertEqual(Component.storageToKubernetesFormat(StorageRequirement(f"5{unit.lower()}")), f"5{unit.lower()}")

    def test_valid_formats(self):
        """Test various valid storage requirement formats"""
        valid_specs = [
            "1G", "10G", "100G", "1000G",
            "1T", "2P",
            "512M", "1024K", "8B",
            "1g", "2t", "3p",
            "0G", "999999G"
        ]

        for spec in valid_specs:
            with self.subTest(spec=spec):
                storage = StorageRequirement(spec)
                tree = ValidationTree(storage)
                storage.lint(tree)
                self.assertFalse(tree.hasErrors(), f"Valid spec '{spec}' should not have errors")

    def test_getSizeInBytes(self):
        """Test the getSizeInBytes method"""
        self.assertEqual(StorageRequirement("1G").getSizeInBytes(), 1024 * 1024 * 1024)
        self.assertEqual(StorageRequirement("1T").getSizeInBytes(), 1024 * 1024 * 1024 * 1024)
        self.assertEqual(StorageRequirement("1P").getSizeInBytes(), 1024 * 1024 * 1024 * 1024 * 1024)
        self.assertEqual(StorageRequirement("1M").getSizeInBytes(), 1024 * 1024)
        self.assertEqual(StorageRequirement("1K").getSizeInBytes(), 1024)

    def test_gt(self):
        """Test the gt method"""
        self.assertTrue(StorageRequirement("1G") > StorageRequirement("100M"))
        self.assertTrue(StorageRequirement("1T") > StorageRequirement("1G"))
        self.assertTrue(StorageRequirement("1P") > StorageRequirement("1T"))
        self.assertTrue(StorageRequirement("1M") > StorageRequirement("1K"))
        self.assertTrue(StorageRequirement("1K") > StorageRequirement("1B"))
        self.assertTrue(StorageRequirement("1025G") > StorageRequirement("1T"))
        self.assertTrue(StorageRequirement("1025T") > StorageRequirement("1P"))

    def test_invalid_formats(self):
        """Test various invalid storage requirement formats"""
        invalid_specs = [
            "",  # Empty string
            "G",  # No number
            "5",  # No unit
            "5GB",  # Multiple characters
            "5.5G",  # Decimal number
            "G5",  # Unit before number
            "5X",  # Invalid unit
            "-5G",  # Negative number
            "5 G",  # Space in between
            "five G",  # Non-numeric
            "5Gi",  # Kubernetes format not allowed in spec
        ]

        for spec in invalid_specs:
            with self.subTest(spec=spec):
                storage = StorageRequirement(spec)
                tree = ValidationTree(storage)
                storage.lint(tree)
                self.assertTrue(tree.hasErrors(), f"Invalid spec '{spec}' should have errors")

    def test_equality(self):
        """Test StorageRequirement equality"""
        storage1 = StorageRequirement("5G")
        storage2 = StorageRequirement("5G")
        storage3 = StorageRequirement("10G")

        self.assertEqual(storage1, storage2)
        self.assertNotEqual(storage1, storage3)
        self.assertNotEqual(storage1, "5G")  # Different type

    def test_hash(self):
        """Test StorageRequirement hashing"""
        storage1 = StorageRequirement("5G")
        storage2 = StorageRequirement("5G")
        storage3 = StorageRequirement("10G")

        self.assertEqual(hash(storage1), hash(storage2))
        self.assertNotEqual(hash(storage1), hash(storage3))

    def test_string_representation(self):
        """Test string representation"""
        storage = StorageRequirement("5G")
        self.assertEqual(str(storage), "StorageRequirement(5G)")

    def test_json_serialization(self):
        """Test JSON serialization"""
        storage = StorageRequirement("5G")
        json_data = storage.to_json()

        self.assertIsInstance(json_data, dict)
        self.assertEqual(json_data["spec"], "5G")
        self.assertEqual(json_data["_type"], "StorageRequirement")

    def test_edge_cases(self):
        """Test edge cases"""
        # Test very large numbers
        large_storage = StorageRequirement("999999999G")
        tree = ValidationTree(large_storage)
        large_storage.lint(tree)
        self.assertFalse(tree.hasErrors())

        # Test zero values
        zero_storage = StorageRequirement("0G")
        tree = ValidationTree(zero_storage)
        zero_storage.lint(tree)
        self.assertFalse(tree.hasErrors())
