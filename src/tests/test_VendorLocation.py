"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


from typing import Optional
import unittest

from datasurface.md import InfrastructureLocation, InfrastructureVendor, LocationKey, Ecosystem, ValidationTree, CloudVendor, AttributeNotSet
from datasurface.md import UnknownVendorProblem, UnknownLocationProblem, InvalidLocationStringProblem
import tests.nwdb.eco


class Test_VendorLocation(unittest.TestCase):
    def test_VendorLocation(self):
        vendor: InfrastructureVendor = InfrastructureVendor(
            "AWS",
            InfrastructureLocation(
                "USA",
                InfrastructureLocation("us-east-1"),
                InfrastructureLocation("us-west-1")
                ),
            InfrastructureLocation(
                "EU",
                InfrastructureLocation("eu-west-1"),
                InfrastructureLocation("eu-west-2"))
            )

        east: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(["USA", "us-east-1"])
        self.assertIsNotNone(east)

        west: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(["USA", "us-west-1"])
        self.assertIsNotNone(west)

        unknown: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(["USA", "us-west-2"])
        self.assertIsNone(unknown)

        usa: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(["USA"])
        self.assertIsNotNone(usa)

        eu: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(["EU"])
        self.assertIsNotNone(eu)

        if (eu is not None and usa is not None and east is not None and west is not None):
            self.assertTrue(usa.containsLocation(east))
            self.assertTrue(usa.containsLocation(west))
            self.assertTrue(usa.containsLocation(usa))
            self.assertTrue(eu.containsLocation(eu))
            self.assertFalse(usa.containsLocation(eu))
            self.assertFalse(eu.containsLocation(usa))

    def test_lint_methods(self):
        # Test InfrastructureLocation.lint() with no key
        loc = InfrastructureLocation("loc1")
        tree = ValidationTree(loc)
        loc.lint(tree)
        self.assertTrue(tree.hasErrors())
        self.assertTrue(tree.containsProblemType(AttributeNotSet))

        # Test InfrastructureVendor.lint() with no name
        vendor = InfrastructureVendor("")  # Empty name
        tree = ValidationTree(vendor)
        vendor.lint(tree)
        self.assertTrue(tree.hasErrors())
        self.assertTrue(tree.containsProblemType(AttributeNotSet))

    def test_InfrastructureVendor_more(self):
        # Test legacy constructor
        vendor_legacy = InfrastructureVendor.create_legacy(
            "AWS_Legacy",
            InfrastructureLocation("us-east-1"),
            CloudVendor.AWS
        )
        self.assertIsNotNone(vendor_legacy.findLocationUsingKey(["us-east-1"]))
        self.assertEqual(vendor_legacy.hardCloudVendor, CloudVendor.AWS)

        # Test setting cloud vendor
        vendor = InfrastructureVendor("MyCloud", cloud_vendor=CloudVendor.PRIVATE)
        self.assertEqual(vendor.hardCloudVendor, CloudVendor.PRIVATE)

    def test_InfrastructureLocation_more(self):
        loc_child = InfrastructureLocation("child")
        loc_parent = InfrastructureLocation("parent", loc_child)

        # Test duplicate location
        with self.assertRaises(Exception):
            loc_parent.addLocation(InfrastructureLocation("child"))

        # Test getLocationOrThrow
        with self.assertRaises(AssertionError):
            loc_parent.getLocationOrThrow("non-existent")

        # Test getEveryChildLocation
        loc_grandchild = InfrastructureLocation("grandchild")
        loc_child.addLocation(loc_grandchild)
        all_children = loc_parent.getEveryChildLocation()
        self.assertEqual(all_children, {loc_child, loc_grandchild})

        # Test legacy constructor
        loc_legacy = InfrastructureLocation.create_legacy("parent", InfrastructureLocation("child1"), InfrastructureLocation("child2"))
        self.assertIsNotNone(loc_legacy.getLocation("child1"))
        self.assertIsNotNone(loc_legacy.getLocation("child2"))

    def test_LocationKey(self):
        eco: Ecosystem = tests.nwdb.eco.createEcosystem()

        key: LocationKey = LocationKey("MyCorp:USA/NJ_1")
        tree: ValidationTree = ValidationTree(key)

        key.lint(tree)
        self.assertFalse(tree.hasErrors())
        self.assertEqual(eco.getAsInfraLocation(key), eco.getLocationOrThrow("MyCorp", ["USA", "NJ_1"]))
        self.assertNotEqual(eco.getAsInfraLocation(key), eco.getLocationOrThrow("MyCorp", ["USA", "NY_1"]))

        key = LocationKey("MyCorp:USA/NY_1")
        tree = ValidationTree(key)
        key.lint(tree)
        self.assertFalse(tree.hasErrors())
        self.assertEqual(eco.getAsInfraLocation(key), eco.getLocationOrThrow("MyCorp", ["USA", "NY_1"]))
        self.assertNotEqual(eco.getAsInfraLocation(key), eco.getLocationOrThrow("MyCorp", ["USA", "NJ_1"]))

        key = LocationKey("MyCorp:USA")
        tree = ValidationTree(key)
        key.lint(tree)
        self.assertFalse(tree.hasErrors())
        self.assertEqual(eco.getAsInfraLocation(key), eco.getLocationOrThrow("MyCorp", ["USA"]))
        self.assertNotEqual(eco.getAsInfraLocation(key), eco.getLocationOrThrow("MyCorp", ["USA", "NJ_1"]))

        # Check unknown vendor fails
        key = LocationKey("Unknown:USA")
        tree = ValidationTree(key)
        eco.lintLocationKey(key, tree)
        self.assertTrue(tree.hasErrors())
        self.assertEqual(eco.getAsInfraLocation(key), None)
        self.assertTrue(tree.containsProblemType(UnknownVendorProblem))

        # Check unknown location fails
        key = LocationKey("MyCorp:Unknown")
        tree = ValidationTree(key)
        eco.lintLocationKey(key, tree)
        self.assertTrue(tree.hasErrors())
        self.assertEqual(eco.getAsInfraLocation(key), None)
        self.assertTrue(tree.containsProblemType(UnknownLocationProblem))

        # Check bad syntax fails, leave out a colon
        key = LocationKey("USA/NY_1")
        tree = ValidationTree(key)
        eco.lintLocationKey(key, tree)
        self.assertTrue(tree.hasErrors())
        self.assertTrue(tree.containsProblemType(InvalidLocationStringProblem))

    def test_LocationKey_lint_failures(self):
        # Test multiple colons
        key: LocationKey = LocationKey("MyCorp:USA:NJ_1")
        tree: ValidationTree = ValidationTree(key)
        key.lint(tree)
        self.assertTrue(tree.hasErrors())
        self.assertTrue(tree.containsProblemType(InvalidLocationStringProblem))

        # Test empty vendor
        key = LocationKey(":USA/NJ_1")
        tree = ValidationTree(key)
        key.lint(tree)
        self.assertTrue(tree.hasErrors())
        self.assertTrue(tree.containsProblemType(InvalidLocationStringProblem))

        # Test no location parts
        key = LocationKey("MyCorp:")
        tree = ValidationTree(key)
        key.lint(tree)
        self.assertTrue(tree.hasErrors())
        self.assertTrue(tree.containsProblemType(InvalidLocationStringProblem))

        # Test empty location part in the middle
        key = LocationKey("MyCorp:USA//NJ_1")
        tree = ValidationTree(key)
        key.lint(tree)
        self.assertTrue(tree.hasErrors())
        self.assertTrue(tree.containsProblemType(InvalidLocationStringProblem))

        # Test location starts with /
        key = LocationKey("MyCorp:/USA")
        tree = ValidationTree(key)
        key.lint(tree)
        self.assertTrue(tree.hasErrors())
        self.assertTrue(tree.containsProblemType(InvalidLocationStringProblem))
