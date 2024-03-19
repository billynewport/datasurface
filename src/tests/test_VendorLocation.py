from typing import Optional
import unittest

from datasurface.md.Governance import InfrastructureLocation, InfrastructureVendor


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
