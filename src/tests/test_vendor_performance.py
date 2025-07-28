"""
Performance test comparing old *args approach vs new named parameters approach for InfrastructureVendor and InfrastructureLocation
"""
import time
import unittest
from typing import List
from datasurface.md.vendor import InfrastructureVendor, InfrastructureLocation, CloudVendor
from datasurface.md.documentation import PlainTextDocumentation


class TestVendorPerformance(unittest.TestCase):

    def test_infrastructure_vendor_performance_comparison(self):
        """Test performance difference between old *args and new named parameters for InfrastructureVendor"""

        # Test data
        count = 1000
        doc = PlainTextDocumentation("Test vendor")
        cloud_vendor = CloudVendor.AWS

        # Create some sample locations
        loc1 = InfrastructureLocation(name="us-east-1", documentation=PlainTextDocumentation("US East"))
        loc2 = InfrastructureLocation(name="us-west-2", documentation=PlainTextDocumentation("US West"))
        locations = [loc1, loc2]

        print(f"Creating {count} InfrastructureVendor objects...")

        # Test new named parameters approach (should be faster)
        start_time = time.perf_counter()
        new_vendors: List[InfrastructureVendor] = []
        for i in range(count):
            vendor = InfrastructureVendor(
                name=f"vendor_{i}",
                locations=locations,
                documentation=doc,
                cloud_vendor=cloud_vendor
            )
            new_vendors.append(vendor)
        new_approach_time = time.perf_counter() - start_time

        # Test legacy *args approach (should be slower)
        start_time = time.perf_counter()
        legacy_vendors: List[InfrastructureVendor] = []
        for i in range(count):
            vendor = InfrastructureVendor(
                f"legacy_vendor_{i}",
                loc1, loc2,  # locations
                cloud_vendor,  # cloud vendor
                doc  # documentation
            )
            legacy_vendors.append(vendor)
        legacy_approach_time = time.perf_counter() - start_time

        print("\nInfrastructureVendor Performance Results:")
        print(f"  Named parameters:  {new_approach_time:.4f}s")
        print(f"  Legacy *args:      {legacy_approach_time:.4f}s")
        print(f"  Speedup factor:    {legacy_approach_time / new_approach_time:.2f}x")

        # Verify both approaches create equivalent objects (comparing first ones)
        self.assertEqual(new_vendors[0].name, "vendor_0")
        self.assertEqual(legacy_vendors[0].name, "legacy_vendor_0")
        self.assertEqual(len(new_vendors[0].locations), 2)
        self.assertEqual(len(legacy_vendors[0].locations), 2)
        self.assertEqual(new_vendors[0].hardCloudVendor, CloudVendor.AWS)
        self.assertEqual(legacy_vendors[0].hardCloudVendor, CloudVendor.AWS)

    def test_infrastructure_location_performance_comparison(self):
        """Test performance difference between old *args and new named parameters for InfrastructureLocation"""

        # Test data
        count = 1000
        doc = PlainTextDocumentation("Test location")

        # Create some child locations
        child1 = InfrastructureLocation(name="zone-a", documentation=PlainTextDocumentation("Zone A"))
        child2 = InfrastructureLocation(name="zone-b", documentation=PlainTextDocumentation("Zone B"))
        child_locations = [child1, child2]

        print(f"\nCreating {count} InfrastructureLocation objects...")

        # Test new named parameters approach (should be faster)
        start_time = time.perf_counter()
        new_locations: List[InfrastructureLocation] = []
        for i in range(count):
            location = InfrastructureLocation(
                name=f"location_{i}",
                locations=child_locations,
                documentation=doc
            )
            new_locations.append(location)
        new_approach_time = time.perf_counter() - start_time

        # Test legacy *args approach (should be slower)
        start_time = time.perf_counter()
        legacy_locations: List[InfrastructureLocation] = []
        for i in range(count):
            location = InfrastructureLocation(
                f"legacy_location_{i}",
                child1, child2,  # child locations
                doc  # documentation
            )
            legacy_locations.append(location)
        legacy_approach_time = time.perf_counter() - start_time

        print("\nInfrastructureLocation Performance Results:")
        print(f"  Named parameters:  {new_approach_time:.4f}s")
        print(f"  Legacy *args:      {legacy_approach_time:.4f}s")
        print(f"  Speedup factor:    {legacy_approach_time / new_approach_time:.2f}x")

        # Verify both approaches create equivalent objects (comparing first ones)
        self.assertEqual(new_locations[0].name, "location_0")
        self.assertEqual(legacy_locations[0].name, "legacy_location_0")
        self.assertEqual(len(new_locations[0].locations), 2)
        self.assertEqual(len(legacy_locations[0].locations), 2)

    def test_create_legacy_factory_methods(self):
        """Test the create_legacy factory methods work correctly"""

        # Test InfrastructureVendor.create_legacy
        doc = PlainTextDocumentation("Legacy vendor")
        loc1 = InfrastructureLocation(name="region-1")
        loc2 = InfrastructureLocation(name="region-2")

        vendor = InfrastructureVendor.create_legacy(
            "test-vendor",
            loc1, loc2,
            CloudVendor.GCP,
            doc
        )

        self.assertEqual(vendor.name, "test-vendor")
        self.assertEqual(len(vendor.locations), 2)
        self.assertEqual(vendor.hardCloudVendor, CloudVendor.GCP)
        self.assertEqual(vendor.documentation, doc)

        # Test InfrastructureLocation.create_legacy
        child1 = InfrastructureLocation(name="zone-1")
        child2 = InfrastructureLocation(name="zone-2")
        location_doc = PlainTextDocumentation("Legacy location")

        location = InfrastructureLocation.create_legacy(
            "test-location",
            child1, child2,
            location_doc
        )

        self.assertEqual(location.name, "test-location")
        self.assertEqual(len(location.locations), 2)
        self.assertEqual(location.documentation, location_doc)


if __name__ == '__main__':
    unittest.main() 