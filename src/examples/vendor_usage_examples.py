"""
Usage examples for InfrastructureVendor and InfrastructureLocation showing both
the new named parameters approach and the legacy *args approach.
"""

from datasurface.md.vendor import InfrastructureVendor, InfrastructureLocation, CloudVendor
from datasurface.md.documentation import PlainTextDocumentation


def demonstrate_new_named_parameters_approach():
    """Demonstrate the new, preferred named parameters approach for better performance and clarity"""
    print("=== New Named Parameters Approach (Recommended) ===")
    
    # Create documentation
    vendor_doc = PlainTextDocumentation("Primary AWS vendor for our cloud infrastructure")
    location_doc = PlainTextDocumentation("US East region for primary workloads")
    
    # Create child locations using named parameters
    zone_a = InfrastructureLocation(
        name="us-east-1a",
        documentation=PlainTextDocumentation("Availability zone A")
    )
    zone_b = InfrastructureLocation(
        name="us-east-1b", 
        documentation=PlainTextDocumentation("Availability zone B")
    )
    
    # Create parent location with child locations
    us_east_region = InfrastructureLocation(
        name="us-east-1",
        locations=[zone_a, zone_b],
        documentation=location_doc
    )
    
    # Create vendor with all parameters named
    aws_vendor = InfrastructureVendor(
        name="aws-primary",
        locations=[us_east_region],
        cloud_vendor=CloudVendor.AWS,
        documentation=vendor_doc
    )
    
    print(f"Created vendor: {aws_vendor}")
    print(f"Vendor has {len(aws_vendor.locations)} regions")
    print(f"Region has {len(us_east_region.locations)} availability zones")
    print()


def demonstrate_legacy_args_approach():
    """Demonstrate the legacy *args approach for backward compatibility"""
    print("=== Legacy *args Approach (Backward Compatibility) ===")
    
    # Create documentation
    vendor_doc = PlainTextDocumentation("Secondary Azure vendor")
    location_doc = PlainTextDocumentation("West US region for backup workloads")
    
    # Create child locations using legacy approach
    zone_1 = InfrastructureLocation("us-west-2a", PlainTextDocumentation("Zone 1"))
    zone_2 = InfrastructureLocation("us-west-2b", PlainTextDocumentation("Zone 2"))
    
    # Create parent location with child locations (legacy style)
    us_west_region = InfrastructureLocation("us-west-2", zone_1, zone_2, location_doc)
    
    # Create vendor using legacy *args approach
    azure_vendor = InfrastructureVendor(
        "azure-secondary",         # name
        us_west_region,           # location
        CloudVendor.AZURE,        # cloud vendor
        vendor_doc                # documentation
    )
    
    print(f"Created vendor: {azure_vendor}")
    print(f"Vendor has {len(azure_vendor.locations)} regions")
    print(f"Region has {len(us_west_region.locations)} availability zones")
    print()


def demonstrate_create_legacy_factory_methods():
    """Demonstrate the create_legacy factory methods for explicit legacy usage"""
    print("=== create_legacy Factory Methods ===")
    
    # Using factory methods for explicit legacy creation
    doc = PlainTextDocumentation("Factory method example")
    
    # Create locations using factory method
    zone1 = InfrastructureLocation.create_legacy(
        "factory-zone-1",
        PlainTextDocumentation("Factory zone 1")
    )
    
    zone2 = InfrastructureLocation.create_legacy(
        "factory-zone-2", 
        PlainTextDocumentation("Factory zone 2")
    )
    
    region = InfrastructureLocation.create_legacy(
        "factory-region",
        zone1, zone2,
        PlainTextDocumentation("Factory region")
    )
    
    # Create vendor using factory method
    vendor = InfrastructureVendor.create_legacy(
        "factory-vendor",
        region,
        CloudVendor.GCP,
        doc
    )
    
    print(f"Created vendor using factory: {vendor}")
    print(f"Cloud vendor: {vendor.hardCloudVendor}")
    print()


def demonstrate_mixed_usage():
    """Demonstrate mixing both approaches (not recommended but possible)"""
    print("=== Mixed Usage (Not Recommended) ===")
    
    # Create some locations with new approach
    zone_new = InfrastructureLocation(
        name="mixed-zone-new",
        documentation=PlainTextDocumentation("New style zone")
    )
    
    # Create some locations with legacy approach  
    zone_legacy = InfrastructureLocation("mixed-zone-legacy", PlainTextDocumentation("Legacy style zone"))
    
    # Mix them in a vendor (using new approach for vendor)
    mixed_vendor = InfrastructureVendor(
        name="mixed-vendor",
        locations=[zone_new, zone_legacy],
        cloud_vendor=CloudVendor.IBM,
        documentation=PlainTextDocumentation("Mixed approach vendor")
    )
    
    print(f"Mixed vendor: {mixed_vendor}")
    print(f"Has {len(mixed_vendor.locations)} zones created with different approaches")
    print()


if __name__ == "__main__":
    print("InfrastructureVendor and InfrastructureLocation Usage Examples\n")
    
    demonstrate_new_named_parameters_approach()
    demonstrate_legacy_args_approach()
    demonstrate_create_legacy_factory_methods()
    demonstrate_mixed_usage()
    
    print("Recommendation: Use the named parameters approach for new code!")
    print("It provides better performance, type safety, and code clarity.") 