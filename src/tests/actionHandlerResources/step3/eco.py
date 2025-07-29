"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md import LocationKey, VendorKey
from datasurface.md.repo import GitHubRepository
from datasurface.md import CloudVendor, DataPlatformPolicy, \
    DefaultDataPlatform, Ecosystem, GovernanceZone, GovernanceZoneDeclaration, DataPlatformKey, \
    InfraStructureLocationPolicy, InfraStructureVendorPolicy, InfrastructureLocation, InfrastructureVendor, TeamDeclaration
from tests.actionHandlerResources.step3.defineEU_GZ import defineEU_GZ
from datasurface.platforms.legacy import LegacyDataPlatform

# Base branch for step 1, define an Ecosystem, data platforms, infrastructure vendors/locations and 3 Governance Zones


def createEcosystem() -> Ecosystem:
    e: Ecosystem = Ecosystem(
        "Test", GitHubRepository("billynewport/test_step1", "main"),
        LegacyDataPlatform(
            "Azure Platform",
            PlainTextDocumentation("Test")),
        DefaultDataPlatform(DataPlatformKey("Azure Platform")),
        LegacyDataPlatform("AWS Platform", PlainTextDocumentation("Test")),

        GovernanceZoneDeclaration("USA", GitHubRepository("billynewport/test_step1", "USAmain")),
        GovernanceZoneDeclaration("EU", GitHubRepository("billynewport/test_step1", "EUmain")),
        GovernanceZoneDeclaration("UK", GitHubRepository("billynewport/test_step1", "UKmain")),

        # Infra Vendors and locations
        InfrastructureVendor(
            "AWS",
            CloudVendor.AWS,
            PlainTextDocumentation("Amazon AWS"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation("us-east-1"),  # Virginia
                InfrastructureLocation("us-west-1")),
            InfrastructureLocation(
                "UK",
                InfrastructureLocation("eu-west-1"),  # Ireland
                InfrastructureLocation("eu-west-2")),  # London
            InfrastructureLocation(
                "EU",
                InfrastructureLocation("eu-central-1"),  # Frankfurt
                InfrastructureLocation("eu-west-3"))),

        InfrastructureVendor(
            "Azure",
            CloudVendor.AZURE,
            PlainTextDocumentation("Microsoft Azure"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation(
                    "Central",
                    InfrastructureLocation("Central US"),  # Iowa
                    InfrastructureLocation("North Central US"),  # Illinois
                    InfrastructureLocation("South Central US"),  # Texas
                    InfrastructureLocation("West Central US")),  # Wyoming
                InfrastructureLocation(
                    "East",
                    InfrastructureLocation("East US"),  # Virginia
                    InfrastructureLocation("East US 2"),  # Virginia
                    InfrastructureLocation("East US 3")),  # Georgia
                InfrastructureLocation(
                    "West",
                    InfrastructureLocation("West US"),  # California
                    InfrastructureLocation("West US 2"),  # Washington
                    InfrastructureLocation("West US 3")))),  # Arizona
            liveRepo=GitHubRepository("billynewport/test_step1", "live")
        )

    # Add the EU GZ and its policies limiting locations to EU only
    gzEU: GovernanceZone = e.getZoneOrThrow("EU")

    # Enumerate all EU locations for AWS
    allEULocations: set[InfrastructureLocation] = e.getLocationOrThrow("AWS", ["EU"]).getEveryChildLocation()
    allEUKeys: set[LocationKey] = {LocationKey("AWS:EU/" + loc.name) for loc in allEULocations}

    allUSALocations: set[InfrastructureLocation] = e.getLocationOrThrow("AWS", ["USA"]).getEveryChildLocation()
    allUSAKeys: set[LocationKey] = {LocationKey("AWS:USA/" + loc.name) for loc in allUSALocations}

    gzEU.add(
        InfraStructureLocationPolicy("EU Only", PlainTextDocumentation("Test"), allEUKeys),
        TeamDeclaration("ParisTeam", GitHubRepository("billynewport/test_step1", "ParisMain"))
    )

    # Define the EU team and datastores
    defineEU_GZ(gzEU, e)

    # Define the USA GZ, restrict to AWS East locations only, only the AWS Dataplatform is allowed and define the NY team
    gzUSA: GovernanceZone = e.getZoneOrThrow("USA")
    gzUSA.add(
        # AWS Locations only
        InfraStructureVendorPolicy("AWS Only", PlainTextDocumentation("Test"), {VendorKey("AWS")}),  # AWS Locations only
        # AWS USA locations
        InfraStructureLocationPolicy("AWS US Only", PlainTextDocumentation("Test"), allUSAKeys),
        # AWS Dataplatform only
        DataPlatformPolicy("AWS Platform only", PlainTextDocumentation("Test"), {DataPlatformKey("AWS Platform")}),  # AWS DataPlatform only
        # NY Team definition
        TeamDeclaration("NYTeam", GitHubRepository("billynewport/test_step1", "NYMain"))  # NY Team
    )
    return e
