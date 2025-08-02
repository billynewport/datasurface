"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.md import CloudVendor, Ecosystem, \
    GovernanceZoneDeclaration, InfrastructureLocation, InfrastructureVendor, LocationKey
from datasurface.platforms.legacy import LegacyDataPlatform, LegacyPlatformServiceProvider

# Base branch for step 1, define an Ecosystem, data platforms, infrastructure vendors/locations and 3 Governance Zones


def createEcosystem() -> Ecosystem:
    azurePSP: LegacyPlatformServiceProvider = LegacyPlatformServiceProvider(
        "AzurePSP",
        {LocationKey("Azure:USA/Central")},
        [
            LegacyDataPlatform("Azure Platform", PlainTextDocumentation("Test"))
        ]
    )
    awsPSP: LegacyPlatformServiceProvider = LegacyPlatformServiceProvider(
        "AWSPSP",
        {LocationKey("AWS:USA/Virginia")},
        [
            LegacyDataPlatform("AWS Platform", PlainTextDocumentation("Test"))
        ]
    )
    e: Ecosystem = Ecosystem(
        "Test", GitHubRepository("billynewport/test_step1", "main"),

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
            liveRepo=GitHubRepository("billynewport/test_step1", "live"),
            platform_services_providers=[azurePSP, awsPSP]
        )
    return e
