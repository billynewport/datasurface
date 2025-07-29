"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


from typing import Optional
import unittest

from datasurface.md import InfrastructureVendor, InfrastructureLocation, TeamDeclaration, Ecosystem
from datasurface.md import GovernanceZone, GovernanceZoneDeclaration
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.md import CloudVendor
from datasurface.md import ValidationTree
from tests.nwdb.eco import createEcosystem


class TestZones(unittest.TestCase):

    def test_DuplicateTeamNames(self):
        eco: Ecosystem = createEcosystem()

        gzUSA: GovernanceZone = eco.getZoneOrThrow("USA")
        self.assertEqual(eco.getTeamOrThrow("USA", "FrontOffice"), gzUSA.getTeamOrThrow("FrontOffice"))
        self.assertEqual(eco.getTeamOrThrow("USA", "MiddleOffice"), gzUSA.getTeamOrThrow("MiddleOffice"))
        self.assertEqual(eco.getTeamOrThrow("USA", "BackOffice"), gzUSA.getTeamOrThrow("BackOffice"))

        tree: ValidationTree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        gzEU: GovernanceZone = eco.getZoneOrThrow("UK")
        self.assertEqual(eco.getTeamOrThrow("UK", "FrontOffice"), gzEU.getTeamOrThrow("FrontOffice"))
        self.assertEqual(eco.getTeamOrThrow("UK", "MiddleOffice"), gzEU.getTeamOrThrow("MiddleOffice"))
        self.assertEqual(eco.getTeamOrThrow("UK", "BackOffice"), gzEU.getTeamOrThrow("BackOffice"))

        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

    def checkChildLocation(self, parent: InfrastructureLocation, childName: str, vendor: InfrastructureVendor):
        child: Optional[InfrastructureLocation] = parent.locations.get(childName)
        if (child is None):
            raise Exception("Child location {} not found in parent {}".format(childName, parent.name))
        self.assertIsNotNone(child)
        self.assertEqual(child.name, childName)
        self.assertIsNotNone(child)

    def test_CreateUSAEco(self):
        eco = Ecosystem("BigCorp", GitHubRepository("o/r", "b"), liveRepo=GitHubRepository("o/r", "live"))
        self.assertEqual(eco.name, "BigCorp")
        self.assertEqual(eco.zones.getNumObjects(), 0)
        usZoneName: str = "US"
        eco.add(
            GovernanceZoneDeclaration(usZoneName, GitHubRepository("aa/cc", "bb")),
        )
        gzUSA: GovernanceZone = eco.getZoneOrThrow(usZoneName)
        eco.add(
                InfrastructureVendor(
                    "AWS",
                    CloudVendor.AWS,
                    PlainTextDocumentation("AWS is a cloud provider"),
                    InfrastructureLocation(
                        "USA",
                        InfrastructureLocation("us-east-1"),
                        InfrastructureLocation("us-east-2"),
                        InfrastructureLocation("us-west-1"),
                        InfrastructureLocation("us-west-2")
                        ),
                    InfrastructureLocation(
                        "Europe",
                        InfrastructureLocation("eu-west-1"),
                        InfrastructureLocation("eu-west-2")
                        ),
                ))

        eco.add(
            InfrastructureVendor(
                "AZURE",
                CloudVendor.AZURE,
                PlainTextDocumentation("AZURE is a cloud provider"),
                InfrastructureLocation(
                    "USA",
                    InfrastructureLocation("Central US"),
                    InfrastructureLocation("North Central US"),
                    InfrastructureLocation("South Central US"),
                    InfrastructureLocation("West Central US"),
                    InfrastructureLocation("West US"),
                    InfrastructureLocation("West US 2"),
                    InfrastructureLocation("West US 3"),
                    InfrastructureLocation("East US"),
                    InfrastructureLocation("East US 2"),
                    InfrastructureLocation("East US 3")
                    ),
                ))
        gzUSA.add(
            TeamDeclaration(
                "Billys team",
                GitHubRepository("data-fracture/ecomgr", "module")
                ),
            TeamDeclaration(
                "Jacks team",
                GitHubRepository("gitrepo2/aa", "module2"))
            )
        self.assertEqual(eco.zones.getNumObjects(), 1)
        self.assertEqual(eco.getZone("US"), gzUSA)

        self.assertEqual(gzUSA.name, usZoneName)
        self.assertEqual(len(eco.vendors), 2)

        aws: Optional[InfrastructureVendor] = eco.vendors.get("AWS")
        if (aws is None):
            raise Exception("Vendor AWS not found")
        self.assertIsNotNone(aws)
        self.assertEqual(len(aws.locations), 2)
        awsUSA: Optional[InfrastructureLocation] = aws.locations.get("USA")
        if (awsUSA is None):
            raise Exception("Location USA not found")
        self.assertIsNotNone(awsUSA)
        self.assertEqual(awsUSA.name, "USA")

        for locName in ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']:
            self.checkChildLocation(awsUSA, locName, aws)
        awsEurope: Optional[InfrastructureLocation] = aws.locations.get("Europe")
        if (awsEurope is None):
            self.assertIsNotNone(awsEurope)
            raise Exception("Location Europe not found")

        self.assertEqual(awsEurope.name, "Europe")
        for locName in ['eu-west-1', 'eu-west-2']:
            self.checkChildLocation(awsEurope, locName, aws)

        azure: Optional[InfrastructureVendor] = eco.vendors.get("AZURE")
        if (azure is None):
            raise Exception("Vendor AZURE not found")
        self.assertIsNotNone(azure)
        self.assertEqual(len(azure.locations), 1)
        azureUSA: Optional[InfrastructureLocation] = azure.locations.get("USA")
        if (azureUSA is None):
            raise Exception("Location USA not found")
        self.assertIsNotNone(azureUSA)
        self.assertEqual(azureUSA.name, "USA")
        self.assertEqual(len(azureUSA.locations), 10)
        for locName in ['Central US', 'North Central US', 'South Central US', 'West Central US', 'West US', 'West US 2',
                        'West US 3', 'East US', 'East US 2', 'East US 3']:
            self.checkChildLocation(azureUSA, locName, azure)

        tree: ValidationTree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())


if __name__ == '__main__':
    unittest.main()
