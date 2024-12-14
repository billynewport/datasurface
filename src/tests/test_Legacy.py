"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.md import Ecosystem, GovernanceZone, Team, DataPlatformKey, TeamDeclaration, GovernanceZoneDeclaration
from datasurface.md import GitHubRepository
from datasurface.md import CloudVendor, DefaultDataPlatform, InfrastructureVendor, InfrastructureLocation
from datasurface.md import PlainTextDocumentation
from datasurface.md import ValidationTree
from datasurface.platforms.legacy import LegacyDataPlatform
from tests.nwdb.nwdb import defineTables as defineNWTeamTables
from tests.nwdb.nwdb import defineWorkspaces as defineNWTeamWorkspaces


def createEcosystem() -> Ecosystem:
    ecosys: Ecosystem = Ecosystem(
        "Test",
        GitHubRepository("billynewport/repo", "ECOmain"),
        LegacyDataPlatform(
            "LegacyA",
            PlainTextDocumentation("Test")),

        # Data Platforms
        DefaultDataPlatform(DataPlatformKey("LegacyA")),

        # GovernanceZones
        GovernanceZoneDeclaration("USA", GitHubRepository("billynewport/repo", "USAmain")),

        # Infra Vendors and locations
        InfrastructureVendor(
            "LegacyA",
            CloudVendor.PRIVATE,
            PlainTextDocumentation("Legacy infrastructure"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation("ny1"),  # New York City
                InfrastructureLocation("nj1"))),  # New Jersey
        )

    gzUSA: GovernanceZone = ecosys.getZoneOrThrow("USA")

    gzUSA.add(
            TeamDeclaration("LegacyApplicationTeam", GitHubRepository("billynewport/repo", "legacyAppTeamMain")),
        )

    # Fill out the NorthWindTeam managed by the USA governance zone
    legacy_Team: Team = ecosys.getTeamOrThrow("USA", "LegacyApplicationTeam")
    defineNWTeamTables(ecosys, gzUSA, legacy_Team)
    defineNWTeamWorkspaces(ecosys, legacy_Team, ecosys.getLocationOrThrow("LegacyA", ["USA", "ny1"]))

    tree: ValidationTree = ecosys.lintAndHydrateCaches()
    if (tree.hasErrors()):
        tree.printTree()
        raise Exception("Ecosystem validation failed")
    return ecosys


class TestLegacy(unittest.TestCase):
    def testLegacyPlatform(self):
        e: Ecosystem = createEcosystem()
        self.assertTrue(e is not None)
