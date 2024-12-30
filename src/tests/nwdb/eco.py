"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


from datasurface.md import Team, GovernanceZoneDeclaration, GovernanceZone, InfrastructureVendor, InfrastructureLocation, TeamDeclaration
from datasurface.md import Ecosystem
from datasurface.platforms.legacy import LegacyDataPlatform
from datasurface.md import PlainTextDocumentation
from datasurface.md import GitHubRepository
from datasurface.md import CloudVendor, DefaultDataPlatform, InfraStructureLocationPolicy, \
        DataPlatformKey
from datasurface.md import ValidationTree
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
        GovernanceZoneDeclaration("EU", GitHubRepository("billynewport/repo", "EUmain")),
        GovernanceZoneDeclaration("UK", GitHubRepository("billynewport/repo", "UKmain")),

        # Infra Vendors and locations

        # Onsite data centers
        InfrastructureVendor(
            "MyCorp",
            CloudVendor.PRIVATE,
            PlainTextDocumentation("Private company data centers"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation("NJ_1"),
                InfrastructureLocation("NY_1")),
            InfrastructureLocation(
                "UK",
                InfrastructureLocation("London"),
                InfrastructureLocation("Cambridge"))),

        # Outsourced data centers with same location equivalents
        InfrastructureVendor(
            "Outsource",
            CloudVendor.PRIVATE,
            PlainTextDocumentation("Outsourced company data centers"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation("NJ_1"),
                InfrastructureLocation("NY_1")),
            InfrastructureLocation(
                "UK",
                InfrastructureLocation("London"),
                InfrastructureLocation("Cambridge")))
        )

    gzUSA: GovernanceZone = ecosys.getZoneOrThrow("USA")

    gzUSA.add(
            TeamDeclaration("FrontOffice", GitHubRepository("billynewport/repo", "FOmain")),
            TeamDeclaration("MiddleOffice", GitHubRepository("billynewport/repo", "MOmain")),
            TeamDeclaration("NorthWindTeam", GitHubRepository("billynewport/repo", "NWmain")),
            TeamDeclaration("BackOffice", GitHubRepository("billynewport/repo", "BOmain")),
            InfraStructureLocationPolicy("Private USA Only", PlainTextDocumentation("Test"), ecosys.getAllChildLocations("MyCorp", ["USA"]), None)
        )

    gzUK: GovernanceZone = ecosys.getZoneOrThrow("UK")
    gzUK.add(
        TeamDeclaration("FrontOffice", GitHubRepository("billynewport/repo", "FOmain")),
        TeamDeclaration("MiddleOffice", GitHubRepository("billynewport/repo", "MOmain")),
        TeamDeclaration("BackOffice", GitHubRepository("billynewport/repo", "BOmain")),
        InfraStructureLocationPolicy("Private UK Only", PlainTextDocumentation("Test"), ecosys.getAllChildLocations("MyCorp", ["UK"]), None)
    )

    # Fill out the NorthWindTeam managed by the USA governance zone
    nw_team: Team = ecosys.getTeamOrThrow("USA", "NorthWindTeam")
    defineNWTeamTables(ecosys, gzUSA, nw_team)
    defineNWTeamWorkspaces(ecosys, nw_team, {ecosys.getLocationOrThrow("MyCorp", ["USA", "NY_1"])})

    tree: ValidationTree = ecosys.lintAndHydrateCaches()
    if (tree.hasErrors()):
        tree.printTree()
        raise Exception("Ecosystem validation failed")
    return ecosys


def test_Validate():
    ecosys: Ecosystem = createEcosystem()
    vTree: ValidationTree = ecosys.lintAndHydrateCaches()
    if (vTree.getErrors()):
        print(vTree)
        raise Exception("Ecosystem validation failed")
    else:
        print("Ecosystem validated OK")


if __name__ == "__main__":
    test_Validate()
