"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


from datasurface.md import Team, GovernanceZoneDeclaration, GovernanceZone, InfrastructureVendor, InfrastructureLocation, TeamDeclaration
from datasurface.md import Ecosystem, LocationKey
from datasurface.platforms.legacy import LegacyDataPlatform
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.md import CloudVendor, InfraStructureLocationPolicy, \
        DataPlatformChooser
from datasurface.md import ValidationTree
from tests.nwdb.nwdb import defineTables as defineNWTeamTables
from tests.nwdb.nwdb import defineWorkspaces as defineNWTeamWorkspaces
from datasurface.platforms.legacy import LegacyDataPlatformChooser, LegacyPlatformServiceProvider


def createEcosystem() -> Ecosystem:
    psp: LegacyPlatformServiceProvider = LegacyPlatformServiceProvider(
        "LegacyPSP",
        {LocationKey("MyCorp:USA/NY_1")},
        [
            LegacyDataPlatform("LegacyA", PlainTextDocumentation("Test")),
        ]
    )
    ecosys: Ecosystem = Ecosystem(
        name="Test",
        repo=GitHubRepository("billynewport/repo", "ECOmain"),
        platform_services_providers=[psp],
        liveRepo=GitHubRepository("billynewport/repo", "EcoLive"),
        governance_zone_declarations=[
            GovernanceZoneDeclaration("USA", GitHubRepository("billynewport/repo", "USAmain")),
            GovernanceZoneDeclaration("EU", GitHubRepository("billynewport/repo", "EUmain")),
            GovernanceZoneDeclaration("UK", GitHubRepository("billynewport/repo", "UKmain"))
        ],
        infrastructure_vendors=[
            # Onsite data centers
            InfrastructureVendor(
                name="MyCorp",
                cloud_vendor=CloudVendor.PRIVATE,
                documentation=PlainTextDocumentation("Private company data centers"),
                locations=[
                    InfrastructureLocation(
                        name="USA",
                        locations=[
                            InfrastructureLocation(name="NJ_1"),
                            InfrastructureLocation(name="NY_1")
                        ]
                    ),
                    InfrastructureLocation(
                        name="UK",
                        locations=[
                            InfrastructureLocation(name="London"),
                            InfrastructureLocation(name="Cambridge")
                        ]
                    )
                ]
            ),

            # Outsourced data centers with same location equivalents
            InfrastructureVendor(
                name="Outsource",
                cloud_vendor=CloudVendor.PRIVATE,
                documentation=PlainTextDocumentation("Outsourced company data centers"),
                locations=[
                    InfrastructureLocation(
                        name="USA",
                        locations=[
                            InfrastructureLocation(name="NJ_1"),
                            InfrastructureLocation(name="NY_1")
                        ]
                    ),
                    InfrastructureLocation(
                        name="UK",
                        locations=[
                            InfrastructureLocation(name="London"),
                            InfrastructureLocation(name="Cambridge")
                        ]
                    )
                ]
            )
        ]
    )

    gzUSA: GovernanceZone = ecosys.getZoneOrThrow("USA")

    allUSALocations: set[InfrastructureLocation] = ecosys.getAllChildLocations("MyCorp", ["USA"])
    allUSAKeys: set[LocationKey] = {LocationKey("MyCorp:USA/" + loc.name) for loc in allUSALocations}

    gzUSA.add(
            TeamDeclaration("FrontOffice", GitHubRepository("billynewport/repo", "FOmain")),
            TeamDeclaration("MiddleOffice", GitHubRepository("billynewport/repo", "MOmain")),
            TeamDeclaration("NorthWindTeam", GitHubRepository("billynewport/repo", "NWmain")),
            TeamDeclaration("BackOffice", GitHubRepository("billynewport/repo", "BOmain")),
            InfraStructureLocationPolicy("Private USA Only", PlainTextDocumentation("Test"), allUSAKeys, None)
        )

    allUKLocations: set[InfrastructureLocation] = ecosys.getAllChildLocations("MyCorp", ["UK"])
    allUKKeys: set[LocationKey] = {LocationKey("MyCorp:UK/" + loc.name) for loc in allUKLocations}

    gzUK: GovernanceZone = ecosys.getZoneOrThrow("UK")
    gzUK.add(
        TeamDeclaration("FrontOffice", GitHubRepository("billynewport/repo", "FOmain")),
        TeamDeclaration("MiddleOffice", GitHubRepository("billynewport/repo", "MOmain")),
        TeamDeclaration("BackOffice", GitHubRepository("billynewport/repo", "BOmain")),
        InfraStructureLocationPolicy("Private UK Only", PlainTextDocumentation("Test"), allUKKeys, None)
    )

    # Fill out the NorthWindTeam managed by the USA governance zone
    nw_team: Team = ecosys.getTeamOrThrow("USA", "NorthWindTeam")
    defineNWTeamTables(ecosys, gzUSA, nw_team)
    chooser: DataPlatformChooser = LegacyDataPlatformChooser(
        "LegacyA",
        PlainTextDocumentation("This is a legacy application that is managed by the LegacyApplicationTeam"),
        set()
    )
    defineNWTeamWorkspaces(ecosys, nw_team, {LocationKey("MyCorp:USA/NY_1")}, chooser, ecosys.getDataPlatformOrThrow("LegacyA"))

    tree: ValidationTree = ecosys.lintAndHydrateCaches()
    if (tree.hasErrors()):
        tree.printTree()
        raise Exception("Ecosystem validation failed")
    return ecosys


def test_Validate():
    ecosys: Ecosystem = createEcosystem()
    vTree: ValidationTree = ecosys.lintAndHydrateCaches()
    if (vTree.hasErrors()):
        print("Ecosystem validation failed with errors:")
        vTree.printTree()
        raise Exception("Ecosystem validation failed")
    else:
        print("Ecosystem validated OK")
        if vTree.hasWarnings():
            print("Note: There are some warnings:")
            vTree.printTree()


if __name__ == "__main__":
    test_Validate()
