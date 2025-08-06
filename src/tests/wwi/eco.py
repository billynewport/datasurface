"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Team, GovernanceZoneDeclaration, GovernanceZone, InfrastructureVendor, InfrastructureLocation, TeamDeclaration
from datasurface.md import Ecosystem, LocationKey
from datasurface.md.credential import Credential, CredentialType
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.platforms.yellow import YellowDataPlatform, YellowMilestoneStrategy, YellowPlatformServiceProvider
from datasurface.md import CloudVendor
from datasurface.md import ValidationTree
from datasurface.md.governance import HostPortPair
from datasurface.md import PostgresDatabase
from tests.wwi.wwi import defineTables as defineWWITeamTables
from tests.wwi.wwi import defineWorkspaces as defineWWITeamWorkspaces
from datasurface.md.governance import ConsumerRetentionRequirements, DataMilestoningStrategy, DataLatency, WorkspacePlatformConfig
from datetime import timedelta


def createEcosystem() -> Ecosystem:
    """Wide World Importers ecosystem with comprehensive table definitions.
    This demonstrates the YellowDataPlatform with a realistic enterprise database."""

    merge_datacontainer: PostgresDatabase = PostgresDatabase(
        "MergeDB",
        hostPort=HostPortPair("localhost", 5432),
        locations={LocationKey("MyCorp:USA/NY_1")},
        databaseName="datasurface_merge"
    )

    psp: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "Test_DP",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("Test"),
        namespace="ns-kub-pg-test",
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        connectCredentials=Credential("connect", CredentialType.API_TOKEN),
        postgresCredential=Credential("postgres", CredentialType.USER_PASSWORD),
        merge_datacontainer=merge_datacontainer,
        dataPlatforms=[
            YellowDataPlatform(
                name="YellowLive",
                doc=PlainTextDocumentation("Live Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.LIVE_ONLY),
            YellowDataPlatform(
                "YellowForensic",
                doc=PlainTextDocumentation("Forensic Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.BATCH_MILESTONED
                )
        ]
    )

    ecosys: Ecosystem = Ecosystem(
        name="WorldWideImporters",
        repo=GitHubRepository("billynewport/mvpmodel", "main"),
        platform_services_providers=[psp],
        governance_zone_declarations=[
            GovernanceZoneDeclaration("USA", GitHubRepository("billynewport/repo", "USAmain"))
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
                            InfrastructureLocation(name="NY_1")
                        ]
                    )
                ]
            )
        ],
        liveRepo=GitHubRepository("billynewport/repo", "live")
    )
    gz: GovernanceZone = ecosys.getZoneOrThrow("USA")

    # Add a team to the governance zone
    gz.add(TeamDeclaration(
        "team1",
        GitHubRepository("billynewport/repo", "team1")
        ))

    team: Team = gz.getTeamOrThrow("team1")

    # Add WWI tables and workspaces to the team
    defineWWITeamTables(ecosys, gz, team)
    retention_req = ConsumerRetentionRequirements(
        DataMilestoningStrategy.LIVE_ONLY,
        DataLatency.MINUTES,
        "GDPR",
        timedelta(days=365)
    )
    chooser = WorkspacePlatformConfig(retention_req)
    defineWWITeamWorkspaces(ecosys, team, {LocationKey("MyCorp:USA/NY_1")}, chooser, ecosys.getDataPlatformOrThrow("YellowLive"))

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
