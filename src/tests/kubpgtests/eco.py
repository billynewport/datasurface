"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


from datasurface.md import Team, GovernanceZoneDeclaration, GovernanceZone, InfrastructureVendor, InfrastructureLocation, TeamDeclaration
from datasurface.md import Ecosystem, LocationKey
from datasurface.md.credential import Credential, CredentialType
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.platforms.kubpgstarter import KubernetesPGStarterDataPlatform
from datasurface.md import CloudVendor, DefaultDataPlatform, \
        DataPlatformKey, WorkspaceFixedDataPlatform
from datasurface.md import ValidationTree
from datasurface.md.governance import Datastore, Dataset, SQLSnapshotIngestion, HostPortPair, CronTrigger, IngestionConsistencyType
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import VarChar, Date
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md import Workspace, DatasetSink, DatasetGroup, PostgresDatabase


def createEcosystem() -> Ecosystem:
    """This is a very simple test model with a single datastore and dataset.
    It is used to test the KubernetesPGStarterDataPlatform."""
    ecosys: Ecosystem = Ecosystem(
        name="Test",
        repo=GitHubRepository("billynewport/repo", "ECOmain"),
        data_platforms=[
            KubernetesPGStarterDataPlatform(
                "Test_DP",
                {LocationKey("MyCorp:USA/NY_1")},
                PlainTextDocumentation("Test"),
                "ns-kub-pg-test",
                Credential("connect", CredentialType.API_TOKEN),
                Credential("postgres", CredentialType.USER_PASSWORD),
                Credential("git", CredentialType.API_TOKEN),
                Credential("slack", CredentialType.API_TOKEN),
                "airflow")
        ],
        default_data_platform=DefaultDataPlatform(DataPlatformKey("Test_DP")),
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
        ]
    )
    gz: GovernanceZone = ecosys.getZoneOrThrow("USA")

    # Add a team to the governance zone
    gz.add(TeamDeclaration(
        "team1",
        GitHubRepository("billynewport/repo", "team1")
        ))

    team: Team = gz.getTeamOrThrow("team1")
    team.add(
        Datastore(
            "Store1",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLSnapshotIngestion(
                PostgresDatabase("NW_DB", HostPortPair("localhost", 1344), {LocationKey("MyCorp:USA/NY_1")}, "DBName"),
                CronTrigger("NW_Data Every 10 mins", "0,10,20,30,40,50 * * * *"),
                IngestionConsistencyType.MULTI_DATASET,
                Credential("eu_cred", CredentialType.USER_PASSWORD),
                ),
            datasets=[
                Dataset(
                    "people",
                    schema=DDLTable(
                        columns=[
                            DDLColumn(
                                "id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Unique Identifier")]),
                            DDLColumn(
                                "firstName", VarChar(100), nullable=NullableStatus.NOT_NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "First Name")]),
                            DDLColumn(
                                "lastName", VarChar(100), nullable=NullableStatus.NOT_NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Last Name")]),
                            DDLColumn(
                                "dob", Date(), nullable=NullableStatus.NOT_NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Date of Birth")]),
                            DDLColumn(
                                "employer", VarChar(100), nullable=NullableStatus.NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Employer")]),
                            DDLColumn(
                                "dod", Date(), nullable=NullableStatus.NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Date of Death")])
                        ]
                    )
                )
            ]
        ),
        Workspace(
            "Consumer1",
            DatasetGroup(
                "TestDSG",
                sinks=[
                    DatasetSink("Store1", "people")
                ],
                platform_chooser=WorkspaceFixedDataPlatform(DataPlatformKey("Test_DP"))
            )
        )
    )

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
