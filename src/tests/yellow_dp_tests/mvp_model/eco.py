"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Team, GovernanceZoneDeclaration, GovernanceZone, InfrastructureVendor, InfrastructureLocation, TeamDeclaration
from datasurface.md import Ecosystem, LocationKey
from datasurface.md.credential import Credential, CredentialType
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.platforms.yellow import YellowDataPlatform, YellowMilestoneStrategy
from datasurface.md import CloudVendor, DefaultDataPlatform, \
        DataPlatformKey, WorkspacePlatformConfig
from datasurface.md import ValidationTree
from datasurface.md.governance import Datastore, Dataset, SQLSnapshotIngestion, HostPortPair, CronTrigger, IngestionConsistencyType, \
    ConsumerRetentionRequirements, DataMilestoningStrategy, DataLatency
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import VarChar, Date
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md import Workspace, DatasetSink, DatasetGroup, PostgresDatabase


def createEcosystem() -> Ecosystem:
    """This is a very simple test model with a single datastore and dataset.
    It is used to test the YellowDataPlatform."""
    ecosys: Ecosystem = Ecosystem(
        name="Test",
        repo=GitHubRepository("billynewport/repo", "ECOmain"),
        data_platforms=[
            YellowDataPlatform(
                name="YellowLive",
                locs={LocationKey("MyCorp:USA/NY_1")},
                doc=PlainTextDocumentation("Live Yellow DataPlatform"),
                postgresName="pg-data",  # Both YellowDataPlatforms use the same postgres and database, tables are different because of platform name prefix
                namespace="ns-kub-pg-test",
                connectCredentials=Credential("connect", CredentialType.API_TOKEN),
                postgresCredential=Credential("postgres", CredentialType.USER_PASSWORD),
                gitCredential=Credential("git", CredentialType.API_TOKEN),
                slackCredential=Credential("slack", CredentialType.API_TOKEN),
                airflowName="airflow",
                milestoneStrategy=YellowMilestoneStrategy.LIVE_ONLY
                ),
            YellowDataPlatform(
                "YellowForensic",
                locs={LocationKey("MyCorp:USA/NY_1")},
                doc=PlainTextDocumentation("Forensic Yellow DataPlatform"),
                postgresName="pg-data",  # Both YellowDataPlatforms use the same postgres and database, tables are different because of platform name prefix
                namespace="ns-kub-pg-test",
                connectCredentials=Credential("connect", CredentialType.API_TOKEN),
                postgresCredential=Credential("postgres", CredentialType.USER_PASSWORD),
                gitCredential=Credential("git", CredentialType.API_TOKEN),
                slackCredential=Credential("slack", CredentialType.API_TOKEN),
                airflowName="airflow",
                milestoneStrategy=YellowMilestoneStrategy.BATCH_MILESTONED
                )
        ],
        default_data_platform=DefaultDataPlatform(DataPlatformKey("YellowLive")),
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
                PostgresDatabase(
                    "Test_DB",  # Model name for database
                    hostPort=HostPortPair("localhost", 5432),  # Host and port for database
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    databaseName="test_db"  # Database name
                ),
                CronTrigger("Test_DB Every 10 mins", "0,10,20,30,40,50 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("postgres", CredentialType.USER_PASSWORD),  # Credential for platform to read from database
                ),
            datasets=[
                Dataset(
                    "customers",
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
                                "email", VarChar(100), nullable=NullableStatus.NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Email")]),
                            DDLColumn(
                                "phone", VarChar(100), nullable=NullableStatus.NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Phone")]),
                            DDLColumn(
                                "primaryAddressId", VarChar(20), nullable=NullableStatus.NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Unique Identifier")]),
                            DDLColumn(
                                "billingAddressId", VarChar(20), nullable=NullableStatus.NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Unique Identifier")])
                        ]
                    )
                ),
                Dataset(
                    "addresses",
                    schema=DDLTable(
                        columns=[
                            DDLColumn(
                                "id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Unique Identifier")]),
                            DDLColumn(
                                "customerId", VarChar(20), nullable=NullableStatus.NOT_NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Unique Identifier")]),
                            DDLColumn(
                                "streetName", VarChar(100), nullable=NullableStatus.NOT_NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Address")]),
                            DDLColumn(
                                "city", VarChar(100), nullable=NullableStatus.NOT_NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Address")]),
                            DDLColumn(
                                "state", VarChar(100), nullable=NullableStatus.NOT_NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Address")]),
                            DDLColumn(
                                "zipCode", VarChar(30), nullable=NullableStatus.NOT_NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Address")])
                        ]
                    )
                )
            ]
        ),
        Workspace(
            "Consumer1",
            # No DataContainer here, it's provided by the DataPlatform
            # In this example, it would be the merge postgres database configured for the DataPlatform
            DatasetGroup(
                "LiveDSG",
                sinks=[
                    DatasetSink("Store1", "customers"),
                    DatasetSink("Store1", "addresses")
                ],
                platform_chooser=WorkspacePlatformConfig(
                    hist=ConsumerRetentionRequirements(
                        r=DataMilestoningStrategy.LIVE_ONLY,
                        latency=DataLatency.MINUTES,
                        regulator=None
                    )
                ),
            ),
            DatasetGroup(
                "ForensicDSG",
                sinks=[
                    DatasetSink("Store1", "customers"),
                    DatasetSink("Store1", "addresses")
                ],
                platform_chooser=WorkspacePlatformConfig(
                    hist=ConsumerRetentionRequirements(
                        r=DataMilestoningStrategy.FORENSIC,
                        latency=DataLatency.MINUTES,
                        regulator=None
                    )
                )
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
