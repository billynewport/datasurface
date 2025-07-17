"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Team, GovernanceZoneDeclaration, GovernanceZone, InfrastructureVendor, InfrastructureLocation, TeamDeclaration
from datasurface.md import Ecosystem, LocationKey, DataPlatformManagedDataContainer
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

    merge_datacontainer: PostgresDatabase = PostgresDatabase(
        "MergeDB",
        hostPort=HostPortPair("pg-data.ns-kub-pg-test.svc.cluster.local", 5432),
        locations={LocationKey("MyCorp:USA/NY_1")},
        databaseName="datasurface_merge"
    )

    ecosys: Ecosystem = Ecosystem(
        name="Test",
        repo=GitHubRepository("billynewport/mvpmodel", "main"),
        data_platforms=[
            YellowDataPlatform(
                name="YellowLive",
                locs={LocationKey("MyCorp:USA/NY_1")},
                doc=PlainTextDocumentation("Live Yellow DataPlatform"),
                namespace="ns-kub-pg-test",
                connectCredentials=Credential("connect", CredentialType.API_TOKEN),
                postgresCredential=Credential("postgres", CredentialType.USER_PASSWORD),
                gitCredential=Credential("git", CredentialType.API_TOKEN),
                slackCredential=Credential("slack", CredentialType.API_TOKEN),
                merge_datacontainer=merge_datacontainer,
                airflowName="airflow",
                milestoneStrategy=YellowMilestoneStrategy.LIVE_ONLY
                ),
            YellowDataPlatform(
                "YellowForensic",
                locs={LocationKey("MyCorp:USA/NY_1")},
                doc=PlainTextDocumentation("Forensic Yellow DataPlatform"),
                namespace="ns-kub-pg-test",
                connectCredentials=Credential("connect", CredentialType.API_TOKEN),
                postgresCredential=Credential("postgres", CredentialType.USER_PASSWORD),
                gitCredential=Credential("git", CredentialType.API_TOKEN),
                slackCredential=Credential("slack", CredentialType.API_TOKEN),
                merge_datacontainer=merge_datacontainer,
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
                    "CustomerDB",  # Model name for database
                    hostPort=HostPortPair("pg-data.ns-kub-pg-test.svc.cluster.local", 5432),  # Host and port for database
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    databaseName="customer_db"  # Database name
                ),
                CronTrigger("Every 1 minute", "* * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("postgres", CredentialType.USER_PASSWORD),  # Credential for platform to read from database
                ),
            datasets=[
                Dataset(
                    "customers",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("firstname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("lastname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("dob", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("email", VarChar(100)),
                            DDLColumn("phone", VarChar(100)),
                            DDLColumn("primaryaddressid", VarChar(20)),
                            DDLColumn("billingaddressid", VarChar(20))
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.CPI, "Customer")]
                ),
                Dataset(
                    "addresses",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customerId", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("streetName", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("city", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("state", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("zipCode", VarChar(30), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.CPI, "Address")]
                )
            ]
        ),
        Workspace(
            "Consumer1",
            DataPlatformManagedDataContainer("Consumer1 container"),
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
