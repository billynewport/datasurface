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
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import VarChar, Date
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md import PostgresDatabase, Datastore, SQLSnapshotIngestion, CronTrigger, IngestionConsistencyType, Dataset, Workspace, \
    DatasetGroup, DataPlatformManagedDataContainer, DatasetSink
from datasurface.md.governance import ConsumerRetentionRequirements, DataMilestoningStrategy, DataLatency, WorkspacePlatformConfig


def defineTablesAndWorkspaces(eco: Ecosystem, gz: GovernanceZone, t: Team):
    t.add(
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
            "ConsumerLive",
            DataPlatformManagedDataContainer("ConsumerLive container"),
            DatasetGroup(
                "DSGLive",
                sinks=[
                    DatasetSink("Store1", "people")
                ],
                platform_chooser=WorkspacePlatformConfig(
                    hist=ConsumerRetentionRequirements(
                        r=DataMilestoningStrategy.LIVE_ONLY,
                        latency=DataLatency.MINUTES,
                        regulator=None
                    )
                )
            )
        ),
        Workspace(
            "ConsumerRemoteForensic",
            DataPlatformManagedDataContainer("ConsumerLive container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store1", "people")
                ],
                platform_chooser=WorkspacePlatformConfig(
                    hist=ConsumerRetentionRequirements(
                        r=DataMilestoningStrategy.FORENSIC,
                        latency=DataLatency.MINUTES,
                        regulator=None
                    )
                )
            )
        ),
        Workspace(
            "ConsumerForensic",
            DataPlatformManagedDataContainer("ConsumerForensic container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store1", "people")
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
                milestoneStrategy=YellowMilestoneStrategy.SCD1),
            YellowDataPlatform(
                "YellowForensic",
                doc=PlainTextDocumentation("Forensic Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
                ),
            YellowDataPlatform(
                "YellowRemoteForensic",
                doc=PlainTextDocumentation("Forensic remote Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
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

    defineTablesAndWorkspaces(ecosys, gz, team)

    tree: ValidationTree = ecosys.lintAndHydrateCaches()
    if (tree.hasErrors()):
        tree.printTree()
        raise Exception("Ecosystem validation failed")
    return ecosys
