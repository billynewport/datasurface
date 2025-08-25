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
from datasurface.md import CloudVendor, WorkspacePlatformConfig
from datasurface.md import ValidationTree
from datasurface.md.governance import (
    Datastore, Dataset, SQLSnapshotIngestion, SQLWatermarkSnapshotDeltaIngestion, HostPortPair, CronTrigger, IngestionConsistencyType,
    ConsumerRetentionRequirements, DataMilestoningStrategy, DataLatency)
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import VarChar, Date, Timestamp
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md import Workspace, DatasetSink, DatasetGroup, PostgresDatabase, DataPlatformManagedDataContainer
from tests.nwdb.nwdb import addDSGPlatformMappingForWorkspace
from datasurface.platforms.yellow.assembly import GitCacheConfig, YellowSinglePostgresDatabaseAssembly, K8sResourceLimits, StorageRequirement


def createEcosystem() -> Ecosystem:
    """This is a very simple test model with a single datastore and dataset.
    It is used to test the YellowDataPlatform."""
    gitcache: GitCacheConfig = GitCacheConfig(
        enabled=True,
        storageClass="longhorn",
    )

    KUB_NAME_SPACE: str = "ns-kub-pg-test"
    yp_assm: YellowSinglePostgresDatabaseAssembly = YellowSinglePostgresDatabaseAssembly(
        name="Test_DP",
        namespace=f"{KUB_NAME_SPACE}",
        git_cache_config=gitcache,
        nfs_server_node="git-cache-server",
        afHostPortPair=HostPortPair(f"airflow-service.{KUB_NAME_SPACE}.svc.cluster.local", 8080),
        dbStorageNeeds=StorageRequirement("10G"),
        dbResourceLimits=K8sResourceLimits(
            requested_memory=StorageRequirement("1G"),
            limits_memory=StorageRequirement("2G"),
            requested_cpu=1.0,
            limits_cpu=2.0
        ),
        afWebserverResourceLimits=K8sResourceLimits(
            requested_memory=StorageRequirement("1G"),
            limits_memory=StorageRequirement("2G"),
            requested_cpu=1.0,
            limits_cpu=2.0
        ),
        afSchedulerResourceLimits=K8sResourceLimits(
            requested_memory=StorageRequirement("4G"),
            limits_memory=StorageRequirement("4G"),
            requested_cpu=4.0,
            limits_cpu=4.0
        )
    )
    psp: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "Test_DP",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("Test"),
        yp_assembly=yp_assm,
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        connectCredentials=Credential("connect", CredentialType.API_TOKEN),
        mergeRW_Credential=Credential("postgres", CredentialType.USER_PASSWORD),
        merge_datacontainer=PostgresDatabase(
            "Test_DB",  # Model name for database
            hostPort=HostPortPair("localhost", 5432),  # Host and port for database
            locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
            databaseName="test_db"),  # Database name
        dataPlatforms=[
            YellowDataPlatform(
                name="Test_DP",
                doc=PlainTextDocumentation("Test"),
                milestoneStrategy=YellowMilestoneStrategy.SCD1)
            ]
        )
    ecosys: Ecosystem = Ecosystem(
        name="Test",
        repo=GitHubRepository("billynewport/repo", "ECOmain"),
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
        liveRepo=GitHubRepository("billynewport/repo", "EcoLive")
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
        Datastore(
            "Store2",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLWatermarkSnapshotDeltaIngestion(
                PostgresDatabase(
                    "Test_DB",  # Model name for database
                    hostPort=HostPortPair("localhost", 5432),  # Host and port for database
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    databaseName="test_db"  # Database name
                ),
                "last_update_time",  # watermarkColumn
                Timestamp(),  # watermarkDataType
                CronTrigger("Test_DB Every 10 mins", "0,10,20,30,40,50 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("postgres", CredentialType.USER_PASSWORD)  # Credential for platform to read from database
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
                                classifications=[SimpleDC(SimpleDCTypes.CPI, "Date of Death")]),
                            DDLColumn(
                                "last_update_time", Timestamp(), nullable=NullableStatus.NOT_NULLABLE,
                                classifications=[SimpleDC(SimpleDCTypes.PUB, "Last Update Time")])
                        ]
                    )
                )
            ]
        ),
        Workspace(
            "Consumer1",
            DataPlatformManagedDataContainer("Consumer1 container"),
            DatasetGroup(
                "TestDSG",
                sinks=[
                    DatasetSink("Store1", "people"),
                    DatasetSink("Store2", "people")
                ],
                platform_chooser=WorkspacePlatformConfig(
                    hist=ConsumerRetentionRequirements(
                        r=DataMilestoningStrategy.LIVE_ONLY,
                        latency=DataLatency.MINUTES,
                        regulator=None
                    )
                )
            )
        )
    )

    ecosys.lintAndHydrateCaches()
    workspace: Workspace = ecosys.cache_getWorkspaceOrThrow("Consumer1").workspace
    addDSGPlatformMappingForWorkspace(ecosys, workspace, workspace.dsgs["TestDSG"], ecosys.getDataPlatformOrThrow("Test_DP"))

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
