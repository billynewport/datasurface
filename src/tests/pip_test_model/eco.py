"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Team, GovernanceZoneDeclaration, GovernanceZone, InfrastructureVendor, InfrastructureLocation, TeamDeclaration
from datasurface.md import Ecosystem, LocationKey, SQLServerDatabase, OracleDatabase, DB2Database
from datasurface.md.credential import Credential, CredentialType
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.platforms.yellow import YellowDataPlatform, YellowMilestoneStrategy, YellowPlatformServiceProvider
from datasurface.md import CloudVendor
from datasurface.md import ValidationTree
from datasurface.md.governance import HostPortPair, SnowFlakeDatabase
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import VarChar, Date
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md import PostgresDatabase, Datastore, SQLSnapshotIngestion, CronTrigger, IngestionConsistencyType, Dataset, Workspace, \
    DatasetGroup, DataPlatformManagedDataContainer, DatasetSink
from datasurface.md.governance import ConsumerRetentionRequirements, DataMilestoningStrategy, DataLatency, WorkspacePlatformConfig
from datasurface.platforms.yellow.assembly import GitCacheConfig, YellowExternalSingleDatabaseAssembly, YellowSinglePostgresDatabaseAssembly, \
    K8sResourceLimits, StorageRequirement


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
            ]),
        Datastore(
            "Store2",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLSnapshotIngestion(
                SQLServerDatabase(
                    "Test_DB",  # Model name for database
                    hostPort=HostPortPair("sqlserver", 1433),  # Host and port for database
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    databaseName="test_db"  # Database name
                ),
                CronTrigger("Test_DB Every 10 mins", "0,10,20,30,40,50 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("sa", CredentialType.USER_PASSWORD),  # Credential for platform to read from database
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
            "Store3",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLSnapshotIngestion(
                OracleDatabase(
                    "Test_DB",  # Model name for database
                    hostPort=HostPortPair("oracle", 1521),  # Host and port for database
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    databaseName="free"  # Database name
                ),
                CronTrigger("Test_DB Every 10 mins", "0,10,20,30,40,50 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("system", CredentialType.USER_PASSWORD),  # Credential for platform to read from database
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
            "Store4",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLSnapshotIngestion(
                DB2Database(
                    "Test_DB",  # Model name for database
                    hostPort=HostPortPair("db2", 50000),  # Host and port for database
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    databaseName="TESTDB"  # Database name
                ),
                CronTrigger("Test_DB Every 10 mins", "0,10,20,30,40,50 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("system", CredentialType.USER_PASSWORD),  # Credential for platform to read from database
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
            "Store5",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLSnapshotIngestion(
                SnowFlakeDatabase(
                    name="Snowflake_Test_DB",  # Model name for database
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    databaseName="SNOWFLAKE_LEARNING_DB",  # Database name
                    account="HORSEQD-GQC71098",
                    schema="PUBLIC"
                ),
                CronTrigger("Test_DB Every 10 mins", "0,10,20,30,40,50 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("sf_cred", CredentialType.USER_PASSWORD),  # Credential for platform to read from database
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
        ),
        Workspace(
            "SQLServerConsumerLive",
            DataPlatformManagedDataContainer("SQLServerConsumerLive container"),
            DatasetGroup(
                "DSGLive",
                sinks=[
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
        ),
        Workspace(
            "SQLServerConsumerForensic",
            DataPlatformManagedDataContainer("SQLServerConsumerForensic container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store2", "people")
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
            "SQLServerConsumerRemoteForensic",
            DataPlatformManagedDataContainer("SQLServerConsumerRemoteForensic container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store2", "people")
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
            "OracleConsumerLive",
            DataPlatformManagedDataContainer("OracleConsumerLive container"),
            DatasetGroup(
                "DSGLive",
                sinks=[
                    DatasetSink("Store3", "people")
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
            "OracleConsumerForensic",
            DataPlatformManagedDataContainer("OracleConsumerForensic container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store3", "people")
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
            "OracleConsumerRemoteForensic",
            DataPlatformManagedDataContainer("OracleConsumerRemoteForensic container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store3", "people")
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
            "DB2ConsumerLive",
            DataPlatformManagedDataContainer("DB2ConsumerLive container"),
            DatasetGroup(
                "DSGLive",
                sinks=[
                    DatasetSink("Store4", "people")
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
            "DB2ConsumerForensic",
            DataPlatformManagedDataContainer("DB2ConsumerForensic container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store4", "people")
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
            "DB2ConsumerRemoteForensic",
            DataPlatformManagedDataContainer("DB2ConsumerRemoteForensic container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store4", "people")
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
            "SnowflakeConsumerLive",
            DataPlatformManagedDataContainer("SnowflakeConsumerLive container"),
            DatasetGroup(
                "DSGLive",
                sinks=[
                    DatasetSink("Store5", "people")
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
            "SnowflakeConsumerForensic",
            DataPlatformManagedDataContainer("SnowflakeConsumerForensic container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store5", "people")
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
            "SnowflakeConsumerRemoteForensic",
            DataPlatformManagedDataContainer("SnowflakeConsumerRemoteForensic container"),
            DatasetGroup(
                "DSGForensic",
                sinks=[
                    DatasetSink("Store5", "people")
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
    merge_sqlserver_datacontainer: SQLServerDatabase = SQLServerDatabase(
        "MergeDB",
        hostPort=HostPortPair("sqlserver", 1433),
        locations={LocationKey("MyCorp:USA/NY_1")},
        databaseName="datasurface_merge"
    )
    merge_oracle_datacontainer: OracleDatabase = OracleDatabase(
        "MergeDB",
        hostPort=HostPortPair("oracle", 1521),
        locations={LocationKey("MyCorp:USA/NY_1")},
        databaseName="free"
    )
    merge_db2_datacontainer: DB2Database = DB2Database(
        "MergeDB",
        hostPort=HostPortPair("db2", 50000),
        locations={LocationKey("MyCorp:USA/NY_1")},
        databaseName="TESTDB"
    )
    merge_snowflake_datacontainer: SnowFlakeDatabase = SnowFlakeDatabase(
        "MergeDB",
        locations={LocationKey("MyCorp:USA/NY_1")},
        databaseName="SNOWFLAKE_LEARNING_DB",
        account="HORSEQD-GQC71098",
        schema="PUBLIC"
    )

    gitcache: GitCacheConfig = GitCacheConfig(
        enabled=True,
        storageClass="longhorn",
    )

    KUB_NAME_SPACE: str = "ns-pip-test"
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
    yp_assmSQLServer: YellowExternalSingleDatabaseAssembly = YellowExternalSingleDatabaseAssembly(
        name="Test_DP_SQLServer",
        namespace=f"{KUB_NAME_SPACE}",
        git_cache_config=gitcache,
        afHostPortPair=HostPortPair(f"airflow-service.{KUB_NAME_SPACE}.svc.cluster.local", 8080),
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
    yp_assmOracle: YellowExternalSingleDatabaseAssembly = YellowExternalSingleDatabaseAssembly(
        name="Test_DP_Oracle",
        namespace=f"{KUB_NAME_SPACE}",
        git_cache_config=gitcache,
        afHostPortPair=HostPortPair(f"airflow-service.{KUB_NAME_SPACE}.svc.cluster.local", 8080),
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
    yp_assmDB2: YellowExternalSingleDatabaseAssembly = YellowExternalSingleDatabaseAssembly(
        name="Test_DP_DB2",
        namespace=f"{KUB_NAME_SPACE}",
        git_cache_config=gitcache,
        afHostPortPair=HostPortPair(f"airflow-service.{KUB_NAME_SPACE}.svc.cluster.local", 8080),
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
    yp_assmSnowflake: YellowExternalSingleDatabaseAssembly = YellowExternalSingleDatabaseAssembly(
        name="Test_DP_Snowflake",
        namespace=f"{KUB_NAME_SPACE}",
        git_cache_config=gitcache,
        afHostPortPair=HostPortPair(f"airflow-service.{KUB_NAME_SPACE}.svc.cluster.local", 8080),
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
        merge_datacontainer=merge_datacontainer,
        pv_storage_class="longhorn",
        dataPlatforms=[
            YellowDataPlatform(
                name="YellowLive",
                doc=PlainTextDocumentation("Live Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD1
                ),
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
    pspSQLServer: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "Test_DP_SQLServer",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("Test"),
        yp_assembly=yp_assmSQLServer,
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        connectCredentials=Credential("connect", CredentialType.API_TOKEN),
        mergeRW_Credential=Credential("sa", CredentialType.USER_PASSWORD),
        merge_datacontainer=merge_sqlserver_datacontainer,
        pv_storage_class="longhorn",
        dataPlatforms=[
            YellowDataPlatform(
                name="YellowLiveSQLServer",
                doc=PlainTextDocumentation("Live Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD1
                ),
            YellowDataPlatform(
                "YellowForensicSQLServer",
                doc=PlainTextDocumentation("Forensic Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
                ),
            YellowDataPlatform(
                "YellowRemoteForensicSQLServer",
                doc=PlainTextDocumentation("Forensic remote Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
                )
        ]
    )
    pspOracle: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "Test_DP_Oracle",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("Test"),
        yp_assembly=yp_assmOracle,
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        connectCredentials=Credential("connect", CredentialType.API_TOKEN),
        mergeRW_Credential=Credential("system", CredentialType.USER_PASSWORD),
        merge_datacontainer=merge_oracle_datacontainer,
        pv_storage_class="longhorn",
        dataPlatforms=[
            YellowDataPlatform(
                name="YellowLiveOracle",
                doc=PlainTextDocumentation("Live Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD1
                ),
            YellowDataPlatform(
                "YellowForensicOracle",
                doc=PlainTextDocumentation("Forensic Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
                ),
            YellowDataPlatform(
                "YellowRemoteForensicOracle",
                doc=PlainTextDocumentation("Forensic remote Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
                )
        ]
    )
    pspDB2: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "Test_DP_DB2",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("Test"),
        yp_assembly=yp_assmDB2,
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        connectCredentials=Credential("connect", CredentialType.API_TOKEN),
        mergeRW_Credential=Credential("system", CredentialType.USER_PASSWORD),
        merge_datacontainer=merge_db2_datacontainer,
        pv_storage_class="longhorn",
        dataPlatforms=[
            YellowDataPlatform(
                name="YellowLiveDB2",
                doc=PlainTextDocumentation("Live Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD1
                ),
            YellowDataPlatform(
                "YellowForensicDB2",
                doc=PlainTextDocumentation("Forensic Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
                ),
            YellowDataPlatform(
                "YellowRemoteForensicDB2",
                doc=PlainTextDocumentation("Forensic remote Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
                )
        ]
    )
    pspSnowflake: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "Test_DP_Snowflake",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("Test"),
        yp_assembly=yp_assmSnowflake,
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        connectCredentials=Credential("connect", CredentialType.API_TOKEN),
        mergeRW_Credential=Credential("system", CredentialType.USER_PASSWORD),
        merge_datacontainer=merge_snowflake_datacontainer,
        pv_storage_class="longhorn",
        dataPlatforms=[
            YellowDataPlatform(
                name="YellowLiveSnowflake",
                doc=PlainTextDocumentation("Live Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD1
                ),
            YellowDataPlatform(
                "YellowForensicSnowflake",
                doc=PlainTextDocumentation("Forensic Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
                ),
            YellowDataPlatform(
                "YellowRemoteForensicSnowflake",
                doc=PlainTextDocumentation("Forensic remote Yellow DataPlatform"),
                milestoneStrategy=YellowMilestoneStrategy.SCD2
                )
        ]
    )

    ecosys: Ecosystem = Ecosystem(
        name="WorldWideImporters",
        repo=GitHubRepository("billynewport/mvpmodel", "main"),
        platform_services_providers=[psp, pspSQLServer, pspOracle, pspDB2, pspSnowflake],
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
