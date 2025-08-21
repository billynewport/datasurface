"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1

This is a starter datasurface repository. It defines a simple Ecosystem using YellowDataPlatform with Live and Forensic modes.
It will generate 2 pipelines, one with live records only and the other with full milestoning.
"""

from datasurface.md import Team, GovernanceZoneDeclaration, GovernanceZone, InfrastructureVendor, InfrastructureLocation, \
    TeamDeclaration, DataTransformer
from datasurface.md import Ecosystem, LocationKey, DataPlatformManagedDataContainer
from datasurface.md.credential import Credential, CredentialType
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.md import StorageRequirement
from datasurface.platforms.yellow import YellowDataPlatform, YellowMilestoneStrategy, YellowPlatformServiceProvider, K8sResourceLimits
from datasurface.md import CloudVendor, WorkspacePlatformConfig
from datasurface.md import ValidationTree
from datasurface.md.governance import Datastore, Dataset, SQLSnapshotIngestion, HostPortPair, CronTrigger, IngestionConsistencyType, \
    ConsumerRetentionRequirements, DataMilestoningStrategy, DataLatency
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import VarChar, Date
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md import Workspace, DatasetSink, DatasetGroup, PostgresDatabase, DataPlatform, EcosystemPipelineGraph, PlatformPipelineGraph
from datasurface.md.codeartifact import PythonRepoCodeArtifact
from typing import Any, Optional
from datasurface.platforms.yellow.assembly import YellowSinglePostgresDatabaseAssembly, GitCacheConfig

KUB_NAME_SPACE: str = "ns-yellow-starter"  # This is the namespace you want to use for your kubernetes environment
GH_REPO_OWNER: str = "billynewport"  # Change to your github username
GH_REPO_NAME: str = "yellow_starter"  # Change to your github repository name containing this project
GH_DT_REPO_NAME: str = "yellow_starter"  # For now, we use the same repo for the transformer


def createPSP() -> YellowPlatformServiceProvider:
    # Kubernetes merge database configuration
    k8s_merge_datacontainer: PostgresDatabase = PostgresDatabase(
        "K8sMergeDB",  # Container name for Kubernetes deployment
        hostPort=HostPortPair(f"pg-data.{KUB_NAME_SPACE}.svc.cluster.local", 5432),
        locations={LocationKey("MyCorp:USA/NY_1")},  # Kubernetes cluster location
        databaseName="datasurface_merge"  # The database we created
    )

    gitcache: GitCacheConfig = GitCacheConfig(
        enabled=True,
        storageClass="longhorn",
    )

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
        merge_datacontainer=k8s_merge_datacontainer,
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
                )
        ]
    )
    return psp


def createEcosystem() -> Ecosystem:
    """This is a very simple test model with a single datastore and dataset.
    It is used to test the YellowDataPlatform."""

    git: Credential = Credential("git", CredentialType.API_TOKEN)

    ecosys: Ecosystem = Ecosystem(
        name="YellowStarter",
        repo=GitHubRepository(f"{GH_REPO_OWNER}/{GH_REPO_NAME}", "main_edit", credential=git),
        platform_services_providers=[createPSP()],
        governance_zone_declarations=[
            GovernanceZoneDeclaration("USA", GitHubRepository(f"{GH_REPO_OWNER}/{GH_REPO_NAME}", "gzmain"))
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
        liveRepo=GitHubRepository(f"{GH_REPO_OWNER}/{GH_REPO_NAME}", "main", credential=git)
    )
    gz: GovernanceZone = ecosys.getZoneOrThrow("USA")

    # Add a team to the governance zone
    gz.add(TeamDeclaration(
        "team1",
        GitHubRepository(f"{GH_REPO_OWNER}/{GH_REPO_NAME}", "team1", credential=git)
        ))

    team: Team = gz.getTeamOrThrow("team1")
    team.add(
        Datastore(
            "Store1",
            documentation=PlainTextDocumentation("Test datastore"),
            capture_metadata=SQLSnapshotIngestion(
                PostgresDatabase(
                    "CustomerDB",  # Model name for database
                    hostPort=HostPortPair(f"pg-data.{KUB_NAME_SPACE}.svc.cluster.local", 5432),
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Locations for database
                    databaseName="customer_db"  # Database name
                ),
                CronTrigger("Every 5 minute", "*/1 * * * *"),  # Cron trigger for ingestion
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
                            DDLColumn("customerid", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("streetname", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("city", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("state", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("zipcode", VarChar(30), nullable=NullableStatus.NOT_NULLABLE)
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
                    DatasetSink("Store1", "addresses"),
                    DatasetSink("MaskedCustomers", "customers")
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
                    DatasetSink("Store1", "addresses"),
                    DatasetSink("MaskedCustomers", "customers")
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
            "MaskedStoreGenerator",
            DataPlatformManagedDataContainer("MaskedStoreGenerator container"),
            DatasetGroup(
                "Original",
                sinks=[DatasetSink("Store1", "customers")]
            ),
            DataTransformer(
                name="MaskedCustomerGenerator",
                code=PythonRepoCodeArtifact(GitHubRepository(f"{GH_REPO_OWNER}/{GH_DT_REPO_NAME}", "main", credential=git), "main"),
                trigger=CronTrigger("Every 1 minute", "*/1 * * * *"),
                store=Datastore(
                    name="MaskedCustomers",
                    documentation=PlainTextDocumentation("MaskedCustomers datastore"),
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
                            classifications=[SimpleDC(SimpleDCTypes.PUB, "Customer")]
                        )
                    ]
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
    live_dp: DataPlatform[Any] = ecosys.getDataPlatformOrThrow("YellowLive")
    assert live_dp is not None
    forensic_dp: DataPlatform[Any] = ecosys.getDataPlatformOrThrow("YellowForensic")
    assert forensic_dp is not None
    graph: EcosystemPipelineGraph = ecosys.getGraph()
    assert graph is not None
    live_root: Optional[PlatformPipelineGraph] = graph.roots.get(live_dp.name)
    if live_root is None:
        print(f"Unknown graph for platform: {live_dp.name}")
    forensic_root: Optional[PlatformPipelineGraph] = graph.roots.get(forensic_dp.name)
    if forensic_root is None:
        print(f"Unknown graph for platform: {forensic_dp.name}")
    else:
        print(f"Forensic graph for platform: {forensic_dp.name}")
        print(forensic_root)


if __name__ == "__main__":
    test_Validate()
