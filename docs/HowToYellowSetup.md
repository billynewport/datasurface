# YellowDataPlatform Ecosystem Model Setup

This guide shows you how to create a DataSurface ecosystem model using the YellowDataPlatform with the modern PSP (Platform Service Provider) and assembly pattern.

For complete deployment instructions including Kubernetes setup, secrets management, and troubleshooting, see the [YellowDataPlatform Environment Setup Guide](yellow_dp/HOWTO_Setup_YellowDataPlatform_Environment.md).

## Overview

The YellowDataPlatform uses a structured approach with:
- **Platform Service Provider (PSP)**: Manages platform configuration and assembly
- **Assembly Configuration**: Defines Kubernetes resources, storage, and networking
- **Dual Platforms**: Live (SCD1) and Forensic (SCD2) data processing modes

## Create Your Ecosystem Model

### Step 1: Create Project Structure

Create a directory structure for your ecosystem model:

```bash
mkdir -p src/tests/my-ecosystem
```

### Step 2: Define the Ecosystem Model

Create `src/tests/my-ecosystem/eco.py`:

```python
from datasurface.md import Team, GovernanceZoneDeclaration, GovernanceZone, InfrastructureVendor, InfrastructureLocation, TeamDeclaration
from datasurface.md import Ecosystem, LocationKey, StorageRequirement
from datasurface.md.credential import Credential, CredentialType
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.platforms.yellow import YellowDataPlatform, YellowMilestoneStrategy, YellowPlatformServiceProvider, K8sResourceLimits
from datasurface.md import CloudVendor, WorkspacePlatformConfig
from datasurface.md import ValidationTree
from datasurface.md.governance import Datastore, Dataset, SQLSnapshotIngestion, HostPortPair, CronTrigger, IngestionConsistencyType, \
    ConsumerRetentionRequirements, DataMilestoningStrategy, DataLatency
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import VarChar, Date
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md import Workspace, DatasetSink, DatasetGroup, PostgresDatabase, DataPlatformManagedDataContainer
from datasurface.platforms.yellow.assembly import YellowSingleDatabaseAssembly, GitCacheConfig
from typing import Any, Optional
from datetime import timedelta

# Configuration constants
KUB_NAME_SPACE: str = "my-kub-namespace"  # Your Kubernetes namespace
GH_REPO_OWNER: str = "your-username"  # Your GitHub username
GH_REPO_NAME: str = "your-repo"  # Your GitHub repository name


def createPSP() -> YellowPlatformServiceProvider:
    """Create the Platform Service Provider with assembly configuration."""
    
    # Kubernetes merge database configuration
    k8s_merge_datacontainer: PostgresDatabase = PostgresDatabase(
        "K8sMergeDB",  # Container name for Kubernetes deployment
        hostPort=HostPortPair(f"pg-data.{KUB_NAME_SPACE}.svc.cluster.local", 5432),
        locations={LocationKey("MyCorp:USA/NY_1")},  # Kubernetes cluster location
        databaseName="datasurface_merge"  # The merge database name
    )

    # Git cache configuration for shared repository access
    gitcache: GitCacheConfig = GitCacheConfig(
        enabled=True,
        storageClass="longhorn",  # Use appropriate storage class for your cluster
    )

    # Yellow platform assembly configuration
    yp_assm: YellowSingleDatabaseAssembly = YellowSingleDatabaseAssembly(
        name="MyKubPlatform",
        namespace=f"{KUB_NAME_SPACE}",
        git_cache_config=gitcache,
        nfs_server_node="git-cache-server",
        afHostPortPair=HostPortPair(f"airflow-service.{KUB_NAME_SPACE}.svc.cluster.local", 8080),
        pgStorageNeeds=StorageRequirement("10G"),
        pgResourceLimits=K8sResourceLimits(
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
    
    # Platform Service Provider with both Live and Forensic platforms
    psp: YellowPlatformServiceProvider = YellowPlatformServiceProvider(
        "MyKubPlatform",
        {LocationKey("MyCorp:USA/NY_1")},
        PlainTextDocumentation("YellowDataPlatform Service Provider"),
        yp_assembly=yp_assm,
        gitCredential=Credential("git", CredentialType.API_TOKEN),
        connectCredentials=Credential("connect", CredentialType.API_TOKEN),
        postgresCredential=Credential("postgres", CredentialType.USER_PASSWORD),
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
    """Create the ecosystem for the YellowDataPlatform."""
    
    git: Credential = Credential("git", CredentialType.API_TOKEN)

    ecosys: Ecosystem = Ecosystem(
        name="MyKubEcosystem",
        repo=GitHubRepository(f"{GH_REPO_OWNER}/{GH_REPO_NAME}", "main", credential=git),
        platform_services_providers=[createPSP()],
        governance_zone_declarations=[
            GovernanceZoneDeclaration("USA", GitHubRepository(f"{GH_REPO_OWNER}/{GH_REPO_NAME}", "gzmain"))
        ],
        infrastructure_vendors=[
            # Private data centers
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
    
    # Add a sample datastore with PostgreSQL source
    team.add(
        Datastore(
            "SampleStore",
            documentation=PlainTextDocumentation("Sample datastore for testing"),
            capture_metadata=SQLSnapshotIngestion(
                PostgresDatabase(
                    "SampleDB",  # Model name for database
                    hostPort=HostPortPair("your-db-host.com", 5432),  # Your database host
                    locations={LocationKey("MyCorp:USA/NY_1")},  # Database location
                    databaseName="sample_db"  # Database name
                ),
                CronTrigger("Sample_Data Every 10 mins", "*/10 * * * *"),  # Cron trigger for ingestion
                IngestionConsistencyType.MULTI_DATASET,  # Ingestion consistency type
                Credential("sample_db_creds", CredentialType.USER_PASSWORD)  # Database credentials
            ),
            datasets=[
                Dataset(
                    "customers",
                    schema=DDLTable(
                        DDLColumn("customer_id", VarChar(10), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                        DDLColumn("company_name", VarChar(100), NullableStatus.NOT_NULLABLE),
                        DDLColumn("contact_name", VarChar(50)),
                        DDLColumn("email", VarChar(100)),
                        DDLColumn("created_date", Date())
                    ),
                    classification=SimpleDC(SimpleDCTypes.PC2),  # Personal data classification
                    documentation=PlainTextDocumentation("Customer information")
                )
            ]
        )
    )

    # Add a sample workspace
    retention_req = ConsumerRetentionRequirements(
        DataMilestoningStrategy.LIVE_ONLY,
        DataLatency.MINUTES,
        "Sample retention policy",
        timedelta(days=365)
    )
    chooser = WorkspacePlatformConfig(retention_req)
    
    team.add(
        Workspace(
            "SampleWorkspace",
            documentation=PlainTextDocumentation("Sample workspace for data consumers"),
            datasetGroups=[
                DatasetGroup(
                    "CustomerData",
                    documentation=PlainTextDocumentation("Customer data group"),
                    dataContainer=DataPlatformManagedDataContainer(),
                    sinks=[
                        DatasetSink("customers", "SampleStore")
                    ],
                    platformConfig=chooser
                )
            ]
        )
    )

    # Validate the ecosystem
    tree: ValidationTree = ecosys.lintAndHydrateCaches()
    if tree.hasErrors():
        tree.printTree()
        raise Exception("Ecosystem validation failed")
    
    return ecosys
```

### Step 3: Validate Your Ecosystem

```bash
# Test your ecosystem model
python -c "
import sys
sys.path.append('src/tests/my-ecosystem')
from eco import createEcosystem
eco = createEcosystem()
print('Ecosystem created successfully:', eco.name)
print('Platform Service Providers:', len(eco.platform_services_providers))
print('Data Platforms:', [dp.name for psp in eco.platform_services_providers for dp in psp.dataPlatforms])
"
```

### Step 4: Generate Kubernetes Artifacts

```bash
# Generate the Kubernetes YAML and Airflow DAG
python -m datasurface.cmd.platform generatePlatformBootstrap \
    --ringLevel 0 \
    --model src/tests/my-ecosystem \
    --output src/tests/my-ecosystem/output \
    --psp MyKubPlatform
```

This will create in `src/tests/my-ecosystem/output/MyKubPlatform/`:

- `kubernetes-bootstrap.yaml` - Complete Kubernetes deployment
- `mykubplatform_infrastructure_dag.py` - Airflow DAG for infrastructure management
- `mykubplatform_model_merge_job.yaml` - Model merge job for populating ingestion configurations
- `mykubplatform_ring1_init_job.yaml` - Ring 1 initialization job for database schemas
- `mykubplatform_reconcile_views_job.yaml` - Workspace views reconciliation job

```bash
# Check the generated files
ls -la src/tests/my-ecosystem/output/MyKubPlatform/
```

## Next Steps: Deploy to Kubernetes

Once you have generated your ecosystem artifacts, you can deploy them to a Kubernetes cluster. For complete deployment instructions including:

- Kubernetes cluster setup and configuration
- Secrets management and credential configuration  
- Database initialization and Ring 1 setup
- Airflow DAG deployment and management
- Troubleshooting and monitoring

See the comprehensive [YellowDataPlatform Environment Setup Guide](yellow_dp/HOWTO_Setup_YellowDataPlatform_Environment.md).

## Key Concepts Explained

### Platform Service Provider (PSP)
The PSP manages the overall platform configuration and contains:
- **Assembly Configuration**: Kubernetes resource specifications
- **Credential Management**: Database, Git, and API credentials
- **Multiple Data Platforms**: Live and Forensic processing modes
- **Storage Configuration**: Persistent volumes and storage classes

### Assembly Configuration
The `YellowSingleDatabaseAssembly` defines:
- **Resource Limits**: CPU and memory for each component
- **Storage Requirements**: Persistent volume sizes
- **Network Configuration**: Service endpoints and port mappings
- **Git Cache**: Shared repository access configuration

### Dual Platform Architecture
- **YellowLive (SCD1)**: Current state data processing
- **YellowForensic (SCD2)**: Historical data with full audit trail

### Data Flow
1. **Datastore**: Defines source data and ingestion metadata
2. **Workspace**: Defines consumer access patterns
3. **Platform**: Processes and transforms data according to requirements
4. **Views**: Provides consumer-friendly data access interfaces
