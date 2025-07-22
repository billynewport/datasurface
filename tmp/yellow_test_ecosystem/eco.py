"""
Test Ecosystem Model for YellowDataPlatform DAG Generation
"""

from datasurface.md import *
from datasurface.md.governance import *
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform, YellowMilestoneStrategy

def createEcosystem() -> Ecosystem:
    eco = Ecosystem("TestEcosystem")
    
    # Add GitHub repository for the ecosystem
    eco.setOwningRepository(GitHubRepository("billynewport/test-ecosystem", "main"))
    
    # Create locations
    us_east = USEast1()
    eco.addLocation(us_east)
    
    # Create a governance zone
    gz = GovernanceZone("TestOrg", us_east)
    eco.addGovernanceZone(gz)
    
    # Create team
    team = Team("DataTeam", "Data Engineering Team")
    gz.addTeam(team)
    
    # Create credentials
    postgres_cred = Credential("postgres_cred", CredentialType.USER_PASSWORD)
    git_cred = Credential("git_token", CredentialType.API_TOKEN)  
    kafka_cred = Credential("kafka_token", CredentialType.API_TOKEN)
    slack_cred = Credential("slack_token", CredentialType.API_TOKEN)
    source_db_cred = Credential("source_db_cred", CredentialType.USER_PASSWORD)
    
    eco.addCredential(postgres_cred)
    eco.addCredential(git_cred)
    eco.addCredential(kafka_cred)
    eco.addCredential(slack_cred)
    eco.addCredential(source_db_cred)
    
    # Create data containers
    merge_db = PostgresDatabase(
        "merge_db",
        {us_east},
        Documentation("Merge database"),
        HostPortPair("postgres-service", 5432),
        "datasurface_merge"
    )
    
    source_db = PostgresDatabase(
        "source_db", 
        {us_east},
        Documentation("Source database"),
        HostPortPair("source-postgres", 5432),
        "northwind"
    )
    
    eco.addDataContainer(merge_db)
    eco.addDataContainer(source_db)
    
    # Create yellow data platforms
    yellow_live = YellowDataPlatform(
        name="YellowLive",
        locs={us_east},
        doc=Documentation("Live Yellow Data Platform"),
        namespace="yellow-live",
        connectCredentials=kafka_cred,
        postgresCredential=postgres_cred,
        gitCredential=git_cred,
        slackCredential=slack_cred,
        merge_datacontainer=merge_db,
        milestoneStrategy=YellowMilestoneStrategy.LIVE_ONLY
    )
    
    yellow_forensic = YellowDataPlatform(
        name="YellowForensic", 
        locs={us_east},
        doc=Documentation("Forensic Yellow Data Platform"),
        namespace="yellow-forensic",
        connectCredentials=kafka_cred,
        postgresCredential=postgres_cred,
        gitCredential=git_cred,
        slackCredential=slack_cred,
        merge_datacontainer=merge_db,
        milestoneStrategy=YellowMilestoneStrategy.BATCH_MILESTONED
    )
    
    eco.addDataPlatform(yellow_live)
    eco.addDataPlatform(yellow_forensic)
    
    # Create test datasets and schema
    customer_schema = DDLTable("customers")
    customer_schema.addColumn(DDLColumn("id", Integer(), False))
    customer_schema.addColumn(DDLColumn("name", String(100), False))
    customer_schema.addColumn(DDLColumn("email", String(255), True))
    customer_schema.setPrimaryKey(PrimaryKeyList(["id"]))
    
    customer_dataset = Dataset("customers", customer_schema, Documentation("Customer data"))
    
    # Create datastore with SQL snapshot ingestion
    customer_store = Datastore("CustomerData", Documentation("Customer datastore"))
    customer_store.addDataset(customer_dataset)
    
    # Add SQL snapshot ingestion command with cron trigger
    sql_ingestion = SQLSnapshotIngestion(
        CronTrigger("hourly", "0 * * * *"),  # Run every hour
        source_db_cred,
        source_db,
        IngestionConsistencyType.MULTI_DATASET
    )
    customer_store.setCmd(sql_ingestion)
    
    team.addDatastore(customer_store)
    
    # Create workspace and dataset groups
    workspace = Workspace("TestWorkspace", Documentation("Test workspace"))
    workspace.setDataContainer(DataPlatformManagedDataContainer())
    
    # Live dataset group
    live_dsg = DatasetGroup("LiveDSG", Documentation("Live data"))
    live_dsg.addDatasetSink(DatasetSink("CustomerData", "customers"))
    live_dsg.setPlatformMD(WorkspacePlatformConfig(
        retention=DataRetentionPolicy(DataMilestoningStrategy.LIVE_ONLY)
    ))
    workspace.addDSG(live_dsg)
    
    # Forensic dataset group  
    forensic_dsg = DatasetGroup("ForensicDSG", Documentation("Forensic data"))
    forensic_dsg.addDatasetSink(DatasetSink("CustomerData", "customers"))
    forensic_dsg.setPlatformMD(WorkspacePlatformConfig(
        retention=DataRetentionPolicy(DataMilestoningStrategy.FORENSIC)
    ))
    workspace.addDSG(forensic_dsg)
    
    # Add DataTransformer with scheduled trigger
    output_store = Datastore("MaskedCustomers", Documentation("Masked customer data"))
    masked_schema = DDLTable("masked_customers")
    masked_schema.addColumn(DDLColumn("id", Integer(), False))
    masked_schema.addColumn(DDLColumn("name_hash", String(64), False))
    masked_schema.addColumn(DDLColumn("domain", String(100), True))
    masked_schema.setPrimaryKey(PrimaryKeyList(["id"]))
    
    masked_dataset = Dataset("masked_customers", masked_schema, Documentation("Masked customer data"))
    output_store.addDataset(masked_dataset)
    output_store.setCmd(DataTransformerOutput("TestWorkspace"))
    team.addDatastore(output_store)
    
    # DataTransformer with scheduled trigger (CronTrigger)
    code_artifact = PythonRepoCodeArtifact(GitHubRepository("billynewport/customer-masker", "main"))
    transformer = DataTransformer(
        "CustomerMasker",
        output_store,
        code_artifact,
        Documentation("Masks customer PII data"),
        trigger=CronTrigger("daily", "0 2 * * *")  # Run daily at 2 AM
    )
    workspace.setDataTransformer(transformer)
    
    team.addWorkspace(workspace)
    
    return eco 