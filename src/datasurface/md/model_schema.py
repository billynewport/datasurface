from datasurface.md.governance import DataPlatformManagedDataContainer, DataTransformer, Team, GovernanceZone, GovernanceZoneDeclaration, TeamDeclaration
from datasurface.md import Ecosystem, sqlalchemyutils
from datasurface.md.repo import Repository
from datasurface.md.governance import Datastore, Dataset
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import VarChar
from datasurface.md.governance import Workspace, DatasetGroup, DatasetSink
from datasurface.md.governance import DatasurfaceTransformerCodeArtifact, DatasurfaceTransformerType
from datasurface.platforms.yellow.transformer_context import DataTransformerContext
from sqlalchemy.engine import Connection
import sqlalchemy
from sqlalchemy import Table, text
from typing import Any, Dict, List


def addDatasurfaceModel(eco: Ecosystem, repo: Repository) -> None:
    # This creates a governance zone called "datasurface_gz" with a team called "datasurface_team"

    if eco.getZone("datasurface_gz") is None:
        eco.add(GovernanceZoneDeclaration("datasurface_gz", repo))
    gz: GovernanceZone = eco.getZoneOrThrow("datasurface_gz")
    if gz.getTeam("datasurface_team") is None:
        gz.add(TeamDeclaration("datasurface_team", repo))

    team: Team = gz.getTeamOrThrow("datasurface_team")
    team.add(
        Workspace(
            "Datasurface_ModelExternalization",
            DatasetGroup(
                "DSG",
                sinks=[
                    DatasetSink("Datasurface", "Ecosystem"),
                    DatasetSink("Datasurface", "GovernanceZone"),
                    DatasetSink("Datasurface", "Team"),
                    DatasetSink("Datasurface", "Datastore"),
                    DatasetSink("Datasurface", "Dataset"),
                    DatasetSink("Datasurface", "Workspace"),
                    DatasetSink("Datasurface", "DatasetGroup"),
                    DatasetSink("Datasurface", "DatasetSink"),
                    DatasetSink("Datasurface", "DataTransformer"),
                    DatasetSink("Datasurface", "PSP"),
                    DatasetSink("Datasurface", "DataPlatform"),
                    DatasetSink("Datasurface", "DSGPlatformAssignment"),
                    DatasetSink("Datasurface", "PrimaryIngestionPlatform"),
                    DatasetSink("Datasurface", "InfrastructureVendor"),
                    DatasetSink("Datasurface", "InfrastructureLocation")
                ]),
            DataTransformer(
                name="ExternalizeModel",
                store=Datastore(
                    "Datasurface",
                    datasets=[
                        Dataset("Ecosystem",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("repo", VarChar(255), nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("liveRepo", VarChar(255), nullable=NullableStatus.NOT_NULLABLE),
                                    ]
                                )),
                        Dataset("GovernanceZone",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("gzName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("repo", VarChar(255), nullable=NullableStatus.NOT_NULLABLE),
                                    ]
                                )),
                        Dataset("Team",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("gzName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("teamName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("repo", VarChar(255), nullable=NullableStatus.NOT_NULLABLE),
                                    ]
                                )),
                        Dataset("Datastore",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("gzName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("teamName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("datastoreName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("productionStatus", VarChar(255)),
                                        DDLColumn("deprecationStatus", VarChar(255)),
                                    ]
                                )),
                        Dataset("Dataset",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("gzName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("teamName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("datastoreName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("datasetName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("deprecationStatus", VarChar(255)),
                                    ]
                                )),
                        Dataset("Workspace",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("gzName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("teamName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("workspaceName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                    ]
                                )),
                        Dataset("DatasetGroup",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("gzName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("teamName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("workspaceName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("dsgName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                    ]
                                )),
                        Dataset("DatasetSink",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("gzName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("teamName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("workspaceName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("dsgName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("datastoreName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("datasetName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                    ]
                                )),
                        Dataset("DataTransformer",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("gzName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("teamName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("workspaceName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("dataTransformerName", VarChar(255), nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("trigger_type", VarChar(255)),
                                    ]
                                )),
                        Dataset("PSP",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("pspName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                    ]
                                )),
                        Dataset("DataPlatform",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("pspName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("dataPlatformName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                    ]
                                )),
                        Dataset("DSGPlatformAssignment",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("workspaceName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("dsgName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("dataPlatform", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("productionStatus", VarChar(255)),
                                        DDLColumn("deprecationsAllowed", VarChar(255)),
                                        DDLColumn("status", VarChar(255)),
                                    ]
                                )),
                        Dataset("PrimaryIngestionPlatform",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("datastoreName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("platformName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                    ]
                                )),
                        Dataset("InfrastructureVendor",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("vendorName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("cloudVendor", VarChar(255)),
                                    ]
                                )),
                        Dataset("InfrastructureLocation",
                                schema=DDLTable(
                                    columns=[
                                        DDLColumn("ecoName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("vendorName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("locationName", VarChar(255), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                                        DDLColumn("parentLocationName", VarChar(255), nullable=NullableStatus.NULLABLE),
                                    ]
                                )),
                    ]),
                code=DatasurfaceTransformerCodeArtifact(
                    type=DatasurfaceTransformerType.EXTERNALIZE_MODEL
                ))),
            DataPlatformManagedDataContainer("Datasurface_ModelExternalization container")
        )


class RecordCollector:
    def __init__(self, dataset: Dataset, conn: Connection) -> None:
        self.dataset: Dataset = dataset
        self.conn: Connection = conn
        self.records: List[Dict[str, Any]] = []
        self.table: Table = sqlalchemyutils.datasetToSQLAlchemyTable(dataset, dataset.name, sqlalchemy.MetaData(), conn)

    def insert(self, record: Dict[str, Any]) -> None:
        self.records.append(record)

    def execute_batch_inserts(self, batch_size: int) -> None:
        if self.records:  # Only execute if there are values to insert
            # Get column names from the first record
            columns = list(self.records[0].keys())
            quoted_columns = [f'"{col}"' for col in columns]
            placeholders = ", ".join([f":{i}" for i in range(len(columns))])
            insert_sql = f"INSERT INTO {self.table.name} ({', '.join(quoted_columns)}) VALUES ({placeholders})"

            for i in range(0, len(self.records), batch_size):
                batch_values = self.records[i:i + batch_size]
                # Convert dict values to list format with positional parameters for executemany
                all_params = [{str(j): list(record.values())[j] for j in range(len(columns))} for record in batch_values]
                self.conn.execute(text(insert_sql), all_params)


def executeModelExternalizer(conn: Connection, context: DataTransformerContext) -> None:
    """
    This function should write a snapshot of the context.getEco() model to the output
    datasets. Output tables are empty when starting."""
    eco: Ecosystem = context.getEcosystem()
    # Write eco system model
    modelStore: Datastore = eco.cache_getDatastoreOrThrow("Datasurface").datastore

    # Create lists for each table's insert data
    # We add rows to these for batch insertion later
    ecoT: RecordCollector = RecordCollector(modelStore.datasets["Ecosystem"], conn)
    gzT: RecordCollector = RecordCollector(modelStore.datasets["GovernanceZone"], conn)
    teamT: RecordCollector = RecordCollector(modelStore.datasets["Team"], conn)
    storeT: RecordCollector = RecordCollector(modelStore.datasets["Datastore"], conn)
    datasetT: RecordCollector = RecordCollector(modelStore.datasets["Dataset"], conn)
    workspaceT: RecordCollector = RecordCollector(modelStore.datasets["Workspace"], conn)
    dsgT: RecordCollector = RecordCollector(modelStore.datasets["DatasetGroup"], conn)
    dskT: RecordCollector = RecordCollector(modelStore.datasets["DatasetSink"], conn)
    dtT: RecordCollector = RecordCollector(modelStore.datasets["DataTransformer"], conn)
    pspT: RecordCollector = RecordCollector(modelStore.datasets["PSP"], conn)
    dpT: RecordCollector = RecordCollector(modelStore.datasets["DataPlatform"], conn)
    dsgAssignT: RecordCollector = RecordCollector(modelStore.datasets["DSGPlatformAssignment"], conn)
    pipT: RecordCollector = RecordCollector(modelStore.datasets["PrimaryIngestionPlatform"], conn)
    vendorT: RecordCollector = RecordCollector(modelStore.datasets["InfrastructureVendor"], conn)
    infraLocT: RecordCollector = RecordCollector(modelStore.datasets["InfrastructureLocation"], conn)

    # Collect ecosystem data
    ecoT.insert({"ecoName": eco.name, "repo": str(eco.owningRepo), "liveRepo": str(eco.liveRepo)})

    # Collect governance zone and related data
    for gz in eco.zones.defineAllObjects():
        gzT.insert({"ecoName": eco.name, "gzName": gz.name, "repo": str(gz.owningRepo)})
        for team in gz.teams.defineAllObjects():
            teamT.insert({"ecoName": eco.name, "gzName": gz.name, "teamName": team.name, "repo": str(team.owningRepo)})
            for datastore in team.dataStores.values():
                storeT.insert({
                    "ecoName": eco.name, "gzName": gz.name, "teamName": team.name, "datastoreName": datastore.name,
                    "productionStatus": datastore.productionStatus, "deprecationStatus": datastore.deprecationStatus
                })
                for dataset in datastore.datasets.values():
                    datasetT.insert({
                        "ecoName": eco.name, "gzName": gz.name, "teamName": team.name,
                        "datastoreName": datastore.name, "datasetName": dataset.name, "deprecationStatus": dataset.deprecationStatus
                    })
            for workspace in team.workspaces.values():
                workspaceT.insert({"ecoName": eco.name, "gzName": gz.name, "teamName": team.name, "workspaceName": workspace.name})
                for datasetGroup in workspace.dsgs.values():
                    dsgT.insert({
                        "ecoName": eco.name, "gzName": gz.name, "teamName": team.name,
                        "workspaceName": workspace.name, "dsgName": datasetGroup.name
                    })
                    for datasetSink in datasetGroup.sinks.values():
                        dskT.insert({
                            "ecoName": eco.name, "gzName": gz.name, "teamName": team.name, "workspaceName": workspace.name,
                            "dsgName": datasetGroup.name, "datastoreName": datasetSink.storeName, "datasetName": datasetSink.datasetName
                        })
                if workspace.dataTransformer is not None:
                    dtT.insert({
                        "ecoName": eco.name, "gzName": gz.name, "teamName": team.name, "workspaceName": workspace.name,
                        "dataTransformerName": workspace.dataTransformer.name, "trigger": workspace.dataTransformer.trigger
                    })

    # Collect PSP and platform data
    for psp in eco.platformServicesProviders:
        pspT.insert({"ecoName": eco.name, "pspName": psp.name})
        for dp in psp.dataPlatforms.values():
            dpT.insert({"ecoName": eco.name, "pspName": psp.name, "dataPlatformName": dp.name})

    # Collect platform assignment data
    for dsgPlatformAssignment in eco.dsgPlatformMappings.values():
        for assignment in dsgPlatformAssignment.assignments:
            dsgAssignT.insert({
                "ecoName": eco.name, "workspaceName": dsgPlatformAssignment.workspace, "dsgName": dsgPlatformAssignment.dsgName,
                "dataPlatform": assignment.dataPlatform, "productionStatus": assignment.productionStatus,
                "deprecationsAllowed": assignment.deprecationsAllowed, "status": assignment.status
            })

    # Collect ingestion platform data
    for primaryIngestionPlatform in eco.primaryIngestionPlatforms.values():
        for platformKey in primaryIngestionPlatform.dataPlatforms:
            pipT.insert(
                {
                    "ecoName": eco.name,
                    "datastoreName": primaryIngestionPlatform.storeName,
                    "platformName": platformKey.name
                })

    # Collect infrastructure data
    for infrastructureVendor in eco.vendors.values():
        vendorT.insert(
            {
                "ecoName": eco.name,
                "vendorName": infrastructureVendor.name,
                "cloudVendor": infrastructureVendor.hardCloudVendor.name if infrastructureVendor.hardCloudVendor else None
            })
        for location in infrastructureVendor.locations.values():
            infraLocT.insert({
                "ecoName": eco.name, "vendorName": infrastructureVendor.name, "locationName": location.name,
                "parentLocationName": location.setParentLocation
            })

    # Execute batched inserts for each table using executemany pattern
    batch_size: int = 10000

    ecoT.execute_batch_inserts(batch_size)
    gzT.execute_batch_inserts(batch_size)
    teamT.execute_batch_inserts(batch_size)
    storeT.execute_batch_inserts(batch_size)
    datasetT.execute_batch_inserts(batch_size)
    workspaceT.execute_batch_inserts(batch_size)
    dsgT.execute_batch_inserts(batch_size)
    dskT.execute_batch_inserts(batch_size)
    dtT.execute_batch_inserts(batch_size)
    pspT.execute_batch_inserts(batch_size)
    dpT.execute_batch_inserts(batch_size)
    dsgAssignT.execute_batch_inserts(batch_size)
    pipT.execute_batch_inserts(batch_size)
    vendorT.execute_batch_inserts(batch_size)
    infraLocT.execute_batch_inserts(batch_size)
