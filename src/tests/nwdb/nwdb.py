"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md import HostPortSQLDatabase
from datasurface.md.credential import Credential, CredentialType
from datasurface.md import CDCCaptureIngestion, CronTrigger, LocationKey, \
        Dataset, DatasetGroup, Datastore, Ecosystem, GovernanceZone, \
        IngestionConsistencyType, Team, \
        Workspace, HostPortPair, DataPlatform, DataPlatformKey, DataPlatformChooser, DatasetSink, DataTransformer, PythonCodeArtifact
from datasurface.md.governance import DSGDataPlatformAssignment, DatasetGroupDataPlatformAssignments, DatasetGroupDataPlatformMappingStatus, ProductionStatus
from datasurface.md.governance import DeprecationsAllowed
from datasurface.md import DataContainer

from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md.schema import DDLColumn, DDLTable, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import IEEE32, Date, Integer, SmallInt, VarChar, Variant


def defineTables(eco: Ecosystem, gz: GovernanceZone, t: Team):
    t.add(
        Datastore(
            "NW_Data",
            documentation=PlainTextDocumentation("NW_Data is a datastore that contains the Northwind database"),
            capture_metadata=CDCCaptureIngestion(
                HostPortSQLDatabase("NW_DB", {LocationKey("MyCorp:USA/NY_1")}, HostPortPair("hostName", 1344), "DBName"),
                CronTrigger("NW_Data Every 10 mins", "0,10,20,30,40,50 * * * *"),
                IngestionConsistencyType.MULTI_DATASET,
                Credential("eu_cred", CredentialType.USER_PASSWORD),
                ),
            datasets=[
                Dataset(
                    "us_states",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("state_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("state_name", VarChar(100)),
                            DDLColumn("state_abbr", VarChar(2)),
                            DDLColumn("state_region", VarChar(50))
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "customers",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("customer_id", VarChar(5), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("company_name", VarChar(40), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("contact_name", VarChar(30)),
                            DDLColumn("contact_title", VarChar(30)),
                            DDLColumn("address", VarChar(60)),
                            DDLColumn("city", VarChar(15)),
                            DDLColumn("region", VarChar(15)),
                            DDLColumn("postal_code", VarChar(10)),
                            DDLColumn("country", VarChar(15)),
                            DDLColumn("phone", VarChar(24)),
                            DDLColumn("fax", VarChar(24))
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PC3)],
                    documentation=PlainTextDocumentation("This data includes customer information from the Northwind database. It contains PII data.")
                ),
                Dataset(
                    "orders",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("order_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customer_id", VarChar(5)),
                            DDLColumn("employee_id", SmallInt()),
                            DDLColumn("order_date", Date()),
                            DDLColumn("required_date", Date()),
                            DDLColumn("shipped_date", Date()),
                            DDLColumn("ship_via", SmallInt()),
                            DDLColumn("freight", IEEE32()),
                            DDLColumn("ship_name", VarChar(40)),
                            DDLColumn("ship_address", VarChar(60)),
                            DDLColumn("ship_city", VarChar(15)),
                            DDLColumn("ship_region", VarChar(15)),
                            DDLColumn("ship_postal_code", VarChar(10)),
                            DDLColumn("ship_country", VarChar(15))
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "employees",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("employee_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("last_name", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("first_name", VarChar(10), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("title", VarChar(30)),
                            DDLColumn("title_of_courtesy", VarChar(25)),
                            DDLColumn("birth_date", Date()),
                            DDLColumn("hire_date", Date()),
                            DDLColumn("address", VarChar(60)),
                            DDLColumn("city", VarChar(15)),
                            DDLColumn("region", VarChar(15)),
                            DDLColumn("postal_code", VarChar(10)),
                            DDLColumn("country", VarChar(15)),
                            DDLColumn("home_phone", VarChar(24)),
                            DDLColumn("extension", VarChar(4)),
                            DDLColumn("photo", Variant()),
                            DDLColumn("notes", VarChar()),
                            DDLColumn("reports_to", SmallInt()),
                            DDLColumn("photo_path", VarChar(255))
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PC3)]
                ),
                Dataset(
                    "shippers",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("shipper_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("company_name", VarChar(40), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("phone", VarChar(24))
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "products",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("product_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("product_name", VarChar(40), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("supplier_id", SmallInt()),
                            DDLColumn("category_id", SmallInt()),
                            DDLColumn("quantity_per_unit", VarChar(20)),
                            DDLColumn("unit_price", IEEE32()),
                            DDLColumn("units_in_stock", SmallInt()),
                            DDLColumn("units_on_order", SmallInt()),
                            DDLColumn("reorder_level", SmallInt()),
                            DDLColumn("discontinued", Integer(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "categories",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("category_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("category_name", VarChar(15), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("description", VarChar()),
                            DDLColumn("picture", Variant())
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "suppliers",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("supplier_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("company_name", VarChar(40), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("contact_name", VarChar(30)),
                            DDLColumn("contact_title", VarChar(30)),
                            DDLColumn("address", VarChar(60)),
                            DDLColumn("city", VarChar(15)),
                            DDLColumn("region", VarChar(15)),
                            DDLColumn("postal_code", VarChar(10)),
                            DDLColumn("country", VarChar(15)),
                            DDLColumn("phone", VarChar(24)),
                            DDLColumn("fax", VarChar(24)),
                            DDLColumn("homepage", VarChar())
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "order_details",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("order_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("product_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("unit_price", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("quantity", SmallInt(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("discount", IEEE32(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "region",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("region_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("region_description", VarChar(60), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "territories",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("territory_id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("territory_description", VarChar(60), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("region_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "employee_territories",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("employee_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("territory_id", VarChar(20), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "customer_demographics",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("customer_type_id", VarChar(5), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customer_desc", VarChar())
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "customer_customer_demo",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("customer_id", VarChar(5), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customer_type_id", VarChar(5), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                )
            ]
        )
    )


def addDSGPlatformMappingForWorkspace(eco: Ecosystem, workspace: Workspace, dsg: DatasetGroup, dp: DataPlatform):
    """Add a DSG platform mapping for a workspace/dsg pair, gets set to the current chooser for the dsg"""
    if eco.dsgPlatformMappings.get(f"{workspace.name}#{dsg.name}") is None:
        eco.dsgPlatformMappings[f"{workspace.name}#{dsg.name}"] = DatasetGroupDataPlatformAssignments(
            workspace=workspace.name,
            dsgName=dsg.name,
            assignments=[
                DSGDataPlatformAssignment(
                    workspace=workspace.name,
                    dsgName=dsg.name,
                    dp=DataPlatformKey(dp.name),
                    doc=PlainTextDocumentation("Test docs"),
                    productionStatus=ProductionStatus.PRODUCTION,
                    deprecationsAllowed=DeprecationsAllowed.NEVER,
                    status=DatasetGroupDataPlatformMappingStatus.PROVISIONED)]
        )
    else:
        eco.dsgPlatformMappings[f"{workspace.name}#{dsg.name}"].assignments.append(
            DSGDataPlatformAssignment(
                workspace=workspace.name,
                dsgName=dsg.name,
                dp=DataPlatformKey(dp.name),
                doc=PlainTextDocumentation("Test docs"),
                productionStatus=ProductionStatus.PRODUCTION,
                deprecationsAllowed=DeprecationsAllowed.NEVER, status=DatasetGroupDataPlatformMappingStatus.PROVISIONED))


def defineWorkspaces(eco: Ecosystem, t: Team, locations: set[LocationKey], chooser: DataPlatformChooser, dp: DataPlatform):
    """Create a Workspace and an asset if a location is provided"""

    # Warehouse for Workspaces
    ws_db: DataContainer = HostPortSQLDatabase("AzureSQL", locations, HostPortPair("hostName", 1344), "DBName")

    w: Workspace = Workspace(
        "ProductLiveAdhocReporting",
        ws_db,
        DatasetGroup(
            "LiveProducts",
            platform_chooser=chooser,
            sinks=[
                DatasetSink("NW_Data", "products"),
                DatasetSink("NW_Data", "customers"),
                DatasetSink("NW_Data", "suppliers")
            ]
        ))
    t.add(w)
    addDSGPlatformMappingForWorkspace(eco, w, w.dsgs["LiveProducts"], dp)

    # Define Workspace with Refiner to mask customer table
    # This is a DataTransformer that uses NW_Data#customers as its input and produces
    # Masked_NW_Data#employees as its output. The transform is done using an Azure SQL database and
    # a python application running on a Kubernetes cluster.
    w: Workspace = Workspace(
        "MaskCustomersWorkSpace",
        ws_db,
        DatasetGroup(
            "MaskCustomers",
            sinks=[
                DatasetSink("NW_Data", "customers")
            ]
        ),
        DataTransformer(
            "MaskCustomers",
            Datastore(
                "Masked_NW_Data",
                datasets=[
                    Dataset(
                        "customers",
                        schema=DDLTable(
                            documentation=PlainTextDocumentation("Masked version of customers table with privacy data removed"),
                            columns=[
                                DDLColumn("customer_id", SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                                DDLColumn("last_name", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                                DDLColumn("first_name", VarChar(10), nullable=NullableStatus.NOT_NULLABLE),
                                DDLColumn("country", VarChar(15))
                            ]
                        ),
                        classifications=[SimpleDC(SimpleDCTypes.PUB)]
                    )
                ]
            ),
            PythonCodeArtifact([], {}, "3.11")
            )
        )
    t.add(w)

    w = Workspace(
        "WorkspaceUsingTransformerOutput",
        ws_db,
        DatasetGroup(
            "UseMaskedCustomers",
            platform_chooser=chooser,
            sinks=[
                DatasetSink("Masked_NW_Data", "customers")
            ]
        ))
    t.add(w)
    addDSGPlatformMappingForWorkspace(eco, w, w.dsgs["UseMaskedCustomers"], dp)
