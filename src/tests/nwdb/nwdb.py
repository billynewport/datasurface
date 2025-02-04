"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.platforms.legacy import LegacyDatPlatformChooser, LegacyDataTransformer
from datasurface.md import PlainTextDocumentation, HostPortSQLDatabase, UserPasswordCredential
from datasurface.md import CDCCaptureIngestion, CronTrigger, DataContainer, \
        DataTransformer, Dataset, DatasetGroup, DatasetSink, Datastore, Ecosystem, GovernanceZone, \
        InfrastructureLocation, IngestionConsistencyType, PythonCodeArtifact, Team, TimedTransformerTrigger, \
        Workspace, HostPortPair

from datasurface.md import SimpleDC, SimpleDCTypes
from datasurface.md import IEEE32, DDLColumn, DDLTable, Date, Integer, NullableStatus, PrimaryKeyStatus, SmallInt, VarChar, Variant


def defineTables(eco: Ecosystem, gz: GovernanceZone, t: Team):
    t.add(
        Datastore(
            "NW_Data",
            CDCCaptureIngestion(
                HostPortSQLDatabase("NW_DB", {eco.getLocationOrThrow("MyCorp", ["USA", "NY_1"])}, HostPortPair("hostName", 1344), "DBName"),
                CronTrigger("NW_Data Every 10 mins", "0,10,20,30,40,50 * * * *"),
                IngestionConsistencyType.MULTI_DATASET,
                UserPasswordCredential("user", "pwd")
                ),

            Dataset(
                "us_states",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("state_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("state_name", VarChar(100)),
                    DDLColumn("state_abbr", VarChar(2)),
                    DDLColumn("state_region", VarChar(50))
                )
            ),
            Dataset(
                "customers",
                SimpleDC(SimpleDCTypes.PC3),
                PlainTextDocumentation("This data includes customer information from the Northwind database. It contains PII data."),
                DDLTable(
                    DDLColumn("customer_id", VarChar(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40), NullableStatus.NOT_NULLABLE),
                    DDLColumn("contact_name", VarChar(30)),
                    DDLColumn("contact_title", VarChar(30)),
                    DDLColumn("address", VarChar(60)),
                    DDLColumn("city", VarChar(15)),
                    DDLColumn("region", VarChar(15)),
                    DDLColumn("postal_code", VarChar(10)),
                    DDLColumn("country", VarChar(15)),
                    DDLColumn("phone", VarChar(24)),
                    DDLColumn("fax", VarChar(24))
                )
            ),
            Dataset(
                "orders",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("order_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
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
                )
            ),
            Dataset(
                "employees",
                SimpleDC(SimpleDCTypes.PC3),
                DDLTable(
                    DDLColumn("employee_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("last_name", VarChar(20), NullableStatus.NOT_NULLABLE),
                    DDLColumn("first_name", VarChar(10), NullableStatus.NOT_NULLABLE),
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
                )
            ),
            Dataset(
                "shippers",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("shipper_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40), NullableStatus.NOT_NULLABLE),
                    DDLColumn("phone", VarChar(24))
                )
            ),
            Dataset(
                "products",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("product_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("product_name", VarChar(40), NullableStatus.NOT_NULLABLE),
                    DDLColumn("supplier_id", SmallInt()),
                    DDLColumn("category_id", SmallInt()),
                    DDLColumn("quantity_per_unit", VarChar(20)),
                    DDLColumn("unit_price", IEEE32()),
                    DDLColumn("units_in_stock", SmallInt()),
                    DDLColumn("units_on_order", SmallInt()),
                    DDLColumn("reorder_level", SmallInt()),
                    DDLColumn("discontinued", Integer(), NullableStatus.NOT_NULLABLE)
                )
            ),
            Dataset(
                "categories",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("category_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("category_name", VarChar(15), NullableStatus.NOT_NULLABLE),
                    DDLColumn("description", VarChar()),
                    DDLColumn("picture", Variant())
                )
            ),
            Dataset(
                "suppliers",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("supplier_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40), NullableStatus.NOT_NULLABLE),
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
                )
            ),
            Dataset(
                "order_details",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("order_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("product_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("unit_price", IEEE32(), NullableStatus.NOT_NULLABLE),
                    DDLColumn("quantity", SmallInt(), NullableStatus.NOT_NULLABLE),
                    DDLColumn("discount", IEEE32(), NullableStatus.NOT_NULLABLE)
                )
            ),
            Dataset(
                "region",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("region_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("region_description", VarChar(60), NullableStatus.NOT_NULLABLE)
                )
            ),
            Dataset(
                "territories",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("territory_id", VarChar(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("territory_description", VarChar(60), NullableStatus.NOT_NULLABLE),
                    DDLColumn("region_id", SmallInt(), NullableStatus.NOT_NULLABLE)
                )
            ),
            Dataset(
                "employee_territories",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("employee_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("territory_id", VarChar(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)
                )
            ),
            Dataset(
                "customer_demographics",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("customer_type_id", VarChar(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("customer_desc", VarChar())
                )
            ),
            Dataset(
                "customer_customer_demo",
                SimpleDC(SimpleDCTypes.PUB),
                DDLTable(
                    DDLColumn("customer_id", VarChar(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("customer_type_id", VarChar(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)
                )
            )
        )
    )


def defineWorkspaces(eco: Ecosystem, t: Team, locations: set[InfrastructureLocation]):
    """Create a Workspace and an asset if a location is provided"""

    # Warehouse for Workspaces
    ws_db: DataContainer = HostPortSQLDatabase("AzureSQL", locations, HostPortPair("hostName", 1344), "DBName")

    w: Workspace = Workspace(
        "ProductLiveAdhocReporting",
        ws_db,
        DatasetGroup(
            "LiveProducts",
            LegacyDatPlatformChooser(
                "LegacyA",  # Name of eco level LegacyDataPlatform
                PlainTextDocumentation("This is a legacy application that is managed by the LegacyApplicationTeam"),
                set()),
            DatasetSink("NW_Data", "products"),
            DatasetSink("NW_Data", "customers"),
            DatasetSink("NW_Data", "suppliers")
        ))
    t.add(w)

    # Define Workspace with Refiner to mask customer table
    # This is a DataTransformer that uses NW_Data#customers as its input and produces
    # Masked_NW_Data#employees as its output. The transform is done using an Azure SQL database and
    # a python application running on a Kubernetes cluster.
    w: Workspace = Workspace(
        "MaskCustomersWorkSpace",
        ws_db,
        DatasetGroup(
            "MaskCustomers",
            LegacyDatPlatformChooser(
                "LegacyA",  # Name of eco level LegacyDataPlatform
                PlainTextDocumentation("This is a legacy application that is managed by the LegacyApplicationTeam"),
                set()),
            DatasetSink("NW_Data", "customers")
            ),
        DataTransformer(
            "MaskCustomers",
            Datastore(
                "Masked_NW_Data",
                Dataset(
                    "customers",
                    SimpleDC(SimpleDCTypes.PUB),
                    DDLTable(
                        PlainTextDocumentation("Masked version of customers table with privacy data removed"),
                        DDLColumn("customer_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                        DDLColumn("last_name", VarChar(20), NullableStatus.NOT_NULLABLE),
                        DDLColumn("first_name", VarChar(10), NullableStatus.NOT_NULLABLE),
                        DDLColumn("country", VarChar(15))
                    ))),
            TimedTransformerTrigger("Customer_Mask", CronTrigger("MaskCustomers Every 10 mins", "*/10 * * * *")),
            PythonCodeArtifact([], {}, "3.11"),
            LegacyDataTransformer(
                "Legacy Transformer for mask",
                PlainTextDocumentation("This is a legacy transformer"),
                set()
                )
            )
        )
    t.add(w)

    w = Workspace(
        "WorkspaceUsingTransformerOutput",
        ws_db,
        DatasetGroup(
            "UseMaskedCustomers",
            LegacyDatPlatformChooser(
                "LegacyA",  # Name of eco level LegacyDataPlatform
                PlainTextDocumentation("This is a legacy application that is managed by the LegacyApplicationTeam"),
                set()),
            DatasetSink("Masked_NW_Data", "customers")
        ))
    t.add(w)
