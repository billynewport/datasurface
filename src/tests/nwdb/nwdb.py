from datasurface.md import *
from datasurface.md.Azure import AzureKeyVaultCredential
from datasurface.md.Documentation import PlainTextDocumentation

def defineTables(eco : Ecosystem, gz : GovernanceZone, t : Team):
    t.add(
        Datastore("NW_Data",
            CDCCaptureIngestion(
                PyOdbcSourceInfo(
                    eco.getLocationOrThrow("Azure", ["USA", "East US"]), # Where is the database
                    serverHost="tcp:nwdb.database.windows.net,1433",
                    databaseName="nwdb",
                    driver="{ODBC Driver 17 for SQL Server}",
                    connectionStringTemplate="mssql+pyodbc://{username}:{password}@{serverHost}/{databaseName}?driver={driver}"
                ),
                CronTrigger("NW_Data Every 10 mins", "0,10,20,30,40,50 * * * *"),
                IngestionConsistencyType.MULTI_DATASET,
                AzureKeyVaultCredential("https://mykeyvault.vault.azure.net", "NWDB_Creds")
            ),

            Dataset("us_states",
                DDLTable(
                    DDLColumn("state_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("state_name", VarChar(100)),
                    DDLColumn("state_abbr", VarChar(2)),
                    DDLColumn("state_region", VarChar(50))
                )
            ),
            Dataset("customers",
                DataClassification.PC3,
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
            Dataset("orders",
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
            Dataset("employees",
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
            Dataset("shippers",
                DDLTable(
                    DDLColumn("shipper_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40), NullableStatus.NOT_NULLABLE),
                    DDLColumn("phone", VarChar(24))
                )
            ),
            Dataset("products",
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
            Dataset("categories",
                DDLTable(
                    DDLColumn("category_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("category_name", VarChar(15), NullableStatus.NOT_NULLABLE),
                    DDLColumn("description", VarChar()),
                    DDLColumn("picture", Variant())
                )
            ),
            Dataset("suppliers",
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
            Dataset("order_details",
                DDLTable(
                    DDLColumn("order_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("product_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("unit_price", IEEE32(), NullableStatus.NOT_NULLABLE),
                    DDLColumn("quantity", SmallInt(), NullableStatus.NOT_NULLABLE),
                    DDLColumn("discount", IEEE32(), NullableStatus.NOT_NULLABLE)
                )
            ),
            Dataset("region",
                DDLTable(
                    DDLColumn("region_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("region_description", VarChar(60), NullableStatus.NOT_NULLABLE)
                )
            ),
            Dataset("territories",
                DDLTable(
                    DDLColumn("territory_id", VarChar(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("territory_description", VarChar(60), NullableStatus.NOT_NULLABLE),
                    DDLColumn("region_id", SmallInt(), NullableStatus.NOT_NULLABLE)
                )
            ),
            Dataset("employee_territories",
                DDLTable(
                    DDLColumn("employee_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("territory_id", VarChar(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)
                )
            ),
            Dataset("customer_demographics",
                DDLTable(
                    DDLColumn("customer_type_id", VarChar(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("customer_desc", VarChar())
                )
            ),
            Dataset("customer_customer_demo",
                DDLTable(
                    DDLColumn("customer_id", VarChar(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("customer_type_id", VarChar(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)
                )
            )
        )
    )


def defineWorkspaces(eco : Ecosystem, t : Team, location : InfrastructureLocation):
    """Create a Workspace and an asset if a location is provided"""
    if location.key == None:
        raise Exception("location key is none")
    w : Workspace = Workspace("ProductLiveAdhocReporting",
        Asset("Test Azure SQL", [DataContainer("AzureSQL", location.key)]),                            
        DatasetGroup("LiveProducts",
            WorkspacePlatformConfig(
                ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, 
                    DataLatency.MINUTES, # Minutes of latency is acceptable
                    None, # Regulator
                    None) # Data used here has no retention requirement due to this use case
                ),
            DatasetSink("NW_Data", "products"),
            DatasetSink("NW_Data", "customers"),
            DatasetSink("NW_Data", "suppliers")
        ))
    t.add(w)

    # Define Workspace with Refiner to mask customer table
    w : Workspace = Workspace("MaskCustomersWorkSpace",
        Asset("Test Azure SQL", [DataContainer("AzureSQL", location.key)]),
        DatasetGroup("MaskCustomers",
            WorkspacePlatformConfig(
                ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, 
                    DataLatency.MINUTES, # Minutes of latency is acceptable
                    None, # Regulator
                    None) # Data used here has no retention requirement due to this use case
                ),
            DatasetSink("NW_Data", "customers")),
            DataTransformer(
                "MaskCustomers",         
                Datastore("Masked_NW_Data",
                    Dataset("employees",
                        DDLTable(
                            PlainTextDocumentation("Masked version of employees table with privacy data removed"),
                            DDLColumn("employee_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                            DDLColumn("last_name", VarChar(20), NullableStatus.NOT_NULLABLE),
                            DDLColumn("first_name", VarChar(10), NullableStatus.NOT_NULLABLE),
                            DDLColumn("country", VarChar(15))
                        ))),
                TimedTransformerTrigger("Customer_Mask", CronTrigger("MaskCustomers Every 10 mins", "0,10,20,30,40,50 * * * *")),
                PythonCodeArtifact([], {}, "3.11"),
                KubernetesEnvironment(
                    "kubcluster.here.com", 
                    AzureKeyVaultCredential("myvault", "Kubernetes_Creds"),
                    location
                    )
                )
            )
    t.add(w)


    w = Workspace("WorkspaceUsingTransformerOutput",
        Asset("Test Azure SQL", [DataContainer("AzureSQL", location.key)]),
        DatasetGroup("UseMaskedCustomers",
            WorkspacePlatformConfig(
                ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, 
                    DataLatency.MINUTES, # Minutes of latency is acceptable
                    None, # Regulator
                    None) # Data used here has no retention requirement due to this use case
                ),
            DatasetSink("Masked_NW_Data", "employees")
        ))
    t.add(w)
