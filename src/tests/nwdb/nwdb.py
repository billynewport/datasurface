from datasurface.md import *
from datasurface.md.Azure import AzureKeyVaultCredential


def defineTables(t : Team):
    t.add(
        Datastore("NW_Data",
            CaptureMetaData(
                IngestionConsistencyType.MULTI,
                AzureKeyVaultCredential("https://mykeyvault.vault.azure.net", "NWDB_Creds"),
                PyOdbcSourceInfo(
                    serverHost="tcp:nwdb.database.windows.net,1433",
                    databaseName="nwdb",
                    driver="{ODBC Driver 17 for SQL Server}",
                    connectionStringTemplate="mssql+pyodbc://{username}:{password}@{serverHost}/{databaseName}?driver={driver}"
                ),
            ),
            Dataset("us_states",
                DDLTable(
                    DDLColumn("state_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("state_name", VarChar(100), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("state_abbr", VarChar(2), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("state_region", VarChar(50), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("customers",
                DDLTable(
                    DDLColumn("customer_id", VarChar(5), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("contact_name", VarChar(30), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("contact_title", VarChar(30), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("address", VarChar(60), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("city", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("region", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("postal_code", VarChar(10), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("country", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("phone", VarChar(24), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("fax", VarChar(24), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("orders",
                DDLTable(
                    DDLColumn("order_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("customer_id", VarChar(5), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("employee_id", SmallInt(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("order_date", Date(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("required_date", Date(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("shipped_date", Date(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("ship_via", SmallInt(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("freight", IEEE32(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("ship_name", VarChar(40), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("ship_address", VarChar(60), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("ship_city", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("ship_region", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("ship_postal_code", VarChar(10), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("ship_country", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("employees",
                DDLTable(
                    DDLColumn("employee_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("last_name", VarChar(20), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("first_name", VarChar(10), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("title", VarChar(30), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("title_of_courtesy", VarChar(25), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("birth_date", Date(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("hire_date", Date(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("address", VarChar(60), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("city", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("region", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("postal_code", VarChar(10), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("country", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("home_phone", VarChar(24), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("extension", VarChar(4), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("photo", Variant(None), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("notes", VarChar(None), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("reports_to", SmallInt(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("photo_path", VarChar(255), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("shippers",
                DDLTable(
                    DDLColumn("shipper_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("phone", VarChar(24), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("products",
                DDLTable(
                    DDLColumn("product_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("product_name", VarChar(40), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("supplier_id", SmallInt(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("category_id", SmallInt(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("quantity_per_unit", VarChar(20), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("unit_price", IEEE32(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("units_in_stock", SmallInt(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("units_on_order", SmallInt(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("reorder_level", SmallInt(), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("discontinued", Integer(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("categories",
                DDLTable(
                    DDLColumn("category_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("category_name", VarChar(15), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("description", VarChar(None), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("picture", Variant(None), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("suppliers",
                DDLTable(
                    DDLColumn("supplier_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("contact_name", VarChar(30), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("contact_title", VarChar(30), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("address", VarChar(60), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("city", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("region", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("postal_code", VarChar(10), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("country", VarChar(15), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("phone", VarChar(24), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("fax", VarChar(24), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("homepage", VarChar(None), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("order_details",
                DDLTable(
                    DDLColumn("order_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("product_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("unit_price", IEEE32(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("quantity", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("discount", IEEE32(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("region",
                DDLTable(
                    DDLColumn("region_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("region_description", VarChar(60), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("territories",
                DDLTable(
                    DDLColumn("territory_id", VarChar(20), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("territory_description", VarChar(60), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK),
                    DDLColumn("region_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("employee_territories",
                DDLTable(
                    DDLColumn("employee_id", SmallInt(), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("territory_id", VarChar(20), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK)
                )
            ),
            Dataset("customer_demographics",
                DDLTable(
                    DDLColumn("customer_type_id", VarChar(5), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("customer_desc", VarChar(None), NullableStatus.NULLABLE, DataClassification.PUB, PrimaryKeyStatus.NOT_PK)
                )
            ),
            Dataset("customer_customer_demo",
                DDLTable(
                    DDLColumn("customer_id", VarChar(5), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("customer_type_id", VarChar(5), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK)
                )
            )
        )
    )

def defineWorkspaces(t : Team):
    t.add(
        Workspace("Product live adhoc reporting",
            DatasetGroup("Live Products",
                WorkspacePlatformConfig(
                    ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, 
                        DataLatency.MINUTES, # Minutes of latency is acceptable
                        None, # Regulator
                        None) # Data used here has no retention requirement due to this use case
                    ),
                DatasetSink("NW_Data", "products"),
                DatasetSink("NW_Data", "customers"),
                DatasetSink("NW_Data", "suppliers")
            )
        )
    )