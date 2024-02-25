from datasurface.md.Azure import AzureKeyVaultCredential
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.Governance import CDCCaptureIngestion, CronTrigger, Dataset, Datastore, Ecosystem, GovernanceZone, IngestionConsistencyType, \
    PyOdbcSourceInfo, Team
from datasurface.md.Policy import SimpleDC, SimpleDCTypes
from datasurface.md.Schema import IEEE32, DDLColumn, DDLTable, Date, NullableStatus, PrimaryKeyStatus, SmallInt, VarChar


def defineEU_GZ(gzEU: GovernanceZone, e: Ecosystem):
    parisTeam: Team = gzEU.getTeamOrThrow("ParisTeam")

    parisTeam.add(
        PlainTextDocumentation("This is the Paris team responsible for vaious EU specific data and workspaces"),
        Datastore(
            "EU_Customers",
            PlainTextDocumentation("EU Customer data"),
            CDCCaptureIngestion(
                PyOdbcSourceInfo(
                    "EU_NWDB",
                    e.getLocationOrThrow("AWS", ["EU", "eu-central-1"]),  # Where is the database
                    serverHost="tcp:nwdb.database.windows.net,1433",
                    databaseName="nwdb",
                    driver="{ODBC Driver 17 for SQL Server}",
                    connectionStringTemplate="mssql+pyodbc://{username}:{password}@{serverHost}/{databaseName}?driver={driver}"
                ),
                CronTrigger("NW_Data Every 10 mins", "*/10 * * * *"),
                IngestionConsistencyType.MULTI_DATASET,
                AzureKeyVaultCredential("mykeyvault", "NWDB_Creds")),
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
            )
        )
    )
