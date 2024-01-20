from typing import Optional
from datasurface.md.Azure import AzureKeyVaultCredential
from datasurface.md.Governance import CDCCaptureIngestion, Dataset, Datastore, Ecosystem, GitHubRepository, GovernanceZone, GovernanceZoneDeclaration, IngestionConsistencyType, PyOdbcSourceInfo, Team, TeamDeclaration
from datasurface.md.Lint import ValidationTree
from datasurface.md.Schema import DDLColumn, DDLTable, DataClassification, NullableStatus, PrimaryKeyStatus, SmallInt, VarChar


def createEcosystem() -> Ecosystem:
    # All changes will come from this repo
    repo : GitHubRepository = GitHubRepository("https://github.com/billynewport/eco.git", "main")

    ecosys : Ecosystem = Ecosystem("Test Ecosystem", repo,
        GovernanceZoneDeclaration("USA", repo)
    )

    gz : Optional[GovernanceZone] = ecosys.getZone("USA")
    if(gz == None):
        raise Exception("USA governance zone not found")
    
    gz.add(
        TeamDeclaration("Customer", repo)
        )
    
    t : Optional[Team] = gz.getTeam("Customer")
    if t == None:
        raise Exception("Customer team not found")

    t.add(
        Datastore("NW_Data",
            CDCCaptureIngestion(
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
                    DDLColumn("state_name", VarChar(100)),
                    DDLColumn("state_abbr", VarChar(2)),
                    DDLColumn("state_region", VarChar(50))
                )
            ),
            Dataset("customers",
                DDLTable(
                    DDLColumn("customer_id", VarChar(5), NullableStatus.NOT_NULLABLE, DataClassification.PUB, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40)),
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
            )
        )
    )
    
    return ecosys

if __name__ == "__main__":
    ecosys : Ecosystem = createEcosystem()
    tree : ValidationTree = ecosys.lintAndHydrateCaches()
    tree.printTree()
