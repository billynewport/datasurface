"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md import LocationKey
from datasurface.md import CDCCaptureIngestion, CronTrigger, Dataset, Datastore, Ecosystem, GovernanceZone, IngestionConsistencyType, \
    Team
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md import IEEE32, DDLColumn, DDLTable, Date, NullableStatus, PrimaryKeyStatus, SmallInt, VarChar
from datasurface.md import SQLDatabase
from datasurface.md.credential import Credential, CredentialType


def defineUSA_GZ(gzEU: GovernanceZone, e: Ecosystem):
    nyTeam: Team = gzEU.getTeamOrThrow("NYTeam")

    nyTeam.add(
        PlainTextDocumentation("This is the NY city team responsible for vaious USA specific data and workspaces"),
        Datastore(
            "USA_Customers",
            PlainTextDocumentation("USA Customer data"),
            CDCCaptureIngestion(
                SQLDatabase(
                    "US_NWDB",
                    {LocationKey("AWS:USA/us-east-1")},  # Where is the database
                    databaseName="nwdb"),
                CronTrigger("NW_Data Every 10 mins", "*/10 * * * *"),
                Credential("eu_cred", CredentialType.USER_PASSWORD),
                IngestionConsistencyType.MULTI_DATASET),
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
