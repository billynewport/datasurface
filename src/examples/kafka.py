"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Ecosystem, GitHubRepository, PlainTextDocumentation, InfrastructureVendor, CloudVendor, InfrastructureLocation
from datasurface.md import GovernanceZoneDeclaration, TeamDeclaration, GovernanceZone, Team, Datastore, Dataset
from datasurface.md import CronTrigger, IngestionConsistencyType, SimpleDC, SimpleDCTypes
from datasurface.md import DDLTable, DDLColumn, VarChar, NullableStatus, PrimaryKeyStatus, KafkaIngestion, KafkaServer, HostPortPairList, HostPortPair


def createEcosystem() -> Ecosystem:
    # Define toplevel stuff first, Ecosystem, vendor and a single gzone
    eco: Ecosystem = Ecosystem(
        "KafkaEcosystem",
        GitHubRepository("billynewport", "KafkaEcosystem", PlainTextDocumentation("This is the Kafka Ecosystem")),

        # This is deployed in us-east-1 in AWS
        InfrastructureVendor(
            "AWS",
            CloudVendor.AWS,
            PlainTextDocumentation("Amazon AWS"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation("us-east-1"))  # Virginia
            ),
        GovernanceZoneDeclaration(
            "USA",
            GitHubRepository("billynewport", "USAmain")
            )
        )

    # Define the USA gz
    gz: GovernanceZone = eco.getZoneOrThrow("USA")
    gz.add(
        # NY based team
        TeamDeclaration("NYTeam", GitHubRepository("billynewport/test_step1", "NYMain"))  # NY Team
    )
    team: Team = gz.getTeamOrThrow("NYTeam")

    # Define the producer for this team
    team.add(
        PlainTextDocumentation("This is the New York team responsible for vaious USA specific data and workspaces"),

        # Defines a Kafka server running in us-east-1 with the hostname kafka1 and port 9092
        KafkaServer("kafka1", set([eco.getLocationOrThrow("AWS", ["USA", "us-east-1"])]), HostPortPairList([HostPortPair("kafka1", 9092)]))
    )
    team.add(
        Datastore(
            "EU_Customers",
            PlainTextDocumentation("EU Customer data"),
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
            KafkaIngestion(
                team.getDataContainerOrThrow("kafka1"),
                CronTrigger("Customer data Every 10 mins", "*/10 * * * *"),
                IngestionConsistencyType.MULTI_DATASET
            )
        )
    )
    return eco
