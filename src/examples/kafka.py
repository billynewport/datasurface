"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import Ecosystem, GitLabRepository, PlainTextDocumentation, InfrastructureVendor, CloudVendor, LocationKey, InfrastructureLocation
from datasurface.md import GovernanceZoneDeclaration, TeamDeclaration, GovernanceZone, Team, Datastore, Dataset
from datasurface.md import CronTrigger, IngestionConsistencyType, SimpleDC, SimpleDCTypes, LocalFileCredentialStore
from datasurface.md import DDLTable, DDLColumn, VarChar, NullableStatus, PrimaryKeyStatus, KafkaIngestion, KafkaServer, HostPortPairList, HostPortPair
from datasurface.platforms.zero.zero import ZeroDataPlatform


def createEcosystem() -> Ecosystem:
    # Define toplevel stuff first, Ecosystem, vendor and a single gzone
    gitLabServer: str = "http://localhost:80"
    eco: Ecosystem = Ecosystem(
        "KafkaEcosystem",
        GitLabRepository(gitLabServer, "demo/kafka_example", "main", PlainTextDocumentation("This is the Kafka Ecosystem")),

        # This is deployed in a home lab in Florida
        InfrastructureVendor(
            "HomeLab",
            CloudVendor.PRIVATE,
            PlainTextDocumentation("Home Lab Infrastructure"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation("Home"))  # Home
            ),
        GovernanceZoneDeclaration(
            "Home",
            GitLabRepository(gitLabServer, "demo/kafka_example", "HomeMain")
            ),
        ZeroDataPlatform(
            "KafkaExample",
            PlainTextDocumentation("This is an example of a Kafka data platform"),
            LocalFileCredentialStore(
                "HomeLab",
                {LocationKey("HomeLab:USA/Home")},
                "/run/secrets"),
            "credentialKey.txt",  # This is a credential to use to connect to kafka
            "http://localhost:9000",  # S3 compatible endpoint
            "staging",  # Bucket name for staging files
            "data"  # Bucket name for data files
            )
        )

    # Define the USA gz
    gz: GovernanceZone = eco.getZoneOrThrow("Home")
    gz.add(
        # Home based team
        TeamDeclaration("HomeTeam", GitLabRepository(gitLabServer, "billynewport/test_step1", "homeMain"))  # Home Team
    )
    team: Team = gz.getTeamOrThrow("HomeTeam")

    # Define the producer for this team
    team.add(
        PlainTextDocumentation("This is the home team responsible for various USA specific data and workspaces"),

        # Defines a Kafka server running in us-east-1 with the hostname kafka1 and port 9092
        KafkaServer("kafka1", {LocationKey("HomeLab:USA/Home")}, HostPortPairList([HostPortPair("kafka1", 9092)]))
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
