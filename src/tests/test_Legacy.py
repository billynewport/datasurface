"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.md import Ecosystem, GovernanceZone, Team, DataPlatformKey, TeamDeclaration, GovernanceZoneDeclaration
from datasurface.md import GitHubRepository, ClearTextCredential, DataContainer, Workspace, DatasetGroup
from datasurface.md import CloudVendor, DefaultDataPlatform, InfrastructureVendor, InfrastructureLocation, LocationKey
from datasurface.md import PlainTextDocumentation, HostPortSQLDatabase, HostPortPair, DatasetSink
from datasurface.md import ValidationTree, Datastore, Dataset, CDCCaptureIngestion, CronTrigger, IngestionConsistencyType, \
    SimpleDC, SimpleDCTypes, DDLTable, DDLColumn, SmallInt, VarChar, NullableStatus, PrimaryKeyStatus
from datasurface.platforms.legacy import LegacyDataPlatform, LegacyDatPlatformChooser


"""
The intention here is to test DataPlatforms with GovernanceZones. We will do this using a LegacyDataPlatform with certain
characteristics and make sure that Ecosystems using this can be validated correctly.
"""


def defineTables(eco: Ecosystem, gz: GovernanceZone, t: Team, locations: set[LocationKey]):
    """Create a sample Producer with a couple of tables"""
    t.add(
        Datastore(
            "NW_Data",
            CDCCaptureIngestion(
                HostPortSQLDatabase("NW_DB", locations, HostPortPair("hostName.com", 1344), "DBName"),
                CronTrigger("NW_Data Every 10 mins", "0,10,20,30,40,50 * * * *"),
                IngestionConsistencyType.MULTI_DATASET,
                ClearTextCredential("user", "pwd")
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
            ))
    )


def defineWorkspaces(eco: Ecosystem, t: Team, locations: set[LocationKey]):
    """Create a Workspace and an asset if a location is provided"""

    # Warehouse for Workspaces
    ws_db: DataContainer = HostPortSQLDatabase("WHDB", locations, HostPortPair("whhostname.com", 1344), "WHDB")

    w: Workspace = Workspace(
        "ProductLiveAdhocReporting",
        ws_db,
        DatasetGroup(
            "LiveProducts",
            LegacyDatPlatformChooser(
                "LegacyA",
                PlainTextDocumentation("This is a legacy application that is managed by the LegacyApplicationTeam"),
                set()),
            DatasetSink("NW_Data", "customers"),
            DatasetSink("NW_Data", "us_states")
        ))
    t.add(w)


def createEcosystem() -> Ecosystem:
    """Create an Ecosystem with a LegacyDataPlatform"""
    ecosys: Ecosystem = Ecosystem(
        "Test",
        GitHubRepository("billynewport/repo", "ECOmain"),
        LegacyDataPlatform(
            "LegacyA",
            PlainTextDocumentation("Test")),

        # Data Platforms
        DefaultDataPlatform(DataPlatformKey("LegacyA")),

        # GovernanceZones
        GovernanceZoneDeclaration("USA", GitHubRepository("billynewport/repo", "USAmain")),

        # Infra Vendors and locations
        InfrastructureVendor(
            "LegacyA",
            CloudVendor.PRIVATE,
            PlainTextDocumentation("Legacy infrastructure"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation("ny1"),  # New York City
                InfrastructureLocation("nj1")))  # New Jersey
        )

    gzUSA: GovernanceZone = ecosys.getZoneOrThrow("USA")

    gzUSA.add(
            TeamDeclaration("LegacyApplicationTeam", GitHubRepository("billynewport/repo", "legacyAppTeamMain")),
        )

    # Fill out the NorthWindTeam managed by the USA governance zone
    legacy_Team: Team = ecosys.getTeamOrThrow("USA", "LegacyApplicationTeam")
    locations: set[LocationKey] = {LocationKey("LegacyA:USA/ny1")}
    defineTables(ecosys, gzUSA, legacy_Team, locations)
    defineWorkspaces(ecosys, legacy_Team, locations)

    tree: ValidationTree = ecosys.lintAndHydrateCaches()
    if (tree.hasErrors()):
        tree.printTree()
        raise Exception("Ecosystem validation failed")
    return ecosys


class TestLegacy(unittest.TestCase):
    def testLegacyPlatform(self):
        e: Ecosystem = createEcosystem()
