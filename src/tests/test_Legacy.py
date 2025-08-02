"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.md import Ecosystem, GovernanceZone, Team, TeamDeclaration, GovernanceZoneDeclaration
from datasurface.md.repo import GitHubRepository
from datasurface.md import DataContainer, Workspace, DatasetGroup
from datasurface.md.credential import Credential, CredentialType
from datasurface.md import CloudVendor, InfrastructureVendor, InfrastructureLocation, LocationKey
from datasurface.md import HostPortSQLDatabase, HostPortPair, DatasetSink
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md import ValidationTree, Datastore, Dataset, CDCCaptureIngestion, CronTrigger, IngestionConsistencyType
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import SmallInt, VarChar
from datasurface.platforms.legacy import LegacyDataPlatform, LegacyDataPlatformChooser, LegacyPlatformServiceProvider


"""
The intention here is to test DataPlatforms with GovernanceZones. We will do this using a LegacyDataPlatform with certain
characteristics and make sure that Ecosystems using this can be validated correctly.
"""


def defineTables(eco: Ecosystem, gz: GovernanceZone, t: Team, locations: set[LocationKey]):
    """Create a sample Producer with a couple of tables"""
    t.add(
        Datastore(
            name="NW_Data",
            capture_metadata=CDCCaptureIngestion(
                HostPortSQLDatabase(
                    name="NW_DB",
                    locations=locations,
                    hostPort=HostPortPair(hostName="hostName.com", port=1344),
                    databaseName="DBName"
                ),
                CronTrigger("NW_Data Every 10 mins", "0,10,20,30,40,50 * * * *"),
                IngestionConsistencyType.MULTI_DATASET,
                Credential("eu_cred", CredentialType.USER_PASSWORD),
                ),
            datasets=[
                Dataset(
                    name="us_states",
                    classifications=[SimpleDC(SimpleDCTypes.PUB)],
                    schema=DDLTable(
                        columns=[
                            DDLColumn(name="state_id", data_type=SmallInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn(name="state_name", data_type=VarChar(100)),
                            DDLColumn(name="state_abbr", data_type=VarChar(2)),
                            DDLColumn(name="state_region", data_type=VarChar(50))
                        ]
                    )
                ),
                Dataset(
                    name="customers",
                    classifications=[SimpleDC(SimpleDCTypes.PC3)],
                    documentation=PlainTextDocumentation(
                        description="This data includes customer information from the Northwind database. It contains PII data."
                    ),
                    schema=DDLTable(
                        columns=[
                            DDLColumn(name="customer_id", data_type=VarChar(5), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn(name="company_name", data_type=VarChar(40), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn(name="contact_name", data_type=VarChar(30)),
                            DDLColumn(name="contact_title", data_type=VarChar(30)),
                            DDLColumn(name="address", data_type=VarChar(60)),
                            DDLColumn(name="city", data_type=VarChar(15)),
                            DDLColumn(name="region", data_type=VarChar(15)),
                            DDLColumn(name="postal_code", data_type=VarChar(10)),
                            DDLColumn(name="country", data_type=VarChar(15)),
                            DDLColumn(name="phone", data_type=VarChar(24)),
                            DDLColumn(name="fax", data_type=VarChar(24))
                        ]
                    )
                )
            ]
        )
    )


def defineWorkspaces(eco: Ecosystem, t: Team, locations: set[LocationKey]):
    """Create a Workspace and an asset if a location is provided"""

    # Warehouse for Workspaces
    ws_db: DataContainer = HostPortSQLDatabase(
        name="WHDB",
        locations=locations,
        hostPort=HostPortPair(hostName="whhostname.com", port=1344),
        databaseName="WHDB"
    )

    w: Workspace = Workspace(
        "ProductLiveAdhocReporting",
        ws_db,
        DatasetGroup(
            name="LiveProducts",
            platform_chooser=LegacyDataPlatformChooser(
                dataPlatformName="LegacyA",
                doc=PlainTextDocumentation(
                    description="This is a legacy application that is managed by the LegacyApplicationTeam"
                ),
                containers=set()
            ),
            sinks=[
                DatasetSink(storeName="NW_Data", datasetName="customers"),
                DatasetSink(storeName="NW_Data", datasetName="us_states")
            ]
        ))
    t.add(w)


def createEcosystem() -> Ecosystem:
    """Create an Ecosystem with a LegacyDataPlatform"""
    psp: LegacyPlatformServiceProvider = LegacyPlatformServiceProvider(
        "LegacyPSP",
        {LocationKey("MyCorp:USA/NY_1")},
        [
            LegacyDataPlatform("LegacyA", PlainTextDocumentation("Test")),
        ]
    )
    ecosys: Ecosystem = Ecosystem(
        name="Test",
        repo=GitHubRepository(
            repo="billynewport/repo",
            branchName="ECOmain"
        ),
        platform_services_providers=[psp],
        governance_zone_declarations=[
            GovernanceZoneDeclaration(
                name="USA",
                authRepo=GitHubRepository(
                    repo="billynewport/repo",
                    branchName="USAmain"
                )
            )
        ],
        infrastructure_vendors=[
            InfrastructureVendor(
                name="LegacyA",
                cloud_vendor=CloudVendor.PRIVATE,
                documentation=PlainTextDocumentation(description="Legacy infrastructure"),
                locations=[
                    InfrastructureLocation(
                        name="USA",
                        locations=[
                            InfrastructureLocation(name="ny1"),  # New York City
                            InfrastructureLocation(name="nj1")   # New Jersey
                        ]
                    )
                ]
            )
        ],
        liveRepo=GitHubRepository(
            repo="billynewport/repo",
            branchName="EcoLive"
        )
    )

    gzUSA: GovernanceZone = ecosys.getZoneOrThrow("USA")

    gzUSA.add(
        TeamDeclaration(
            name="LegacyApplicationTeam",
            authRepo=GitHubRepository(
                repo="billynewport/repo",
                branchName="legacyAppTeamMain"
            )
        ),
    )

    # Fill out the NorthWindTeam managed by the USA governance zone
    legacy_Team: Team = ecosys.getTeamOrThrow("USA", "LegacyApplicationTeam")
    locations: set[LocationKey] = {LocationKey(locStr="LegacyA:USA/ny1")}
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
        self.assertIsNotNone(e)
