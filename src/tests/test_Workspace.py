"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


from typing import Sequence, cast, Optional
import unittest
from datasurface.md import Ecosystem, TeamDeclaration, Workspace, Team, DatasetGroup, DatasetSink, WorkspacePlatformConfig, DataLatency, DataPlatform
from datasurface.md import Dataset, Datastore, DDLTable, DDLColumn, Integer, Date, GovernanceZone, LocationKey
from datasurface.md.types import Decimal, Variant, TinyInt, SmallInt, BigInt, Float, Double, Vector, String
from datasurface.md import GovernanceZoneDeclaration
from datasurface.md import ConsumerRetentionRequirements, DataMilestoningStrategy
from datetime import timedelta
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository, FakeRepository
from datasurface.md import CDCCaptureIngestion, CloudVendor, DataTransformerOutput, \
    DatastoreCacheEntry, DependentWorkspaces, \
    DeprecationStatus, DeprecationsAllowed, InfrastructureLocation, InfrastructureVendor, IngestionConsistencyType, ProductionStatus
from datasurface.md import ValidationTree
from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md import SQLDatabase, CronTrigger

from datasurface.md import NullableStatus, PrimaryKeyStatus
from tests.nwdb.eco import createEcosystem
from datasurface.platforms.legacy import LegacyDataPlatform, LegacyPlatformServiceProvider
from datasurface.md.credential import Credential, CredentialType


class TestWorkspace(unittest.TestCase):

    def createEco(self) -> Ecosystem:
        psp: LegacyPlatformServiceProvider = LegacyPlatformServiceProvider(
            "LegacyPSP",
            {LocationKey("Azure:FL")},
            [
                LegacyDataPlatform("FastPlatform", PlainTextDocumentation("Test")),
                LegacyDataPlatform("SlowPlatform", PlainTextDocumentation("Test"))
            ]
        )
        eco: Ecosystem = Ecosystem(
            "BigCorp",
            GitHubRepository("a", "b"),
            InfrastructureVendor(
                "Azure",
                CloudVendor.AZURE,
                PlainTextDocumentation("Azure test vendor"),
                InfrastructureLocation("FL")),
            GovernanceZoneDeclaration("US", GitHubRepository("aa", "bb")),
            GovernanceZoneDeclaration("China", GitHubRepository("aa", "cc")),
            platform_services_providers=[psp],
            liveRepo=GitHubRepository("a", "live")
            )

        self.assertEqual(eco, eco)

        gzUSA: GovernanceZone = eco.getZoneOrThrow("US")
        gzUSA.add(
                TeamDeclaration("Test", GitHubRepository("gitrepo", "module"))
        )

        gzChina: GovernanceZone = eco.getZoneOrThrow("China")
        gzChina.add(
                TeamDeclaration("China Team", GitHubRepository("gitrepo", "module"))
                # Mandatory policy that ALL data must be stored within vendors/assets declared in this zone
            )
        return eco

    def test_SimpleWorkspace(self):
        usZoneName: str = "US"
        chinaZoneName: str = "China"
        testTeamName: str = "Test Team A"
        # First define an ecosystem with a single US zone and a single team
        psp: LegacyPlatformServiceProvider = LegacyPlatformServiceProvider(
            "LegacyPSP",
            {LocationKey("Azure:FL")},
            [
                LegacyDataPlatform("FastPlatform", PlainTextDocumentation("Test")),
                LegacyDataPlatform("SlowPlatform", PlainTextDocumentation("Test"))
            ]
        )
        eco: Ecosystem = Ecosystem(
            "BigCorp",
            GitHubRepository("a", "b"),
            InfrastructureVendor(
                "Azure",
                CloudVendor.AZURE,
                PlainTextDocumentation("Azure test vendor"),
                InfrastructureLocation("FL")),
            GovernanceZoneDeclaration(usZoneName, GitHubRepository("aa", "bb")),
            GovernanceZoneDeclaration(
                chinaZoneName,
                GitHubRepository("aa", "cc")),
            platform_services_providers=[psp],
            liveRepo=GitHubRepository("a", "live")
            )

        gzUSA: GovernanceZone = eco.getZoneOrThrow(usZoneName)
        gzUSA.add(
                TeamDeclaration(testTeamName, GitHubRepository("gitrepo url", "module")),
                )
        gzChina: GovernanceZone = eco.getZoneOrThrow(chinaZoneName)
        gzChina.add(
                TeamDeclaration("China Team", GitHubRepository("git repo 2", "module"))
        )
        self.assertEqual(eco.zones.getNumObjects(), 2)

        # Test Dataplatforms can be found
        eco.getDataPlatformOrThrow("FastPlatform")
        eco.getDataPlatformOrThrow("SlowPlatform")

        # Check we can't get a team that wasnt declared in the GovernanceZone
        self.assertIsNone(gzUSA.getTeam("Undefined Team"))

        t: Team = gzUSA.getTeamOrThrow(testTeamName)
        t.add(
            Datastore(
                "Store1",
                Dataset(
                    "Dataset1",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("Col2", String(10)),
                            DDLColumn("Col3", Date())
                        ]
                    )
                ),
                Dataset(
                    "Dataset2",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("Col2", String(10))
                        ]
                    )
                )
            ),
            Datastore(
                "Store2",
                Dataset(
                    "Dataset1",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("Col2", String(10)),
                            DDLColumn("Col3", Date())
                        ]
                    )
                ),
                Dataset(
                    "Dataset2",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("Col2", String(10))
                        ]
                    )
                )
            ),
            Workspace(
                "WK_A",
                DatasetGroup(
                    "FastStuff",
                    platform_chooser=WorkspacePlatformConfig(ConsumerRetentionRequirements(DataMilestoningStrategy.LIVE_ONLY, DataLatency.SECONDS, None, None)),
                    sinks=[
                        DatasetSink("Store1", "Dataset1"),
                        DatasetSink("Store2", "Dataset2")
                    ]
                ),
                DatasetGroup(
                    "SlowESMAStuff",
                    platform_chooser=WorkspacePlatformConfig(
                        ConsumerRetentionRequirements(DataMilestoningStrategy.FORENSIC, DataLatency.MINUTES, "ESMA", timedelta(weeks=5*52))
                    ),
                    sinks=[
                        DatasetSink("Store3", "Dataset4"),
                        DatasetSink("Store4", "Dataset5")
                    ]
                ),
                DatasetGroup(
                    "SlowSEC_Stuff",
                    platform_chooser=WorkspacePlatformConfig(
                        ConsumerRetentionRequirements(DataMilestoningStrategy.FORENSIC, DataLatency.MINUTES, "SEC", timedelta(weeks=7*52))
                    ),
                    sinks=[
                        DatasetSink("Store3", "Dataset4"),
                        DatasetSink("Store4", "Dataset5")
                    ]
                )
            )
        )

        # Verify that all elements created with named parameter constructors have correct values

        # Test Datastore values
        store1: Datastore = t.dataStores["Store1"]
        store2: Datastore = t.dataStores["Store2"]
        self.assertEqual(store1.name, "Store1")
        self.assertEqual(store2.name, "Store2")
        self.assertEqual(len(store1.datasets), 2)
        self.assertEqual(len(store2.datasets), 2)

        # Test Dataset values
        dataset1_store1: Dataset = store1.datasets["Dataset1"]
        dataset2_store1: Dataset = store1.datasets["Dataset2"]
        dataset1_store2: Dataset = store2.datasets["Dataset1"]
        dataset2_store2: Dataset = store2.datasets["Dataset2"]

        self.assertEqual(dataset1_store1.name, "Dataset1")
        self.assertEqual(dataset2_store1.name, "Dataset2")
        self.assertEqual(dataset1_store2.name, "Dataset1")
        self.assertEqual(dataset2_store2.name, "Dataset2")

        # Verify schema was correctly set
        self.assertIsNotNone(dataset1_store1.originalSchema)
        self.assertIsNotNone(dataset2_store1.originalSchema)
        self.assertIsNotNone(dataset1_store2.originalSchema)
        self.assertIsNotNone(dataset2_store2.originalSchema)

        # Test DDLTable values
        table1: DDLTable = cast(DDLTable, dataset1_store1.originalSchema)
        table2: DDLTable = cast(DDLTable, dataset2_store1.originalSchema)

        self.assertEqual(len(table1.columns), 3)  # Col1, Col2, Col3
        self.assertEqual(len(table2.columns), 2)  # Col1, Col2

        # Test DDLColumn values for table1
        col1_table1: Optional[DDLColumn] = table1.getColumnByName("Col1")
        col2_table1: Optional[DDLColumn] = table1.getColumnByName("Col2")
        col3_table1: Optional[DDLColumn] = table1.getColumnByName("Col3")

        self.assertIsNotNone(col1_table1)
        self.assertIsNotNone(col2_table1)
        self.assertIsNotNone(col3_table1)

        # Verify column properties were set correctly
        if col1_table1:
            self.assertEqual(col1_table1.name, "Col1")
            self.assertEqual(col1_table1.primaryKey, PrimaryKeyStatus.PK)
            self.assertEqual(col1_table1.nullable, NullableStatus.NULLABLE)  # Default value
            self.assertIsInstance(col1_table1.type, Integer)

        if col2_table1:
            self.assertEqual(col2_table1.name, "Col2")
            self.assertEqual(col2_table1.primaryKey, PrimaryKeyStatus.NOT_PK)  # Default value
            self.assertEqual(col2_table1.nullable, NullableStatus.NULLABLE)  # Default value
            self.assertIsInstance(col2_table1.type, String)

        if col3_table1:
            self.assertEqual(col3_table1.name, "Col3")
            self.assertEqual(col3_table1.primaryKey, PrimaryKeyStatus.NOT_PK)  # Default value
            self.assertEqual(col3_table1.nullable, NullableStatus.NULLABLE)  # Default value
            self.assertIsInstance(col3_table1.type, Date)

        # Test DDLColumn values for table2
        col1_table2: Optional[DDLColumn] = table2.getColumnByName("Col1")
        col2_table2: Optional[DDLColumn] = table2.getColumnByName("Col2")

        self.assertIsNotNone(col1_table2)
        self.assertIsNotNone(col2_table2)
        # Check that the column does not exist
        self.assertIsNone(table2.columns.get("Col3"))

        if col1_table2:
            self.assertEqual(col1_table2.name, "Col1")
            self.assertEqual(col1_table2.primaryKey, PrimaryKeyStatus.PK)
            self.assertIsInstance(col1_table2.type, Integer)

        if col2_table2:
            self.assertEqual(col2_table2.name, "Col2")
            self.assertEqual(col2_table2.primaryKey, PrimaryKeyStatus.NOT_PK)
            self.assertIsInstance(col2_table2.type, String)

        # Test Workspace and DatasetGroup values
        workspace: Workspace = t.workspaces["WK_A"]
        self.assertEqual(workspace.name, "WK_A")
        self.assertEqual(len(workspace.dsgs), 3)

        # Test DatasetGroup values
        dsg_fast: DatasetGroup = workspace.dsgs["FastStuff"]
        dsg_esma: DatasetGroup = workspace.dsgs["SlowESMAStuff"]
        dsg_sec: DatasetGroup = workspace.dsgs["SlowSEC_Stuff"]

        self.assertEqual(dsg_fast.name, "FastStuff")
        self.assertEqual(dsg_esma.name, "SlowESMAStuff")
        self.assertEqual(dsg_sec.name, "SlowSEC_Stuff")

        # Verify platform choosers were set correctly
        self.assertIsNotNone(dsg_fast.platformMD)
        self.assertIsNotNone(dsg_esma.platformMD)
        self.assertIsNotNone(dsg_sec.platformMD)

        self.assertIsInstance(dsg_fast.platformMD, WorkspacePlatformConfig)
        self.assertIsInstance(dsg_esma.platformMD, WorkspacePlatformConfig)
        self.assertIsInstance(dsg_sec.platformMD, WorkspacePlatformConfig)

        # Verify sinks were set correctly
        self.assertEqual(len(dsg_fast.sinks), 2)
        self.assertEqual(len(dsg_esma.sinks), 2)
        self.assertEqual(len(dsg_sec.sinks), 2)

        # Check specific sink values
        fast_sink1: DatasetSink = dsg_fast.sinks["Store1:Dataset1"]
        fast_sink2: DatasetSink = dsg_fast.sinks["Store2:Dataset2"]

        self.assertIsNotNone(fast_sink1)
        self.assertIsNotNone(fast_sink2)
        self.assertEqual(fast_sink1.storeName, "Store1")
        self.assertEqual(fast_sink1.datasetName, "Dataset1")
        self.assertEqual(fast_sink2.storeName, "Store2")
        self.assertEqual(fast_sink2.datasetName, "Dataset2")

    def test_DataTypeEquality(self):
        t1: Integer = Integer()
        self.assertEqual(t1, Integer())
        self.assertNotEqual(t1, String(10))
        self.assertNotEqual(t1, Decimal(10, 2))

        t2: TinyInt = TinyInt()
        self.assertEqual(t2, TinyInt())
        self.assertNotEqual(t2, String(10))
        self.assertNotEqual(t2, Decimal(10, 2))

        t3: SmallInt = SmallInt()
        self.assertEqual(t3, SmallInt())
        self.assertNotEqual(t3, String(10))
        self.assertNotEqual(t3, Decimal(10, 2))

        t4: BigInt = BigInt()
        self.assertEqual(t4, BigInt())
        self.assertNotEqual(t4, String(10))
        self.assertNotEqual(t4, Decimal(10, 2))

        t5: Float = Float()
        self.assertEqual(t5, Float())
        self.assertNotEqual(t5, String(10))
        self.assertNotEqual(t5, Decimal(10, 2))

        t6: Double = Double()
        self.assertEqual(t6, Double())
        self.assertNotEqual(t6, String(10))
        self.assertNotEqual(t6, Decimal(10, 2))

        t7: Decimal = Decimal(10, 2)
        self.assertNotEqual(t7, Integer())
        self.assertEqual(t7, Decimal(10, 2))
        self.assertNotEqual(t7, Decimal(9, 2))
        self.assertNotEqual(t7, Decimal(10, 3))

        t8: String = String(10)
        self.assertEqual(t8, String(10))
        self.assertNotEqual(t8, String(11))
        self.assertNotEqual(t8, Integer())

        t9: Vector = Vector(10)
        self.assertEqual(t9, Vector(10))
        self.assertNotEqual(t9, String(11))
        self.assertNotEqual(t9, Integer())

        t10: Date = Date()
        self.assertEqual(t10, Date())
        self.assertNotEqual(t10, String(10))
        self.assertNotEqual(t10, Integer())

        t11: Variant = Variant(10)
        self.assertEqual(t11, Variant(10))
        self.assertNotEqual(t11, Variant(9))
        self.assertNotEqual(t11, String(10))

    def test_ColumnEquality(self):

        # Check equality works
        intType: DDLColumn = DDLColumn(
            "Col1", Integer(), primary_key=PrimaryKeyStatus.PK,
            nullable=NullableStatus.NOT_NULLABLE, classifications=[SimpleDC(SimpleDCTypes.MNPI)]
        )
        col2: DDLColumn = DDLColumn(
            "Col1", Integer(), primary_key=PrimaryKeyStatus.PK,
            nullable=NullableStatus.NOT_NULLABLE, classifications=[SimpleDC(SimpleDCTypes.MNPI)]
        )
        self.assertEqual(intType, col2)
        self.assertEqual(col2, intType)

        # Change name of type
        col2: DDLColumn = DDLColumn(
            "Col2", Integer(), primary_key=PrimaryKeyStatus.PK,
            nullable=NullableStatus.NOT_NULLABLE, classifications=[SimpleDC(SimpleDCTypes.MNPI)]
        )
        self.assertNotEqual(intType, col2)
        self.assertNotEqual(col2, intType)

        # Change type to Decimal
        col2: DDLColumn = DDLColumn(
            "Col1", Decimal(10, 2), primary_key=PrimaryKeyStatus.PK,
            nullable=NullableStatus.NOT_NULLABLE, classifications=[SimpleDC(SimpleDCTypes.MNPI)]
        )
        self.assertNotEqual(col2, intType)
        self.assertNotEqual(intType, col2)

        # Just change primary key flag
        col2: DDLColumn = DDLColumn(
            "Col1", Integer(), primary_key=PrimaryKeyStatus.NOT_PK,
            nullable=NullableStatus.NOT_NULLABLE, classifications=[SimpleDC(SimpleDCTypes.MNPI)]
        )
        self.assertNotEqual(intType, col2)
        self.assertNotEqual(col2, intType)

        # Just change nullable
        col2: DDLColumn = DDLColumn(
            "Col1", Integer(), primary_key=PrimaryKeyStatus.PK,
            nullable=NullableStatus.NULLABLE, classifications=[SimpleDC(SimpleDCTypes.MNPI)]
        )
        self.assertNotEqual(intType, col2)
        self.assertNotEqual(col2, intType)

        # Just change classification
        col2: DDLColumn = DDLColumn(
            "Col1", Integer(), primary_key=PrimaryKeyStatus.PK,
            nullable=NullableStatus.NOT_NULLABLE, classifications=[SimpleDC(SimpleDCTypes.PC1)]
        )
        self.assertNotEqual(intType, col2)
        self.assertNotEqual(col2, intType)

    def test_DDLTable(self):
        t1: DDLTable = DDLTable(
            columns=[
                DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                DDLColumn("Col2", String(10)),
                DDLColumn("Col3", Date())
            ]
        )
        self.assertEqual(t1, t1)
        self.assertIsNotNone(t1.primaryKeyColumns)
        if (t1.primaryKeyColumns):
            self.assertEqual(len(t1.primaryKeyColumns.colNames), 1)
            self.assertEqual(t1.primaryKeyColumns.colNames[0], "Col1")
        self.assertEqual(len(t1.columns), 3)

        # Move primarykey column
        t2: DDLTable = DDLTable(
            columns=[
                DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.NOT_PK),
                DDLColumn("Col2", String(10), primary_key=PrimaryKeyStatus.PK),
                DDLColumn("Col3", Date())
            ]
        )
        self.assertNotEqual(t1, t2)
        self.assertIsNotNone(t2.primaryKeyColumns)
        if (t2.primaryKeyColumns):
            self.assertEqual(len(t2.primaryKeyColumns.colNames), 1)
            self.assertEqual(t2.primaryKeyColumns.colNames[0], "Col2")

        # Change Column name
        t2: DDLTable = DDLTable(
            columns=[
                DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                DDLColumn("Col2_XXX", String(10)),
                DDLColumn("Col3", Date())
            ]
        )
        self.assertNotEqual(t1, t2)

        # Remove column
        t2: DDLTable = DDLTable(
            columns=[
                DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                DDLColumn("Col2_XXX", String(10))
            ]
        )
        self.assertNotEqual(t1, t2)
        self.assertEqual(len(t2.columns), 2)

        # Add Column name
        t2: DDLTable = DDLTable(
            columns=[
                DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                DDLColumn("Col2_XXX", String(10)),
                DDLColumn("Col3", Date()),
                DDLColumn("Col4", Double())
            ]
        )
        self.assertNotEqual(t1, t2)
        self.assertEqual(len(t2.columns), 4)

    def test_DatasetEquality(self):

        d1: Dataset = Dataset(
            "Dataset1",
            schema=DDLTable(
                columns=[
                    DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                    DDLColumn("Col2", String(10)),
                    DDLColumn("Col3", Date())
                ]
            )
        )
        d2: Dataset = Dataset(
            "Dataset2",
            schema=DDLTable(
                columns=[
                    DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                    DDLColumn("Col2", String(10)),
                    DDLColumn("Col3", Date())
                ]
            )
        )
        self.assertEqual(d1.name, "Dataset1")
        self.assertEqual(d1, d1)
        self.assertEqual(d2.name, "Dataset2")
        self.assertNotEqual(d1, d2)

    def test_DatastoreEquality(self):
        s1: Datastore = Datastore(
            "Store1",
            Dataset(
                "Dataset1",
                schema=DDLTable(
                    columns=[
                        DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                        DDLColumn("Col2", String(10)),
                        DDLColumn("Col3", Date())
                    ]
                )
            ),
            Dataset(
                "Dataset2",
                schema=DDLTable(
                    columns=[
                        DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                        DDLColumn("Col2", String(10)),
                        DDLColumn("Col3", Date())
                    ]
                )
            )
        )
        s2: Datastore = Datastore(
            "Store1",
            Dataset(
                "Dataset1",
                schema=DDLTable(
                    columns=[
                        DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                        DDLColumn("Col2", String(10)),
                        DDLColumn("Col3", Date())
                    ]
                )
            ),
        )

        d1: Dataset = s1.datasets["Dataset1"]
        d2: Dataset = s1.datasets["Dataset2"]
        self.assertEqual(d1.name, "Dataset1")
        self.assertEqual(d1, d1)
        self.assertEqual(d2.name, "Dataset2")
        self.assertNotEqual(d1, d2)

        self.assertNotEqual(s1, s2)
        s2.add(
            Dataset(
                "Dataset2",
                schema=DDLTable(
                    columns=[
                        DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK),
                        DDLColumn("Col2", String(10)),
                        DDLColumn("Col3", Date())
                    ]
                )
            )
        )
        self.assertEqual(s1, s2)

    def createSimpleEcosystem(self) -> Ecosystem:

        # Make ecosystem and declare a single zone US
        psp: LegacyPlatformServiceProvider = LegacyPlatformServiceProvider(
            "LegacyPSP",
            {LocationKey("Azure:FL")},
            [
                LegacyDataPlatform("Azure", PlainTextDocumentation("Test"))
            ]
        )
        e: Ecosystem = Ecosystem(
                "BigCorp", FakeRepository("a"),
                InfrastructureVendor(
                    "Azure",
                    CloudVendor.AZURE,
                    PlainTextDocumentation("Azure test vendor"),
                    InfrastructureLocation("FL")),
                GovernanceZoneDeclaration("US", FakeRepository("b")),
                platform_services_providers=[psp],
                liveRepo=FakeRepository("b")
            )

        # Define US zone and declare a single team Test
        gzUSA: GovernanceZone = e.getZoneOrThrow("US")
        gzUSA.add(TeamDeclaration("Test", FakeRepository("c")))

        # Define the team with a single store with 2 datasets and a single Workspace using those datasets
        t: Team = gzUSA.getTeamOrThrow("Test")
        t.add(
            Datastore(
                "Store1",
                CDCCaptureIngestion(
                    SQLDatabase(
                        "US_NWDB",
                        {LocationKey("Azure:FL")},  # Where is the database
                        databaseName="nwdb"
                    ),
                    Credential("eu_cred", CredentialType.USER_PASSWORD),
                    CronTrigger("NW_Data Every 10 mins", "*/10 * * * *"),
                    IngestionConsistencyType.MULTI_DATASET),
                Dataset(
                    "Dataset1",
                    classifications=[SimpleDC(SimpleDCTypes.PUB)],
                    schema=DDLTable(
                        columns=[
                            DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("Col2", String(10)),
                            DDLColumn("Col3", Date())
                        ]
                    )
                ),
                Dataset(
                    "Dataset2",
                    classifications=[SimpleDC(SimpleDCTypes.PUB)],
                    schema=DDLTable(
                        columns=[
                            DDLColumn("Col1", Integer(), primary_key=PrimaryKeyStatus.PK, nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("Col2", String(10)),
                            DDLColumn("Col3", Date())
                        ]
                    )
                )
            ),
            Workspace(
                "WK_A",
                DatasetGroup(
                    "FastStuff",
                    platform_chooser=WorkspacePlatformConfig(ConsumerRetentionRequirements(DataMilestoningStrategy.LIVE_ONLY, DataLatency.SECONDS, None, None)),
                    sinks=[
                        DatasetSink("Store1", "Dataset1"),
                        DatasetSink("Store1", "Dataset2")
                    ]
                )
            )
        )

        # Prepare ecosystem and check no errors or issues
        tree: ValidationTree = e.lintAndHydrateCaches()
        tree.printTree()
        self.assertFalse(tree.hasErrors())
        self.assertFalse(tree.hasWarnings())
        return e

    def test_StoreDependants(self):
        eco: Ecosystem = self.createSimpleEcosystem()

        dep: Sequence[DependentWorkspaces] = eco.calculateDependenciesForDatastore("Store1", set())
        self.assertEqual(len(dep), 1)

        # Check that Transformers are included in the dependency list

    def test_DatasetDeprecation(self):
        """Make a Workspace using a deprecated dataset and check it fails linting, then allow deprecated datasets and check it passes but has a warning"""

        e: Ecosystem = self.createSimpleEcosystem()

        # Make a dataset deprecated with a no deprecation allowed sink link in a workspace
        storeI: DatastoreCacheEntry = e.cache_getDatastoreOrThrow("Store1")
        store: Datastore = storeI.datastore
        dataset2: Dataset = store.datasets["Dataset2"]
        dataset2.deprecationStatus.status = DeprecationStatus.DEPRECATED

        ws: Workspace = e.cache_getWorkspaceOrThrow("WK_A").workspace

        eTree: ValidationTree = e.lintAndHydrateCaches()
        self.assertTrue(eTree.hasErrors())
        self.assertFalse(eTree.hasWarnings())
        self.assertEqual(eTree.numWarnings, 0)

        # Mark Sink as allowing deprecated datasets and check again, should be good
        sink_dataset2: DatasetSink = ws.dsgs["FastStuff"].sinks["Store1:Dataset2"]
        sink_dataset2.deprecationsAllowed = DeprecationsAllowed.ALLOWED

        eTree = e.lintAndHydrateCaches()
        self.assertFalse(eTree.hasErrors())
        self.assertEqual(eTree.numWarnings, 0)

        # Move back to clean model
        dataset2.deprecationStatus.status = DeprecationStatus.NOT_DEPRECATED
        sink_dataset2.deprecationsAllowed = DeprecationsAllowed.NEVER

        eTree = e.lintAndHydrateCaches()
        # Verify its clean
        self.assertFalse(eTree.hasErrors())
        self.assertEqual(eTree.numWarnings, 0)

        # Make sure we checked the production status
        self.assertEqual(store.productionStatus, ProductionStatus.NOT_PRODUCTION)
        self.assertEqual(ws.productionStatus, ProductionStatus.NOT_PRODUCTION)

        # Should get warning that a non production store used in a production workspace
        store.productionStatus = ProductionStatus.NOT_PRODUCTION
        ws.productionStatus = ProductionStatus.PRODUCTION
        eTree = e.lintAndHydrateCaches()
        self.assertFalse(eTree.hasErrors())
        self.assertTrue(eTree.hasWarnings())  # Workspace using deprecated dataset

    def test_WorkspaceEquality(self):
        fastP: DataPlatform = LegacyDataPlatform(
            "FastPlatform",
            PlainTextDocumentation("Test"))
        slowP: DataPlatform = LegacyDataPlatform(
            "SlowPlatform",
            PlainTextDocumentation("Test"))

        self.assertEqual(fastP, fastP)
        self.assertNotEqual(fastP, slowP)
        self.assertNotEqual(slowP, fastP)

        w1: Workspace = Workspace(
            "WK_A",
            DatasetGroup(
                "FastStuff",
                platform_chooser=WorkspacePlatformConfig(ConsumerRetentionRequirements(DataMilestoningStrategy.LIVE_ONLY, DataLatency.SECONDS, None, None)),
                sinks=[
                    DatasetSink("Store1", "Dataset1"),
                    DatasetSink("Store2", "Dataset2")
                ]
            ),
            DatasetGroup(
                "SlowESMA_Stuff",
                platform_chooser=WorkspacePlatformConfig(
                    ConsumerRetentionRequirements(DataMilestoningStrategy.FORENSIC, DataLatency.MINUTES, "ESMA", timedelta(weeks=5*52))
                ),
                sinks=[
                    DatasetSink("Store3", "Dataset4"),
                    DatasetSink("Store4", "Dataset5")
                ]
            ),
            DatasetGroup(
                "SlowSEC_Stuff",
                platform_chooser=WorkspacePlatformConfig(
                    ConsumerRetentionRequirements(DataMilestoningStrategy.FORENSIC, DataLatency.MINUTES, "SEC", timedelta(weeks=7*52))
                ),
                sinks=[
                    DatasetSink("Store3", "Dataset4"),
                    DatasetSink("Store4", "Dataset5")
                ]
            )
        )

        self.assertEqual(w1, w1)

        w1_fastChooser: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, w1.dsgs["FastStuff"].platformMD)
        w1_slowChooser: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, w1.dsgs["SlowESMA_Stuff"].platformMD)

# TODO Not implemented yet
#        w1_fastP: Optional[DataPlatform] = w1_fastChooser.choooseDataPlatform()
#        w1_slowP: Optional[DataPlatform] = w1_slowChooser.choooseDataPlatform()

        retFast: ConsumerRetentionRequirements = w1_fastChooser.retention
        retSlow: ConsumerRetentionRequirements = w1_slowChooser.retention

        self.assertEqual(retFast, retFast)
        self.assertEqual(retSlow, retSlow)
        self.assertNotEqual(retFast, retSlow)
        self.assertNotEqual(retSlow, retFast)

        dsg1: DatasetGroup = w1.dsgs["FastStuff"]
        dsg2: DatasetGroup = w1.dsgs["SlowESMA_Stuff"]
        dsg3: DatasetGroup = w1.dsgs["SlowSEC_Stuff"]
        self.assertIsNotNone(dsg1)
        self.assertIsNotNone(dsg2)
        self.assertIsNotNone(dsg3)

        self.assertEqual(dsg2, dsg2)
        self.assertEqual(dsg3, dsg3)
        self.assertNotEqual(dsg2, dsg3)
        self.assertNotEqual(dsg3, dsg2)
        self.assertNotEqual(dsg1.sinks, dsg2.sinks)
        self.assertEqual(dsg2.sinks, dsg3.sinks)

        # Force dsg3 to be equal to dsg2 and test it
        dsg3.platformMD = WorkspacePlatformConfig(
            ConsumerRetentionRequirements(DataMilestoningStrategy.FORENSIC, DataLatency.MINUTES, "ESMA", timedelta(weeks=5*52)))
        dsg3.name = dsg2.name
        self.assertEqual(dsg2, dsg3)

    def test_TeamEquality(self):
        eco: Ecosystem = self.createEco()

        gzUSA: GovernanceZone = eco.getZoneOrThrow("US")
        gzChina: GovernanceZone = eco.getZoneOrThrow("China")

        t1: Team = gzUSA.getTeamOrThrow("Test")
        t2: Team = gzChina.getTeamOrThrow("China Team")

        self.assertEqual(t1, t1)
        self.assertEqual(t2, t2)
        self.assertNotEqual(t1, t2)
        self.assertNotEqual(t2, t1)

    def test_StoreDependencies(self):
        eco: Ecosystem = createEcosystem()

        depGraph: list[DependentWorkspaces] = list(eco.calculateDependenciesForDatastore("NW_Data", set()))

        # One Workspace and a Transformer Workspace and a Workspace using the transformer output store
        ws_set: set[Workspace] = set()
        for dep in depGraph:
            ws_set.update(dep.flatten())

        ws_names: set[str] = set()
        for ws in ws_set:
            ws_names.add(ws.name)

        self.assertEqual(3, len(ws_names))
        self.assertTrue("ProductLiveAdhocReporting" in ws_names)
        self.assertTrue("MaskCustomersWorkSpace" in ws_names)
        self.assertTrue("WorkspaceUsingTransformerOutput" in ws_names)

    def test_TransformerLinting(self):
        eco: Ecosystem = createEcosystem()

        # Change the output Datastore cmd workspace name to AAA which doesn't exist
        t: Team = eco.getTeamOrThrow("USA", "NorthWindTeam")
        store: Datastore = t.getStoreOrThrow("Masked_NW_Data")
        cmd: DataTransformerOutput = cast(DataTransformerOutput, store.cmd)
        cmd.workSpaceName = "AAA"

        tree: ValidationTree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())

        # Change Datastore refiner
