from typing import Sequence, cast
import unittest
from datasurface.md import Ecosystem, TeamDeclaration, Workspace, Team, DatasetGroup, DatasetSink, WorkspacePlatformConfig, DataLatency, DataPlatform
from datasurface.md import Dataset, Datastore, DDLTable, DDLColumn, Integer, String, Date, GovernanceZone
from datasurface.md import Decimal, Variant, TinyInt, SmallInt, BigInt, Float, Double, Vector, GovernanceZoneDeclaration
from datasurface.md import ConsumerRetentionRequirements, DataRetentionPolicy
from datetime import timedelta
from datasurface.md.AmazonAWS import AmazonAWSDataPlatform
from datasurface.md.Azure import AzureSQLDatabase, AzureDataplatform, AzureKeyVaultCredential
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.GitOps import FakeRepository, GitHubRepository
from datasurface.md.Governance import CDCCaptureIngestion, CloudVendor, DataTransformerOutput, DatastoreCacheEntry, DefaultDataPlatform, DependentWorkspaces, \
    DeprecationStatus, DeprecationsAllowed, InfrastructureLocation, InfrastructureVendor, IngestionConsistencyType, ProductionStatus
from datasurface.md.Lint import ValidationTree
from datasurface.md.Policy import SimpleDC, SimpleDCTypes

from datasurface.md.Schema import NullableStatus, PrimaryKeyStatus
from tests.nwdb.eco import createEcosystem


class TestWorkspace(unittest.TestCase):

    def createEco(self) -> Ecosystem:
        eco: Ecosystem = Ecosystem(
            "BigCorp",
            GitHubRepository("a", "b"),
            GovernanceZoneDeclaration("US", GitHubRepository("aa", "bb")),
            GovernanceZoneDeclaration("China", GitHubRepository("aa", "cc")),
            AmazonAWSDataPlatform("FastPlatform", PlainTextDocumentation("Test")),
            AmazonAWSDataPlatform("SlowPlatform", PlainTextDocumentation("Test"))
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
        eco: Ecosystem = Ecosystem(
            "BigCorp",
            GitHubRepository("a", "b"),
            GovernanceZoneDeclaration(usZoneName, GitHubRepository("aa", "bb")),
            GovernanceZoneDeclaration(
                chinaZoneName,
                GitHubRepository("aa", "cc")),
            AmazonAWSDataPlatform("FastPlatform", PlainTextDocumentation("Test")),
            AmazonAWSDataPlatform("SlowPlatform", PlainTextDocumentation("Test"))
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
                    DDLTable(
                        DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                        DDLColumn("Col2", String(10)),
                        DDLColumn("Col3", Date())
                        )
                    ),
                Dataset(
                    "Dataset2",
                    DDLTable(
                        DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                        DDLColumn("Col2", String(10))
                        )
                    )
                ),
            Datastore(
                "Store2",
                Dataset(
                    "Dataset1",
                    DDLTable(
                        DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                        DDLColumn("Col2", String(10)),
                        DDLColumn("Col3", Date())
                        )
                    ),
                Dataset(
                    "Dataset2",
                    DDLTable(
                        DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                        DDLColumn("Col2", String(10))
                        )
                    )
                ),
            Workspace(
                "WK_A",
                DatasetGroup(
                    "FastStuff",
                    WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, DataLatency.SECONDS, None, None)),
                    DatasetSink("Store1", "Dataset1"),
                    DatasetSink("Store2", "Dataset2")
                    ),
                DatasetGroup(
                    "SlowESMAStuff",
                    WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "ESMA", timedelta(weeks=5*52))),
                    DatasetSink("Store3", "Dataset4"),
                    DatasetSink("Store4", "Dataset5")
                    ),
                DatasetGroup(
                    "SlowSEC_Stuff",
                    WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "SEC", timedelta(weeks=7*52))),
                    DatasetSink("Store3", "Dataset4"),
                    DatasetSink("Store4", "Dataset5")
                    )
                )
            )

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
        intType: DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK, NullableStatus.NOT_NULLABLE, SimpleDC(SimpleDCTypes.MNPI))
        col2: DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK,  NullableStatus.NOT_NULLABLE, SimpleDC(SimpleDCTypes.MNPI))
        self.assertEqual(intType, col2)
        self.assertEqual(col2, intType)

        # Change name of type
        col2: DDLColumn = DDLColumn("Col2", Integer(), PrimaryKeyStatus.PK,  NullableStatus.NOT_NULLABLE, SimpleDC(SimpleDCTypes.MNPI))
        self.assertNotEqual(intType, col2)
        self.assertNotEqual(col2, intType)

        # Change type to Decimal
        col2: DDLColumn = DDLColumn("Col1", Decimal(10, 2), PrimaryKeyStatus.PK,  NullableStatus.NOT_NULLABLE, SimpleDC(SimpleDCTypes.MNPI))
        self.assertNotEqual(col2, intType)
        self.assertNotEqual(intType, col2)

        # Just change primary key flag
        col2: DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.NOT_PK,  NullableStatus.NOT_NULLABLE, SimpleDC(SimpleDCTypes.MNPI))
        self.assertNotEqual(intType, col2)
        self.assertNotEqual(col2, intType)

        # Just change nullable
        col2: DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK,  NullableStatus.NULLABLE, SimpleDC(SimpleDCTypes.MNPI))
        self.assertNotEqual(intType, col2)
        self.assertNotEqual(col2, intType)

        # Just change classification
        col2: DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK,  NullableStatus.NOT_NULLABLE, SimpleDC(SimpleDCTypes.PC1))
        self.assertNotEqual(intType, col2)
        self.assertNotEqual(col2, intType)

    def test_DDLTable(self):
        t1: DDLTable = DDLTable(
            DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
            DDLColumn("Col2", String(10)),
            DDLColumn("Col3", Date())
            )
        self.assertEqual(t1, t1)
        self.assertIsNotNone(t1.primaryKeyColumns)
        if (t1.primaryKeyColumns):
            self.assertEqual(len(t1.primaryKeyColumns.colNames), 1)
            self.assertEqual(t1.primaryKeyColumns.colNames[0], "Col1")
        self.assertEqual(len(t1.columns), 3)

        # Move primarykey column
        t2: DDLTable = DDLTable(
            DDLColumn("Col1", Integer(), PrimaryKeyStatus.NOT_PK),
            DDLColumn("Col2", String(10), PrimaryKeyStatus.PK),
            DDLColumn("Col3", Date())
            )
        self.assertNotEqual(t1, t2)
        self.assertIsNotNone(t2.primaryKeyColumns)
        if (t2.primaryKeyColumns):
            self.assertEqual(len(t2.primaryKeyColumns.colNames), 1)
            self.assertEqual(t2.primaryKeyColumns.colNames[0], "Col2")

        # Change Column name
        t2: DDLTable = DDLTable(
            DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
            DDLColumn("Col2_XXX", String(10)),
            DDLColumn("Col3", Date())
            )
        self.assertNotEqual(t1, t2)

        # Remove column
        t2: DDLTable = DDLTable(
            DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
            DDLColumn("Col2_XXX", String(10))
            )
        self.assertNotEqual(t1, t2)
        self.assertEqual(len(t2.columns), 2)

        # Add Column name
        t2: DDLTable = DDLTable(
            DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
            DDLColumn("Col2_XXX", String(10)),
            DDLColumn("Col3", Date()),
            DDLColumn("Col4", Double())
            )
        self.assertNotEqual(t1, t2)
        self.assertEqual(len(t2.columns), 4)

    def test_DatasetEquality(self):

        d1: Dataset = Dataset(
            "Dataset1",
            DDLTable(
                DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                DDLColumn("Col2", String(10)),
                DDLColumn("Col3", Date())
                )
            )
        d2: Dataset = Dataset(
            "Dataset2",
            DDLTable(
                DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                DDLColumn("Col2", String(10)),
                DDLColumn("Col3", Date())
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
                DDLTable(
                    DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                    DDLColumn("Col2", String(10)),
                    DDLColumn("Col3", Date())
                    )
                ),
            Dataset(
                "Dataset2",
                DDLTable(
                    DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                    DDLColumn("Col2", String(10)),
                    DDLColumn("Col3", Date())
                    )
                )
            )
        s2: Datastore = Datastore(
            "Store1",
            Dataset(
                "Dataset1",
                DDLTable(
                    DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                    DDLColumn("Col2", String(10)),
                    DDLColumn("Col3", Date())
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
                DDLTable(
                    DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK),
                    DDLColumn("Col2", String(10)),
                    DDLColumn("Col3", Date())
                    )
                )
            )
        self.assertEqual(s1, s2)

    def createSimpleEcosystem(self) -> Ecosystem:

        # Make ecosystem and declare a single zone US
        e: Ecosystem = Ecosystem(
                "BigCorp", FakeRepository("a"),
                DefaultDataPlatform(
                    AzureDataplatform("Azure", PlainTextDocumentation("Test"), AzureKeyVaultCredential("keyvault", "aa"))
                    ),
                InfrastructureVendor(
                    "Azure",
                    CloudVendor.AWS,
                    PlainTextDocumentation("Azure test vendor"),
                    InfrastructureLocation("FL")),
                GovernanceZoneDeclaration("US", FakeRepository("b")))

        # Define US zone and declare a single team Test
        gzUSA: GovernanceZone = e.getZoneOrThrow("US")
        gzUSA.add(TeamDeclaration("Test", FakeRepository("c")))

        # Define the team with a single store with 2 datasets and a single Workspace using those datasets
        t: Team = gzUSA.getTeamOrThrow("Test")
        t.add(
            Datastore(
                "Store1",
                CDCCaptureIngestion(
                    AzureSQLDatabase("Test", "mysqlserver.database.windows.net", 1344, "dbName", e.getLocationOrThrow("Azure", ["FL"])),
                    IngestionConsistencyType.MULTI_DATASET,
                    AzureKeyVaultCredential("keyvault", "aa")
                ),
                Dataset(
                    "Dataset1",
                    SimpleDC(SimpleDCTypes.PUB),
                    DDLTable(
                        DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK, NullableStatus.NOT_NULLABLE),
                        DDLColumn("Col2", String(10)),
                        DDLColumn("Col3", Date())
                        )
                ),
                Dataset(
                    "Dataset2",
                    SimpleDC(SimpleDCTypes.PUB),
                    DDLTable(
                        DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK, NullableStatus.NOT_NULLABLE),
                        DDLColumn("Col2", String(10)),
                        DDLColumn("Col3", Date())
                        )
                    )
                ),
            Workspace(
                "WK_A",
                DatasetGroup(
                    "FastStuff",
                    WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, DataLatency.SECONDS, None, None)),
                    DatasetSink("Store1", "Dataset1"),
                    DatasetSink("Store1", "Dataset2")
                    )
                )
        )

        # Prepare ecosystem and check no errors or issues
        tree: ValidationTree = e.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())
        self.assertFalse(tree.hasIssues())
        return e

    def test_StoreDependants(self):
        eco: Ecosystem = self.createSimpleEcosystem()

        dep: Sequence[DependentWorkspaces] = eco.calculateDependenciesForDatastore("Store1")
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
        self.assertFalse(eTree.hasIssues())

        # Mark Sink as allowing deprecated datasets and check again, should be good
        sink_dataset2: DatasetSink = ws.dsgs["FastStuff"].sinks["Store1:Dataset2"]
        sink_dataset2.deprecationsAllowed = DeprecationsAllowed.ALLOWED

        eTree = e.lintAndHydrateCaches()
        self.assertFalse(eTree.hasErrors())
        self.assertTrue(eTree.hasIssues())  # Workspace using deprecated dataset

        # Move back to clean model
        dataset2.deprecationStatus.status = DeprecationStatus.NOT_DEPRECATED
        sink_dataset2.deprecationsAllowed = DeprecationsAllowed.NEVER

        eTree = e.lintAndHydrateCaches()
        # Verify its clean
        self.assertFalse(eTree.hasErrors())
        self.assertFalse(eTree.hasIssues())

        # Make sure we checked the production status
        self.assertEqual(store.productionStatus, ProductionStatus.NOT_PRODUCTION)
        self.assertEqual(ws.productionStatus, ProductionStatus.NOT_PRODUCTION)

        # Should get warning that a non production store used in a production workspace
        store.productionStatus = ProductionStatus.NOT_PRODUCTION
        ws.productionStatus = ProductionStatus.PRODUCTION
        eTree = e.lintAndHydrateCaches()
        self.assertFalse(eTree.hasErrors())
        self.assertTrue(eTree.hasIssues())

    def test_WorkspaceEquality(self):
        fastP: DataPlatform = AmazonAWSDataPlatform("FastPlatform", PlainTextDocumentation("Test"))
        slowP: DataPlatform = AmazonAWSDataPlatform("SlowPlatform", PlainTextDocumentation("Test"))

        self.assertEqual(fastP, fastP)
        self.assertNotEqual(fastP, slowP)
        self.assertNotEqual(slowP, fastP)

        w1: Workspace = Workspace(
            "WK_A",
            DatasetGroup(
                "FastStuff",
                WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, DataLatency.SECONDS, None, None)),
                DatasetSink("Store1", "Dataset1"),
                DatasetSink("Store2", "Dataset2")
                ),
            DatasetGroup(
                "SlowESMA_Stuff",
                WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "ESMA", timedelta(weeks=5*52))),
                DatasetSink("Store3", "Dataset4"),
                DatasetSink("Store4", "Dataset5")
                ),
            DatasetGroup(
                "SlowSEC_Stuff",
                WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "SEC", timedelta(weeks=7*52))),
                DatasetSink("Store3", "Dataset4"),
                DatasetSink("Store4", "Dataset5")
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
            ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "ESMA", timedelta(weeks=5*52)))
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

        depGraph: list[DependentWorkspaces] = list(eco.calculateDependenciesForDatastore("NW_Data"))

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
