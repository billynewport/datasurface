from typing import Optional
import unittest
from datasurface.md import Ecosystem, TeamDeclaration, GitRepository, Workspace, Team, DatasetGroup, DatasetSink, WorkspacePlatformConfig, DataLatency, DataPlatform
from datasurface.md import Dataset, Datastore, DDLTable, DDLColumn, Integer, String, Date, GovernanceZone, LocalGovernanceManagedOnly
from datasurface.md import Decimal, Variant, TinyInt, SmallInt, BigInt, Float, Double, Vector, DataClassification, GovernanceZoneDeclaration
from datasurface.md import ConsumerRetentionRequirements, DataRetentionPolicy
from datetime import timedelta
from datasurface.md.Governance import CDCCaptureIngestion, DeprecationStatus, DeprecationsAllowed, ProductionStatus, TestRepository
from datasurface.md.Lint import ValidationTree

from datasurface.md.Schema import NullableStatus, PrimaryKeyStatus

class TestWorkspace(unittest.TestCase):

    def createEco(self) -> Ecosystem:
        eco : Ecosystem = Ecosystem("BigCorp", GitRepository("a", "b"),
            GovernanceZoneDeclaration("US", GitRepository("aa", "bb")),
            GovernanceZoneDeclaration("China", GitRepository("aa", "cc")))
        
        self.assertEqual(eco, eco)
        
        gzUSA : Optional[GovernanceZone] = eco.getZone("US")
        if(gzUSA is None):
            raise Exception("US zone not found")
        gzUSA.add(
                TeamDeclaration("Test", GitRepository("gitrepo url", "module")),
                DataPlatform("FastPlatform"),
                DataPlatform("SlowPlatform")
        )

        gzChina : Optional[GovernanceZone] = eco.getZone("China")
        if(gzChina):
            gzChina.add(
                    # Mandatory policy that ALL data must be stored within vendors/assets declared in this zone
                    LocalGovernanceManagedOnly("China Only", True)
                )
        else:
            raise Exception("China zone not found")
        return eco

    def test_SimpleWorkspace(self):
        usZoneName : str = "US"
        chinaZoneName : str = "China"
        testTeamName : str = "Test Team A"
        # First define an ecosystem with a single US zone and a single team
        eco : Ecosystem = Ecosystem("BigCorp", GitRepository("a", "b"),
            GovernanceZoneDeclaration(usZoneName, GitRepository("aa", "bb")),
            GovernanceZoneDeclaration(chinaZoneName, GitRepository("aa", "cc")))
        
        gzUSA : Optional[GovernanceZone] = eco.getZone(usZoneName)
        if(gzUSA is None):
            raise Exception("US zone not found")
        gzUSA.add(
                TeamDeclaration(testTeamName, GitRepository("gitrepo url", "module")),
                DataPlatform("FastPlatform"),
                DataPlatform("SlowPlatform")
                )
        gzChina : Optional[GovernanceZone] = eco.getZone(chinaZoneName)
        if(gzChina is None):
            raise Exception("China zone not found")
        gzChina.add(
                TeamDeclaration("China Team", GitRepository("git repo 2", "module")),
                                    LocalGovernanceManagedOnly("China Only", True)
        )
        

        self.assertEqual(eco.zones.getNumObjects(), 2)

        fastP : Optional[DataPlatform] = gzUSA.platforms.get("FastPlatform")
        self.assertIsNotNone(fastP)
        slowP : (DataPlatform | None) = gzUSA.platforms.get("SlowPlatform")
        self.assertIsNotNone(slowP)

        if(fastP is None or slowP is None):
            raise Exception("Fast or Slow platform not found")
        # Check we can't get a team that wasnt declared in the GovernanceZone
        self.assertIsNone(gzUSA.getTeam("Undefined Team"))

        t : Optional[Team] = gzUSA.getTeam(testTeamName)
        if(t is None):
            raise Exception("Team not found")
        t.add(
            Datastore("Store1",
                    Dataset("Dataset1",
                        DDLTable(
                            DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                            DDLColumn    ("Col2", String(10)),
                            DDLColumn    ("Col3", Date())
                            )
                        ),
                    Dataset("Dataset2",
                        DDLTable(
                            DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                            DDLColumn    ("Col2", String(10))
                            )
                        )
                    ),
            Datastore("Store2",
                    Dataset("Dataset1",
                        DDLTable(
                            DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                            DDLColumn    ("Col2", String(10)),
                            DDLColumn    ("Col3", Date())
                            )
                        ),
                    Dataset("Dataset2",
                        DDLTable(
                            DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                            DDLColumn    ("Col2", String(10))
                            )
                       )
                    ),
            Workspace("WK_A",
                DatasetGroup("Fast stuff",
                            WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, DataLatency.SECONDS, None, None)),
                            DatasetSink("Store1", "Dataset1"),
                            DatasetSink("Store2", "Dataset2")
                            ),
                DatasetGroup("Slow ESMA stuff",
                            WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "ESMA", timedelta(weeks=5*52))),
                            DatasetSink("Store3", "Dataset4"),
                            DatasetSink("Store4", "Dataset5")
                            ),
                DatasetGroup("Slow SEC stuff",
                            WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "SEC", timedelta(weeks=7*52))),
                            DatasetSink("Store3", "Dataset4"),
                            DatasetSink("Store4", "Dataset5")
                            )
                )
            )
        
    def test_DataTypeEquality(self):
        t1 : Integer = Integer()
        self.assertEqual(t1, Integer())
        self.assertNotEqual(t1, String(10))
        self.assertNotEqual(t1, Decimal(10,2))

        t2 : TinyInt = TinyInt()
        self.assertEqual(t2, TinyInt())
        self.assertNotEqual(t2, String(10))
        self.assertNotEqual(t2, Decimal(10,2))

        t3 : SmallInt = SmallInt()
        self.assertEqual(t3, SmallInt())
        self.assertNotEqual(t3, String(10))
        self.assertNotEqual(t3, Decimal(10,2))

        t4 : BigInt = BigInt()
        self.assertEqual(t4, BigInt())
        self.assertNotEqual(t4, String(10))
        self.assertNotEqual(t4, Decimal(10,2))

        t5 : Float = Float()
        self.assertEqual(t5, Float())
        self.assertNotEqual(t5, String(10))
        self.assertNotEqual(t5, Decimal(10,2))

        t6 : Double = Double()
        self.assertEqual(t6, Double())
        self.assertNotEqual(t6, String(10))
        self.assertNotEqual(t6, Decimal(10,2))

        t7 : Decimal = Decimal(10,2)
        self.assertNotEqual(t7, Integer())
        self.assertEqual(t7, Decimal(10,2))
        self.assertNotEqual(t7, Decimal(9,2))
        self.assertNotEqual(t7, Decimal(10,3))

        t8 : String = String(10)
        self.assertEqual(t8, String(10))
        self.assertNotEqual(t8, String(11))
        self.assertNotEqual(t8, Integer())

        t9 : Vector = Vector(10)
        self.assertEqual(t9, Vector(10))
        self.assertNotEqual(t9, String(11))
        self.assertNotEqual(t9, Integer())

        t10 : Date = Date()
        self.assertEqual(t10, Date())
        self.assertNotEqual(t10, String(10))
        self.assertNotEqual(t10, Integer())

        t11 : Variant = Variant(10)
        self.assertEqual(t11, Variant(10))
        self.assertNotEqual(t11, Variant(9))
        self.assertNotEqual(t11, String(10))


    def test_ColumnEquality(self):

        # Check equality works
        intType : DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK, NullableStatus.NOT_NULLABLE, DataClassification.MNPI)
        col2 : DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK,  NullableStatus.NOT_NULLABLE, DataClassification.MNPI)
        self.assertEqual(intType, col2)        
        self.assertEqual(col2, intType)        

        # Change name of type
        col2 : DDLColumn = DDLColumn("Col2", Integer(), PrimaryKeyStatus.PK,  NullableStatus.NOT_NULLABLE, DataClassification.MNPI)
        self.assertNotEqual(intType, col2)        
        self.assertNotEqual(col2, intType)        

        # Change type to Decimal
        col2 : DDLColumn = DDLColumn("Col1", Decimal(10,2), PrimaryKeyStatus.PK,  NullableStatus.NOT_NULLABLE, DataClassification.MNPI)
        self.assertNotEqual(col2, intType)        
        self.assertNotEqual(intType, col2)        

        # Just change primary key flag
        col2 : DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.NOT_PK,  NullableStatus.NOT_NULLABLE, DataClassification.MNPI)
        self.assertNotEqual(intType, col2)        
        self.assertNotEqual(col2, intType)        

        # Just change nullable
        col2 : DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK,  NullableStatus.NULLABLE, DataClassification.MNPI)
        self.assertNotEqual(intType, col2)        
        self.assertNotEqual(col2, intType)        

        # Just change classification
        col2 : DDLColumn = DDLColumn("Col1", Integer(), PrimaryKeyStatus.PK,  NullableStatus.NOT_NULLABLE, DataClassification.PC1)
        self.assertNotEqual(intType, col2)        
        self.assertNotEqual(col2, intType)        

    def test_DDLTable(self):
        t1 : DDLTable = DDLTable(
            DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
            DDLColumn    ("Col2", String(10)),
            DDLColumn    ("Col3", Date())
            )
        self.assertEqual(t1, t1)
        self.assertIsNotNone(t1.primaryKeyColumns)
        if(t1.primaryKeyColumns):
            self.assertEqual(len(t1.primaryKeyColumns.colNames), 1)
            self.assertEqual(t1.primaryKeyColumns.colNames[0], "Col1")
        self.assertEqual(len(t1.columns), 3)

        # Move primarykey column
        t2 : DDLTable = DDLTable(
            DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.NOT_PK),
            DDLColumn    ("Col2", String(10), PrimaryKeyStatus.PK),
            DDLColumn    ("Col3", Date())
            )
        self.assertNotEqual(t1, t2)
        self.assertIsNotNone(t2.primaryKeyColumns)
        if(t2.primaryKeyColumns):
            self.assertEqual(len(t2.primaryKeyColumns.colNames), 1)
            self.assertEqual(t2.primaryKeyColumns.colNames[0], "Col2")

        # Change Column name
        t2 : DDLTable = DDLTable(
            DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
            DDLColumn    ("Col2_XXX", String(10)),
            DDLColumn    ("Col3", Date())
            )
        self.assertNotEqual(t1, t2)
                        
        # Remove column
        t2 : DDLTable = DDLTable(
            DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
            DDLColumn    ("Col2_XXX", String(10))
            )
        self.assertNotEqual(t1, t2)
        self.assertEqual(len(t2.columns), 2)

        # Add Column name
        t2 : DDLTable = DDLTable(
            DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
            DDLColumn    ("Col2_XXX", String(10)),
            DDLColumn    ("Col3", Date()),
            DDLColumn    ("Col4", Double())
            )
        self.assertNotEqual(t1, t2)
        self.assertEqual(len(t2.columns), 4)

    def test_DatasetEquality(self):
        eco : Ecosystem = self.createEco()
        china : Optional[GovernanceZone] = eco.getZone("China")
        if(china is None):
            raise Exception("China zone not found")
        
        d1 : Dataset = Dataset("Dataset1",
            DDLTable(
                DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                DDLColumn    ("Col2", String(10)),
                DDLColumn    ("Col3", Date())
                ),
            china.storagePolicies["China Only"]
            )
        d2 : Dataset = Dataset("Dataset2",
            DDLTable(
                DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                DDLColumn    ("Col2", String(10)),
                DDLColumn    ("Col3", Date())
                )
            )
        self.assertEqual(d1.name, "Dataset1")
        self.assertEqual(d1,d1)
        self.assertEqual(d2.name, "Dataset2")
        self.assertNotEqual(d1,d2)
        d2.name = d1.name
        # Now, the difference is no longer the name, but the storage policy
        self.assertNotEqual(d1,d2)

    def test_DatastoreEquality(self):
        s1 : Datastore = Datastore("Store1",
            Dataset("Dataset1",
                DDLTable(
                    DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                    DDLColumn    ("Col2", String(10)),
                    DDLColumn    ("Col3", Date())
                    )
                ),
            Dataset("Dataset2",
                DDLTable(
                    DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                    DDLColumn    ("Col2", String(10)),
                    DDLColumn    ("Col3", Date())
                    )
                )
            )
        s2 : Datastore = Datastore("Store1",
            Dataset("Dataset1",
                DDLTable(
                    DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                    DDLColumn    ("Col2", String(10)),
                    DDLColumn    ("Col3", Date())
                    )
                ),
            )
        
        d1 : Dataset = s1.datasets["Dataset1"]
        d2 : Dataset = s1.datasets["Dataset2"]
        self.assertEqual(d1.name, "Dataset1")
        self.assertEqual(d1,d1)
        self.assertEqual(d2.name, "Dataset2")
        self.assertNotEqual(d1,d2)

        self.assertNotEqual(s1, s2)        
        s2.add(
            Dataset("Dataset2",
                DDLTable(
                    DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK),
                    DDLColumn    ("Col2", String(10)),
                    DDLColumn    ("Col3", Date())
                    )
                )
            )
        self.assertEqual(s1, s2)        

    def test_DatasetDeprecation(self):
        """Make a Workspace using a deprecated dataset and check it fails linting, then allow deprecated datasets and check it passes but has a warning"""
        e : Ecosystem = Ecosystem("BigCorp", TestRepository("a"),
            GovernanceZoneDeclaration("US", TestRepository("b")))
        
        gzUSA : Optional[GovernanceZone] = e.getZone("US")

        if(gzUSA is None):
            raise Exception("US zone not found")
        gzUSA.add(
                TeamDeclaration("Test", TestRepository("c"))
        )
        t : Optional[Team] = gzUSA.getTeam("Test")
        if(t is None):
            raise Exception("Team not found")
        
        d1 : Dataset = Dataset("Dataset1",
            DDLTable(
                DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK, NullableStatus.NOT_NULLABLE),
                DDLColumn    ("Col2", String(10)),
                DDLColumn    ("Col3", Date())
                )
            )
        d2 : Dataset = Dataset("Dataset2",
            DDLTable(
                DDLColumn    ("Col1", Integer(), PrimaryKeyStatus.PK, NullableStatus.NOT_NULLABLE),
                DDLColumn    ("Col2", String(10)),
                DDLColumn    ("Col3", Date())
                ),
            DeprecationStatus.DEPRECATED
            )
        
        store : Datastore = Datastore("Store1", d1, d2)
        store.add(CDCCaptureIngestion())
        t.add(store)

        sink : DatasetSink = DatasetSink("Store1", "Dataset2")

        ws : Workspace = Workspace(
            "WK_A", 
            DatasetGroup("FastStuff",
                        WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, DataLatency.SECONDS, None, None)),
                        DatasetSink("Store1", "Dataset1"),
                        sink
                        )
            )

        t.add(ws)

        eTree : ValidationTree = e.lintAndHydrateCaches()
        self.assertTrue(eTree.hasErrors())
        self.assertFalse(eTree.hasIssues())

        # Mark Sink as allowing deprecated datasets and check again
        sink.deprecationsAllowed = DeprecationsAllowed.ALLOWED

        eTree = e.lintAndHydrateCaches()
        eTree.printTree()
        self.assertFalse(eTree.hasErrors())
        self.assertTrue(eTree.hasIssues()) # Workspace using deprecated dataset

        # Move back to clean model
        d2.deprecationStatus = DeprecationStatus.NOT_DEPRECATED
        sink.deprecationsAllowed = DeprecationsAllowed.NEVER
        eTree = e.lintAndHydrateCaches()
        # Verify its clean
        self.assertFalse(eTree.hasErrors())
        self.assertFalse(eTree.hasIssues())

        # Should get warning that a non production store used in a production workspace
        store.productionStatus = ProductionStatus.NOT_PRODUCTION
        ws.productionStatus = ProductionStatus.PRODUCTION
        eTree = e.lintAndHydrateCaches()
        self.assertFalse(eTree.hasErrors())
        self.assertTrue(eTree.hasIssues())

    def test_WorkspaceEquality(self):
        fastP : DataPlatform = DataPlatform("FastPlatform")
        slowP : DataPlatform = DataPlatform("SlowPlatform")

        self.assertEqual(fastP, fastP)
        self.assertNotEqual(fastP, slowP)
        self.assertNotEqual(slowP, fastP)

        w1 : Workspace = Workspace("WK_A",
            DatasetGroup("Fast stuff",
                        WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.LIVE_ONLY, DataLatency.SECONDS, None, None)),
                        DatasetSink("Store1", "Dataset1"),
                        DatasetSink("Store2", "Dataset2")
                        ),
            DatasetGroup("Slow ESMA stuff",
                        WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "ESMA", timedelta(weeks=5*52))),
                        DatasetSink("Store3", "Dataset4"),
                        DatasetSink("Store4", "Dataset5")
                        ),
            DatasetGroup("Slow SEC stuff",
                        WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "SEC", timedelta(weeks=7*52))),
                        DatasetSink("Store3", "Dataset4"),
                        DatasetSink("Store4", "Dataset5")
                        )
            )
        
        self.assertEqual(w1, w1)

        w1_fastP : Optional[WorkspacePlatformConfig] = w1.dsgs["Fast stuff"].platformMD
        w1_slowP : Optional[WorkspacePlatformConfig] = w1.dsgs["Slow ESMA stuff"].platformMD

        if(w1_fastP is None or w1_slowP is None):
            raise Exception("Fast or Slow platform not found")
        retFast : ConsumerRetentionRequirements = w1_fastP.retention
        retSlow : ConsumerRetentionRequirements = w1_slowP.retention

        self.assertEqual(retFast, retFast)
        self.assertEqual(retSlow, retSlow)
        self.assertNotEqual(retFast, retSlow)
        self.assertNotEqual(retSlow, retFast)

        dsg1 : DatasetGroup = w1.dsgs["Fast stuff"]
        dsg2 : DatasetGroup = w1.dsgs["Slow ESMA stuff"]
        dsg3 : DatasetGroup = w1.dsgs["Slow SEC stuff"]
        self.assertIsNotNone(dsg1)
        self.assertIsNotNone(dsg2)
        self.assertIsNotNone(dsg3)

        self.assertEqual(dsg2, dsg2)
        self.assertEqual(dsg3, dsg3)
        self.assertNotEqual(dsg2, dsg3)
        self.assertNotEqual(dsg3, dsg2)
        self.assertNotEqual(dsg1.datasets, dsg2.datasets)
        self.assertEqual(dsg2.datasets, dsg3.datasets)

        # Force dsg3 to be equal to dsg2 and test it
        dsg3.platformMD = WorkspacePlatformConfig(ConsumerRetentionRequirements(DataRetentionPolicy.FORENSIC, DataLatency.MINUTES, "ESMA", timedelta(weeks=5*52)))
        dsg3.name = dsg2.name
        self.assertEqual(dsg2, dsg3)


    def test_TeamEquality(self):
        eco : Ecosystem = self.createEco()

        gzUSA : Optional[GovernanceZone] = eco.getZone("US")
        gzChina : Optional[GovernanceZone] = eco.getZone("China")

        if(gzUSA is None):
            raise Exception("US zone not found")
        if(gzChina is None):
            raise Exception("China zone not found") 
        
        t1 : Optional[Team] = gzUSA.getTeam("Test")
        t2 : Optional[Team] = gzChina.getTeam("China Team")

        self.assertEqual(t1, t1)
        self.assertEqual(t2, t2)
        self.assertNotEqual(t1, t2)
        self.assertNotEqual(t2, t1)

if __name__ == '__main__':
    unittest.main()

