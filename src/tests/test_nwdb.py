import unittest
from datasurface.md.Governance import GitHubRepository, Repository
from datasurface.md.Lint import ValidationTree
from datasurface.md.Schema import IEEE128, IEEE16, IEEE32, IEEE64, DDLColumn, DataType, Date, Decimal, NullableStatus, PrimaryKeyStatus, String, Vector
import tests.nwdb.eco
from datasurface.md import Ecosystem



class TestEcosystemValidation(unittest.TestCase):

    def test_validate_nwdb(self):
        e : Ecosystem = tests.nwdb.eco.createEcosystem()

        rc : ValidationTree = e.lintAndHydrateCaches()
        rc.printTree()
        self.assertFalse(rc.hasErrors())
        self.assertFalse(rc.hasIssues())

    def test_nwdb_has_all_softlinks(self):
        e : Ecosystem = tests.nwdb.eco.createEcosystem()

        self.assertEqual(e, e)

        rc : ValidationTree = e.lintAndHydrateCaches()
        rc.printTree()
        self.assertFalse(rc.hasErrors())

        # Verify that all softlinks are in the ecosystem
        self.assertIsNotNone(e.key)
        if(e.zones.authorizedObjects):

            for gz in e.zones.authorizedObjects.values():
                for sp in gz.storagePolicies.values():
                    self.assertIsNotNone(sp.key)
                self.assertIsNotNone(gz.key)
                for iv in gz.vendors.values():
                    self.assertIsNotNone(iv.key)
                    for loc in iv.locations.values():
                        self.assertIsNotNone(loc.key)
                for td in gz.teams.authorizedNames.values():
                    self.assertIsNotNone(td.key)

            
    def test_validate_columns(self):
        col : DDLColumn = DDLColumn("col1", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)
        tree : ValidationTree = ValidationTree(col)
        col.lint(tree)
        self.assertFalse(tree.hasErrors())

        # Test cases where the column is not valid
        col.name = "col 1" # Not ANSI SQL Identifier
        tree : ValidationTree = ValidationTree(col)
        col.lint(tree)
        self.assertEqual(len(tree.problems), 1)
        self.assertTrue(tree.hasErrors())

        # Test cases where the column is not valid

    def assertOneIssue(self, o : DataType):
        tree : ValidationTree = ValidationTree(o)
        o.lint(tree)
        self.assertEqual(len(tree.problems), 1)
        self.assertTrue(tree.hasErrors())

    def assertNoIssue(self, o : DataType):
        tree : ValidationTree = ValidationTree(o)
        o.lint(tree)
        self.assertFalse(tree.hasErrors())

    def test_lint_datatypes(self):
        self.assertNoIssue(String(20))

        self.assertOneIssue(String(0)) # String length must be > 0
        self.assertOneIssue(Decimal(10, -1)) # Scale < 0
        self.assertOneIssue(Decimal(10, 11)) # Precision > scale
        self.assertNoIssue(Decimal(10, 0)) # Scale == 0 is ok

        self.assertNoIssue(Vector(10))
        self.assertOneIssue(Vector(0)) # Vector length must be > 0

        self.assertNoIssue(IEEE128())
        self.assertNoIssue(IEEE64())
        self.assertNoIssue(IEEE32())
        self.assertNoIssue(IEEE16())

        self.assertNoIssue(Date())

    
def test_eq_ecosystem():
    e : Ecosystem = tests.nwdb.eco.createEcosystem()
    e2 : Ecosystem = tests.nwdb.eco.createEcosystem()

    diffR : Repository = GitHubRepository("ssh://u@local:/v1/source/eco", "main_other")

    assert e == e2

    # No changes
    problems : ValidationTree = ValidationTree(e)
    e.checkIfChangesAreAuthorized(e2, e.owningRepo, problems)
    assert problems.hasErrors() == False

    e2.name = "Test2"
    # Test name cannot be changed from another repo
    # Verify they are not equal, the name was changed
    assert e != e2

    # Verify that the change is not authorized
    problems = ValidationTree(e)
    e.checkIfChangesAreAuthorized(e2, diffR, problems)
    assert problems.hasErrors()

    e2 : Ecosystem = tests.nwdb.eco.createEcosystem()

    assert e == e2
    e2.zones.removeDefinition("USA")
    assert e != e2

