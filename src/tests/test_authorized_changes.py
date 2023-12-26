from typing import List, Optional
import unittest

from datasurface.md.Governance import Ecosystem, GitRepository, GovernanceZone, Repository, ValidationProblem
import tests.nwdb.eco


class TestGitReopen(unittest.TestCase):
    def test_git_equals(self):
        r1 : Repository = GitRepository("repo", "moduleNameA")
        r2 : Repository = GitRepository("repo", "moduleNameB")
        r3 : Repository = GitRepository("repoA", "moduleNameA")

        self.assertTrue(r1 == r1)
        self.assertTrue(r2 == r2)
        self.assertFalse(r1 == r2)
        self.assertFalse(r2 == r1)
        self.assertFalse(r1 == r3)
        self.assertFalse(r2 == r3)

    
class TestChangesAreAuthorized(unittest.TestCase):
    def test_EcoSystem_Name_Change(self):
        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
        gitMain : Repository = e_main.owningRepo

        e_other : Ecosystem = tests.nwdb.eco.createEcosystem()

        # Check unchanged repo has no problems
        val : List[ValidationProblem] = e_main.checkIfChangesAreAuthorized(e_other, gitMain)
        self.assertEqual(len(val), 0)

        # Change name
        e_other.name = "CHANGE"
        # Should be allowed from eco repo
        val = e_main.checkIfChangesAreAuthorized(e_other, gitMain)
        self.assertEqual(len(val), 0)

        # reset

    def test_checkZoneRemoval(self):
        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
        gitMain : Repository = e_main.owningRepo

        e_other : Ecosystem = tests.nwdb.eco.createEcosystem()

        # Remove USA zone
        gitUSA : Optional[Repository] = e_other.governanceZones["USA"].owningRepo
        self.assertIsNotNone(gitUSA)
        if(gitUSA):
            e_other.governanceZones.pop("USA")
            val = e_main.checkIfChangesAreAuthorized(e_other, gitMain)
            self.assertNotEqual(len(val), 0, "Zone cannot be removed by non-owning repo")
            val = e_main.checkIfChangesAreAuthorized(e_other, gitUSA)
            self.assertEqual(len(val), 0, "Zone can be removed by owning repo")

    def test_checkZoneAddition(self):

        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
        gitMain : Repository = e_main.owningRepo
        
        e_other : Ecosystem = tests.nwdb.eco.createEcosystem()

        # Remove USA zone
        gitUSA : Optional[Repository] = e_other.governanceZones["USA"].owningRepo
        self.assertIsNotNone(gitUSA)
        if(gitUSA):
            gzUSA : Optional[GovernanceZone] = e_other.governanceZones.pop("USA")
            val = e_other.checkIfChangesAreAuthorized(e_main, gitMain)
            self.assertNotEqual(len(val), 0, "Zone cannot be added by non-owning repo")
            val = e_other.checkIfChangesAreAuthorized(e_main, gitUSA)
            self.assertEqual(len(val), 0, "Zone can be added by owning repo")

    def test_changes_are_authorized(self):
#        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
#        gitMain : Repository = e_main.owningRepo
#        gitUSA : Optional[Repository] = e_main.governanceZones["USA"].owningRepo
#        gitEU : Optional[Repository] = e_main.governanceZones["EU"].owningRepo    
        pass


