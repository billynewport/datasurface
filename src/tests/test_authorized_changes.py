from typing import List, Optional
import unittest

from datasurface.md.Governance import Ecosystem, GitRepository, Repository, ValidationProblem
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

        # Make copy and change eco name
        e_other : Ecosystem = tests.nwdb.eco.createEcosystem()
        e_other.name = "CHANGE"

        # Should be allowed from eco repo
        val : List[ValidationProblem] = e_main.checkIfChangesAreAuthorized(e_other, gitMain)
        self.assertEqual(len(val), 0)

    def test_changes_are_authorized(self):
#        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
#        gitMain : Repository = e_main.owningRepo
#        gitUSA : Optional[Repository] = e_main.governanceZones["USA"].owningRepo
#        gitEU : Optional[Repository] = e_main.governanceZones["EU"].owningRepo    
        pass


