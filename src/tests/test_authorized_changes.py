import copy
from typing import List, Optional
import unittest

from datasurface.md.Governance import Ecosystem, GitRepository, GovernanceZone, Repository, ValidationProblem
from datasurface.md import TeamDeclaration, Team, GovernanceZoneDeclaration
import tests.nwdb.eco
import tests.nwdb.nwdb


class TestGitEquals(unittest.TestCase):
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

    
class TestEcoNameChange(unittest.TestCase):
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

    def test_TeamAuthorization(self):
        """Test that a team can be added to a zone by the gz owning repo, but not by another repo"""

        # Make the baseline ecosystem against which changes are checked
        eco_baseline : Ecosystem = tests.nwdb.eco.createEcosystem()
        eco_repo : Repository = eco_baseline.owningRepo

        # Make the ecosystem which has the changed to check if authorized against
        # the baseline ecosystem
        e_other : Ecosystem = tests.nwdb.eco.createEcosystem()

        # Check unchanged repo has no problems
        val : List[ValidationProblem] = eco_baseline.checkIfChangesAreAuthorized(e_other, eco_repo)
        self.assertEqual(len(val), 0)

        # Get the USA zone so we can change it
        gzUSA : Optional[GovernanceZone] = e_other.zones.authorizedObjects["USA"]
        self.assertIsNotNone(gzUSA)

        # NewTeam should not exist
        t : Optional[Team] = gzUSA.getTeam("NewTeam")
        self.assertIsNone(t)

        # Authorize a new team with its repo. Team has to be authorized
        # by the gz repo and later edited with the team repo. Must be 2 steps
        gzUSA.add(TeamDeclaration("NewTeam", GitRepository("ssh://u@local:/v1/source/nt", "newTeamRepo")))

        # Should be allowed from gz repo
        val = eco_baseline.checkIfChangesAreAuthorized(e_other, gzUSA.owningRepo)
        self.assertEqual(len(val), 0)

        # reset baseline ecosystem
        eco_baseline = e_other
        e_other = copy.deepcopy(eco_baseline)

        gzUSA = e_other.zones.authorizedObjects["USA"]
        self.assertIsNotNone(gzUSA)

        # This creates a team object and is the first team specific change that
        # needs to be authorized. The team cannot be defined and authorized in
        # one step. Authorize using gz repo, then create/edit using team repo
        t = gzUSA.getTeam("NewTeam")
        self.assertIsNotNone(t)
        if(t == None):
            raise Exception("NewTeam should not be None")

        # Should not be allowed from other repo
        val = eco_baseline.checkIfChangesAreAuthorized(e_other, eco_repo)
        self.assertNotEqual(len(val), 0)

        # Should be allowed from team repo
        val = eco_baseline.checkIfChangesAreAuthorized(e_other, t.owningRepo)

        # Now verify that the new team can only be changed from its owning repo which can 
        # be different from the owning repo of the zone

        # Change the baseline to the repo with the added team
        eco_baseline = e_other

        # Make a new ecosystem to check against the baseline
        e_other = copy.deepcopy(eco_baseline)

        # Get the USA zone so we can change it
        gzUSA = e_other.zones.authorizedObjects["USA"]
        newTeam : Optional[Team] = gzUSA.getTeam("NewTeam")

        self.assertIsNotNone(newTeam)
        if(newTeam == None):
            raise Exception("NewTeam should not be None")
        
        # Add tables to the new team
        tests.nwdb.nwdb.defineTables(newTeam)

        # Check the unchanged team has no changes
        val = eco_baseline.checkIfChangesAreAuthorized(eco_baseline, newTeam.owningRepo)
        self.assertEqual(len(val), 0)

        val = eco_baseline.checkIfChangesAreAuthorized(e_other, newTeam.owningRepo)
        self.assertEqual(len(val), 0)

        # Check other repos cannot edit team
        val = eco_baseline.checkIfChangesAreAuthorized(e_other, eco_baseline.owningRepo)
        self.assertNotEqual(len(val), 0)


    def test_checkZoneRemoval(self):
        # Check that a gz can only be removed by the gz owning repo
        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
        gitMain : Repository = e_main.owningRepo

        e_other : Ecosystem = tests.nwdb.eco.createEcosystem()

        # Remove USA zone
        gitUSA : Optional[Repository] = e_other.zones.authorizedObjects["USA"].owningRepo
        self.assertIsNotNone(gitUSA)
        if(gitUSA):
            # Only gitUSA can remove the zone definition
            e_other.zones.removeDefinition("USA") # Still authorized but definition is gone
            val = e_main.checkIfChangesAreAuthorized(e_other, gitMain)
            self.assertNotEqual(len(val), 0, "Zone cannot be removed by non-owning repo")
            val = e_main.checkIfChangesAreAuthorized(e_other, gitUSA)
            self.assertEqual(len(val), 0, "Zone can be removed by owning repo")

            e_main = e_other # Promote definition less ecosystem to main ecosystem
            e_other = copy.deepcopy(e_main)
            e_other.zones.removeAuthorization("USA") # Remove authorization

            val = e_main.checkIfChangesAreAuthorized(e_other, gitMain)
            self.assertEqual(len(val), 0, "Zone cannot be unauthorized by non-owning repo")

            val = e_main.checkIfChangesAreAuthorized(e_other, gitUSA)
            self.assertNotEqual(len(val), 0, "Zone can be unauthorized by owning repo")

    def test_checkZoneAddition(self):

        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
        gitMain : Repository = e_main.owningRepo
        
        e_other : Ecosystem = tests.nwdb.eco.createEcosystem()

        # First, add authorization for china gz
        gitChina : Repository = GitRepository("ssh://u@local:/v1/source/gz_china", "main")
        e_other.add(GovernanceZoneDeclaration("China", gitChina))

        # eco repo can add a zone authorization
        val = e_main.checkIfChangesAreAuthorized(e_other, gitMain)
        self.assertEqual(len(val), 0, "Zone can be added by owning repo")

        # check that other repo cannot add a zone authorization
        val = e_main.checkIfChangesAreAuthorized(e_other, gitChina)
        self.assertNotEqual(len(val), 0, "Zone cannot be added by non-owning repo")

        # promote the ecosystem to main
        e_main = e_other
        e_other = copy.deepcopy(e_main)

        # Now add the zone definition
        gzChina : Optional[GovernanceZone] = e_other.getZone("China")
        self.assertIsNotNone(gzChina)

        # Check the china repo CAN add the zone definition
        val = e_main.checkIfChangesAreAuthorized(e_other, gitChina)
        self.assertEqual(len(val), 0, "Zone cannot be added by non-owning repo")

        # Check the eco repo CANNOT add the zone definition
        val = e_main.checkIfChangesAreAuthorized(e_other, gitMain)
        self.assertNotEqual(len(val), 0, "Zone cannot be added by non-owning repo")

    def test_changes_are_authorized(self):
#        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
#        gitMain : Repository = e_main.owningRepo
#        gitUSA : Optional[Repository] = e_main.governanceZones["USA"].owningRepo
#        gitEU : Optional[Repository] = e_main.governanceZones["EU"].owningRepo    
        pass


