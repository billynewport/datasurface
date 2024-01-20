import copy
from typing import Optional
import unittest

from datasurface.md.Governance import Ecosystem, GitHubRepository, GovernanceZone, Repository
from datasurface.md import TeamDeclaration, Team, GovernanceZoneDeclaration
from datasurface.md.Lint import ValidationTree
import tests.nwdb.eco
import tests.nwdb.nwdb


class TestGitEquals(unittest.TestCase):
    def test_git_equals(self):
        r1 : Repository = GitHubRepository("repo", "moduleNameA")
        r2 : Repository = GitHubRepository("repo", "moduleNameB")
        r3 : Repository = GitHubRepository("repoA", "moduleNameA")

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
        eTree : ValidationTree = ValidationTree(e_main)
        
        e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
        self.assertFalse(eTree.hasErrors())

        # Change name
        e_other.name = "CHANGE"
        # Should be allowed from eco repo
        eTree = ValidationTree(e_main)
        e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
        self.assertFalse(eTree.hasErrors())

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
        eTree : ValidationTree = ValidationTree(eco_baseline)
        eco_baseline.checkIfChangesAreAuthorized(e_other, eco_repo, eTree)
        self.assertFalse(eTree.hasErrors())

        # Get the USA zone so we can change it
        gzUSA : GovernanceZone = e_other.getZoneOrThrow("USA")

        # NewTeam should not exist
        t : Optional[Team] = gzUSA.getTeam("NewTeam")
        self.assertIsNone(t)

        # Authorize a new team with its repo. Team has to be authorized
        # by the gz repo and later edited with the team repo. Must be 2 steps
        gzUSA.add(TeamDeclaration("NewTeam", GitHubRepository("ssh://u@local:/v1/source/nt", "newTeamRepo")))

        # Should be allowed from gz repo
        eTree = ValidationTree(e_other)
        eco_baseline.checkIfChangesAreAuthorized(e_other, gzUSA.owningRepo, eTree)
        self.assertFalse(eTree.hasErrors())

        # reset baseline ecosystem
        eco_baseline = e_other
        e_other = copy.deepcopy(eco_baseline)

        gzUSA = e_other.zones.authorizedObjects["USA"]
        self.assertIsNotNone(gzUSA)

        # This creates a team object and is the first team specific change that
        # needs to be authorized. The team cannot be defined and authorized in
        # one step. Authorize using gz repo, then create/edit using team repo
        t = gzUSA.getTeamOrThrow("NewTeam")

        # Should not be allowed from other repo
        eTree = ValidationTree(e_other)
        eco_baseline.checkIfChangesAreAuthorized(e_other, eco_repo, eTree)
        self.assertTrue(eTree.hasErrors())

        # Should be allowed from team repo
        eTree = ValidationTree(e_other)
        eco_baseline.checkIfChangesAreAuthorized(e_other, t.owningRepo, eTree)
        self.assertFalse(eTree.hasErrors())

        # Now verify that the new team can only be changed from its owning repo which can 
        # be different from the owning repo of the zone

        # Change the baseline to the repo with the added team
        eco_baseline = e_other

        # Make a new ecosystem to check against the baseline
        e_other = copy.deepcopy(eco_baseline)

        # Get the USA zone so we can change it
        gzUSA = e_other.zones.authorizedObjects["USA"]
        newTeam : Team = gzUSA.getTeamOrThrow("NewTeam")

        # Add tables to the new team
        tests.nwdb.nwdb.defineTables(newTeam)

        # Check the unchanged team has no changes
        eTree = ValidationTree(eco_baseline)
        eco_baseline.checkIfChangesAreAuthorized(eco_baseline, newTeam.owningRepo, eTree)
        self.assertFalse(eTree.hasErrors())

        eTree = ValidationTree(eco_baseline)
        eco_baseline.checkIfChangesAreAuthorized(e_other, newTeam.owningRepo, eTree)
        self.assertFalse(eTree.hasErrors())

        # Check other repos cannot edit team
        eTree = ValidationTree(eco_baseline)
        eco_baseline.checkIfChangesAreAuthorized(e_other, eco_baseline.owningRepo, eTree)
        self.assertTrue(eTree.hasErrors())

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
            eTree = ValidationTree(e_main)
            e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
            self.assertTrue(eTree.hasErrors())
            eTree = ValidationTree(e_main)
            e_main.checkIfChangesAreAuthorized(e_other, gitUSA, eTree)
            self.assertFalse(eTree.hasErrors())

            e_main = e_other # Promote definition less ecosystem to main ecosystem
            e_other = copy.deepcopy(e_main)
            e_other.zones.removeAuthorization("USA") # Remove authorization

            eTree = ValidationTree(e_main)
            e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
            self.assertFalse(eTree.hasErrors())

            eTree = ValidationTree(e_main)
            e_main.checkIfChangesAreAuthorized(e_other, gitUSA, eTree)
            self.assertTrue(eTree.hasErrors())

    def test_checkZoneAddition(self):

        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
        gitMain : Repository = e_main.owningRepo
        
        e_other : Ecosystem = tests.nwdb.eco.createEcosystem()

        # First, add authorization for china gz
        gitChina : Repository = GitHubRepository("ssh://u@local:/v1/source/gz_china", "main")
        e_other.add(GovernanceZoneDeclaration("China", gitChina))

        # eco repo can add a zone authorization
        eTree : ValidationTree = ValidationTree(e_main)
        e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
        self.assertFalse(eTree.hasErrors())

        # check that other repo cannot add a zone authorization
        eTree = ValidationTree(e_main)
        e_main.checkIfChangesAreAuthorized(e_other, gitChina, eTree)
        self.assertTrue(eTree.hasErrors())

        # promote the ecosystem to main
        e_main = e_other
        e_other = copy.deepcopy(e_main)

        # Now add the zone definition
        gzChina : Optional[GovernanceZone] = e_other.getZone("China")
        self.assertIsNotNone(gzChina)

        # Check the china repo CAN add the zone definition
        eTree = ValidationTree(e_main)
        e_main.checkIfChangesAreAuthorized(e_other, gitChina, eTree)
        self.assertFalse(eTree.hasErrors())

        # Check the eco repo CANNOT add the zone definition
        eTree = ValidationTree(e_main)
        e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
        self.assertTrue(eTree.hasErrors())

    def test_changes_are_authorized(self):
#        e_main : Ecosystem = tests.nwdb.eco.createEcosystem()
#        gitMain : Repository = e_main.owningRepo
#        gitUSA : Optional[Repository] = e_main.governanceZones["USA"].owningRepo
#        gitEU : Optional[Repository] = e_main.governanceZones["EU"].owningRepo    
        pass


