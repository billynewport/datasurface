import copy
from typing import Optional
import unittest
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.GitOps import GitHubRepository

from datasurface.md.Governance import CloudVendor, Ecosystem, GovernanceZone, InfrastructureLocation, InfrastructureVendor, Repository
from datasurface.md import TeamDeclaration, Team, GovernanceZoneDeclaration
from datasurface.md.Lint import ValidationTree
import tests.nwdb.eco
import tests.nwdb.nwdb


class TestGitEquals(unittest.TestCase):
    def test_git_equals(self):
        r1: Repository = GitHubRepository("repo", "moduleNameA")
        r2: Repository = GitHubRepository("repo", "moduleNameB")
        r3: Repository = GitHubRepository("repoA", "moduleNameA")

        self.assertTrue(r1 == r1)
        self.assertTrue(r2 == r2)
        self.assertFalse(r1 == r2)
        self.assertFalse(r2 == r1)
        self.assertFalse(r1 == r3)
        self.assertFalse(r2 == r3)


class TestEcoNameChange(unittest.TestCase):
    def test_EcoSystem_Name_Change(self):
        e_main: Ecosystem = tests.nwdb.eco.createEcosystem()
        gitMain: Repository = e_main.owningRepo

        e_other: Ecosystem = tests.nwdb.eco.createEcosystem()

        # Check unchanged repo has no problems
        eTree: ValidationTree = ValidationTree(e_main)

        e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
        self.assertFalse(eTree.hasErrors())

        # Change name
        e_other.name = "CHANGE"
        # Should be allowed from eco repo
        eTree = ValidationTree(e_main)
        e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
        eTree.printTree()
        self.assertFalse(eTree.hasErrors())

        # reset

    def test_EcoRepositoryWrong(self):
        eco1: Ecosystem = Ecosystem(
            "AcmeEco",
            GitHubRepository("billynewport/testsurface", "eco_edits")
            )

        eco2 = Ecosystem(
            "AcmeEco",
            GitHubRepository("billynewport/testsurface", "eco_edits"),
            InfrastructureVendor(
                "Azure",
                CloudVendor.AZURE,
                PlainTextDocumentation("Microsoft Azure"),
                InfrastructureLocation(
                    "USA",
                    InfrastructureLocation(
                        "Central",
                        InfrastructureLocation("Central US"),  # Iowa
                        InfrastructureLocation("North Central US"),  # Illinois
                        InfrastructureLocation("South Central US"),  # Texas
                        InfrastructureLocation("West Central US")),  # Wyoming
                    InfrastructureLocation(
                        "East",
                        InfrastructureLocation("East US"),  # Virginia
                        InfrastructureLocation("East US 2"),  # Virginia
                        InfrastructureLocation("East US 3")),  # Georgia
                    InfrastructureLocation(
                        "West",
                        InfrastructureLocation("West US"),  # California
                        InfrastructureLocation("West US 2"),  # Washington
                        InfrastructureLocation("West US 3")))  # Arizona
                )
            )

        tree: ValidationTree = eco1.checkIfChangesCanBeMerged(eco2, GitHubRepository("billynewport/test-surface", "eco_edits"))
        self.assertTrue(tree.hasErrors())

    def test_TeamAuthorization(self):
        """Test that a team can be added to a zone by the gz owning repo, but not by another repo"""

        # Make the baseline ecosystem against which changes are checked
        eco_base: Ecosystem = tests.nwdb.eco.createEcosystem()
        eco_repo: Repository = eco_base.owningRepo

        # Make the ecosystem which has the changed to check if authorized against
        # the baseline ecosystem
        e_head: Ecosystem = tests.nwdb.eco.createEcosystem()

        # Check unchanged repo has no problems
        eTree: ValidationTree = ValidationTree(eco_base)
        eco_base.checkIfChangesAreAuthorized(e_head, eco_repo, eTree)
        self.assertFalse(eTree.hasErrors())

        # Get the USA zone so we can change it
        headGzUSA: GovernanceZone = e_head.getZoneOrThrow("USA")

        # NewTeam should not exist
        t: Optional[Team] = headGzUSA.getTeam("NewTeam")
        self.assertIsNone(t)

        # Authorize a new team with its repo. Team has to be authorized
        # by the gz repo and later edited with the team repo. Must be 2 steps
        headGzUSA.add(TeamDeclaration("NewTeam", GitHubRepository("owner/repo", "main")))

        # Should not be allowed from eco repo
        eTree = ValidationTree(e_head)
        eco_base.checkIfChangesAreAuthorized(e_head, eco_repo, eTree)
        self.assertTrue(eTree.hasErrors())

        # Should be allowed from gz repo
        eTree = ValidationTree(e_head)
        eco_base.checkIfChangesAreAuthorized(e_head, headGzUSA.owningRepo, eTree)
        self.assertFalse(eTree.hasErrors())

        # reset baseline ecosystem
        eco_base = e_head
        e_head = copy.deepcopy(eco_base)

        headGzUSA = e_head.zones.authorizedObjects["USA"]
        self.assertIsNotNone(headGzUSA)

        # This creates a team object and is the first team specific change that
        # needs to be authorized. The team cannot be defined and authorized in
        # one step. Authorize using gz repo, then create/edit using team repo
        t = headGzUSA.getTeamOrThrow("NewTeam")

        # Should not be allowed from other repo
        eTree = ValidationTree(e_head)
        eco_base.checkIfChangesAreAuthorized(e_head, eco_repo, eTree)
        self.assertTrue(eTree.hasErrors())

        # Should be allowed from team repo
        eTree = ValidationTree(e_head)
        eco_base.checkIfChangesAreAuthorized(e_head, t.owningRepo, eTree)
        self.assertFalse(eTree.hasErrors())

        # Now verify that the new team can only be changed from its owning repo which can
        # be different from the owning repo of the zone

        # Change the baseline to the repo with the added team
        eco_base = e_head

        # Make a new ecosystem to check against the baseline
        e_head = copy.deepcopy(eco_base)

        # Get the USA zone so we can change it
        headGzUSA = e_head.zones.authorizedObjects["USA"]
        newTeam: Team = headGzUSA.getTeamOrThrow("NewTeam")

        # Add tables to the new team
        tests.nwdb.nwdb.defineTables(e_head, headGzUSA, newTeam)

        # Check the unchanged team has no changes
        eTree = ValidationTree(eco_base)
        eco_base.checkIfChangesAreAuthorized(eco_base, newTeam.owningRepo, eTree)
        self.assertFalse(eTree.hasErrors())

        eTree = ValidationTree(eco_base)
        eco_base.checkIfChangesAreAuthorized(e_head, newTeam.owningRepo, eTree)
        self.assertFalse(eTree.hasErrors())

        # Check other repos cannot edit team
        eTree = ValidationTree(eco_base)
        eco_base.checkIfChangesAreAuthorized(e_head, eco_base.owningRepo, eTree)
        self.assertTrue(eTree.hasErrors())

    def test_checkZoneRemoval(self):
        # Check that a gz can only be removed by the gz owning repo
        e_main: Ecosystem = tests.nwdb.eco.createEcosystem()
        gitMain: Repository = e_main.owningRepo

        e_other: Ecosystem = tests.nwdb.eco.createEcosystem()

        # Remove USA zone
        gitUSA: Optional[Repository] = e_other.zones.authorizedObjects["USA"].owningRepo
        self.assertIsNotNone(gitUSA)
        if (gitUSA):
            # Only gitUSA can remove the zone definition
            e_other.zones.removeDefinition("USA")  # Still authorized but definition is gone
            eTree = ValidationTree(e_main)
            e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
            self.assertTrue(eTree.hasErrors())
            eTree = ValidationTree(e_main)
            e_main.checkIfChangesAreAuthorized(e_other, gitUSA, eTree)
            self.assertFalse(eTree.hasErrors())

            e_main = e_other  # Promote definition less ecosystem to main ecosystem
            e_other = copy.deepcopy(e_main)
            e_other.zones.removeAuthorization("USA")  # Remove authorization

            eTree = ValidationTree(e_main)
            e_main.checkIfChangesAreAuthorized(e_other, gitMain, eTree)
            self.assertFalse(eTree.hasErrors())

            eTree = ValidationTree(e_main)
            e_main.checkIfChangesAreAuthorized(e_other, gitUSA, eTree)
            self.assertTrue(eTree.hasErrors())

    def test_checkZoneAddition(self):

        e_main: Ecosystem = tests.nwdb.eco.createEcosystem()
        gitMain: Repository = e_main.owningRepo

        e_other: Ecosystem = tests.nwdb.eco.createEcosystem()

        # First, add authorization for china gz
        gitChina: Repository = GitHubRepository("ssh://u@local:/v1/source/gz_china", "main")
        e_other.add(GovernanceZoneDeclaration("China", gitChina))

        # eco repo can add a zone authorization
        eTree: ValidationTree = ValidationTree(e_main)
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
        gzChina: Optional[GovernanceZone] = e_other.getZone("China")
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
        #        e_main: Ecosystem = tests.nwdb.eco.createEcosystem()
        #        gitMain: Repository = e_main.owningRepo
        #        gitUSA: Optional[Repository] = e_main.governanceZones["USA"].owningRepo
        #        gitEU: Optional[Repository] = e_main.governanceZones["EU"].owningRepo
        pass
