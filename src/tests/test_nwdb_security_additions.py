"""
Additional security tests for test_nwdb.py authorization coverage.

These tests address critical security gaps in the current GitControlledObject authorization testing.
They should be integrated into the main test_nwdb.py file to ensure unauthorized repos cannot
make changes to objects they don't own.

// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import copy
import unittest
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.md import ValidationTree, TeamDeclaration
import tests.nwdb.eco


class TestCriticalAuthorizationGaps(unittest.TestCase):
    """Critical authorization tests missing from current test_nwdb.py"""

    def test_unauthorized_zone_ownership_change(self) -> None:
        """CRITICAL: Test that zones cannot change ownership to unauthorized repos."""
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)

        # Attempt to change USA zone ownership to attacker repo
        attacker_repo = GitHubRepository("attacker/malicious", "main")
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        usa_zone.owningRepo = attacker_repo

        # This should fail - no repo should be able to change ownership
        problems = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, attacker_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "SECURITY FAILURE: Zone ownership change should be blocked")

        # Even legitimate repo should not be able to change its own ownership
        problems2 = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, eco_main.owningRepo, problems2)

        self.assertTrue(problems2.hasErrors(),
                        "SECURITY FAILURE: Even legitimate repo should not change ownership")

    def test_team_authorization_hijack_attempt(self) -> None:
        """CRITICAL: Test team authorization cannot be hijacked by unauthorized repos."""
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)

        # Add legitimate team
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        legitimate_repo = GitHubRepository("legitimate/team", "main")
        usa_zone.add(TeamDeclaration("LegitTeam", legitimate_repo))

        # Update baseline
        eco_main = copy.deepcopy(eco_proposed)
        eco_proposed = copy.deepcopy(eco_main)

        # Attempt to change team authorization to attacker repo
        attacker_repo = GitHubRepository("attacker/malicious", "main")
        usa_zone_proposed = eco_proposed.getZoneOrThrow("USA")
        usa_zone_proposed.teams.authorizedNames["LegitTeam"] = TeamDeclaration("LegitTeam", attacker_repo)

        # This should fail from attacker repo
        problems = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, attacker_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "SECURITY FAILURE: Team authorization hijack should be blocked")

        # Should also fail from ecosystem repo (wrong level)
        problems2 = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, eco_main.owningRepo, problems2)

        self.assertTrue(problems2.hasErrors(),
                        "SECURITY FAILURE: Ecosystem repo should not control team authorization")

    def test_cross_zone_unauthorized_changes(self) -> None:
        """CRITICAL: Test that one zone cannot modify another zone."""
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)

        # Get different zone repos
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        eu_zone = eco_proposed.getZoneOrThrow("EU")
        usa_repo = usa_zone.owningRepo

        # Try to have USA zone modify EU zone
        eu_zone.add(TeamDeclaration("USAControlledTeam", usa_repo))

        # USA repo should not be able to modify EU zone
        problems = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, usa_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "SECURITY FAILURE: Cross-zone modifications should be blocked")

    def test_repository_spoofing_prevention(self) -> None:
        """CRITICAL: Test prevention of repository spoofing attacks."""
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)

        # Get legitimate repo details
        legit_repo = eco_main.owningRepo
        if isinstance(legit_repo, GitHubRepository):
            # Create spoofed repositories
            spoofed_repos = [
                GitHubRepository(legit_repo.repositoryName.upper(), legit_repo.branchName),  # Case change
                GitHubRepository(legit_repo.repositoryName + "x", legit_repo.branchName),     # Extra char
                GitHubRepository(legit_repo.repositoryName, legit_repo.branchName + "_fake"),  # Fake branch
            ]

            # Try to make changes with spoofed repos
            eco_proposed.name = "Spoofed Change"

            for spoofed_repo in spoofed_repos:
                with self.subTest(spoofed_repo=str(spoofed_repo)):
                    problems = ValidationTree(eco_main)
                    eco_main.checkIfChangesAreAuthorized(eco_proposed, spoofed_repo, problems)

                    self.assertTrue(problems.hasErrors(),
                                    f"SECURITY FAILURE: Spoofed repo {spoofed_repo} should be rejected")

    def test_null_repository_attack_prevention(self) -> None:
        """CRITICAL: Test handling of null/None repository attacks."""
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)
        eco_proposed.name = "Null Attack"

        # Test None changeSource
        problems = ValidationTree(eco_main)
        try:
            eco_main.checkIfChangesAreAuthorized(eco_proposed, None, problems)  # type: ignore
            self.assertTrue(problems.hasErrors(),
                            "SECURITY FAILURE: None changeSource should be rejected")
        except (TypeError, AttributeError):
            # Exception is acceptable - prevents attack
            pass

    def test_deep_object_unauthorized_modification(self) -> None:
        """CRITICAL: Test that deep objects cannot be modified by unauthorized repos."""
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)

        # Add team through proper authorization chain
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        team_repo = GitHubRepository("team/repo", "main")
        usa_zone.add(TeamDeclaration("TestTeam", team_repo))

        # Update baseline
        eco_main = copy.deepcopy(eco_proposed)
        eco_proposed = copy.deepcopy(eco_main)

        # Get team and try to modify it from unauthorized repo
        usa_zone_proposed = eco_proposed.getZoneOrThrow("USA")
        test_team = usa_zone_proposed.getTeamOrThrow("TestTeam")
        test_team.documentation = PlainTextDocumentation("Unauthorized change")

        # Attacker repo should not be able to modify team
        attacker_repo = GitHubRepository("attacker/malicious", "main")
        problems = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, attacker_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "SECURITY FAILURE: Deep object modification should require proper authorization")

        # Ecosystem repo should also not be able to modify team
        problems2 = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, eco_main.owningRepo, problems2)

        self.assertTrue(problems2.hasErrors(),
                        "SECURITY FAILURE: Ecosystem repo should not control team objects")


class TestAuthorizationBoundaryViolations(unittest.TestCase):
    """Test violations of authorization boundaries."""

    def test_ecosystem_repo_cannot_control_zones(self) -> None:
        """Test that ecosystem repo cannot directly control zone objects."""
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)

        # Try to modify zone object from ecosystem repo
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        usa_zone.add(TeamDeclaration("EcoControlledTeam", eco_main.owningRepo))

        # Ecosystem repo should not be able to control zone contents
        problems = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, eco_main.owningRepo, problems)

        self.assertTrue(problems.hasErrors(),
                        "Ecosystem repo should not directly control zone contents")

    def test_zone_repo_cannot_control_other_zones(self) -> None:
        """Test that zone repos cannot control other zones."""
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)

        # Get zone repos
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        eu_zone = eco_proposed.getZoneOrThrow("EU")
        usa_repo = usa_zone.owningRepo

        # Try to have USA repo modify EU zone
        eu_zone.add(TeamDeclaration("USAInjectedTeam", usa_repo))

        problems = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, usa_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "Zone repo should not control other zones")

    def test_team_repo_cannot_control_zone(self) -> None:
        """Test that team repos cannot control their parent zones."""
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)

        # Add team
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        team_repo = GitHubRepository("team/repo", "main")
        usa_zone.add(TeamDeclaration("TestTeam", team_repo))

        # Update baseline
        eco_main = copy.deepcopy(eco_proposed)
        eco_proposed = copy.deepcopy(eco_main)

        # Try to have team repo modify zone
        usa_zone_proposed = eco_proposed.getZoneOrThrow("USA")
        usa_zone_proposed.add(TeamDeclaration("TeamAddedTeam", team_repo))

        problems = ValidationTree(eco_main)
        eco_main.checkIfChangesAreAuthorized(eco_proposed, team_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "Team repo should not control zone authorization")


if __name__ == '__main__':
    unittest.main()