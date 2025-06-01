"""
Security tests for GitControlledObject authorization system.

This module contains comprehensive tests for potential security vulnerabilities:
- Object replacement attacks
- Repository spoofing attempts
- Null/malformed repository handling
- Type confusion attacks
- Hierarchical authorization bypass
- Edge cases in authorization logic

// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import copy
import unittest

from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository, FakeRepository
from datasurface.md import GovernanceZone, TeamDeclaration, ValidationTree
import tests.nwdb.eco


class TestObjectReplacementAttacks(unittest.TestCase):
    """Test attacks where objects are replaced with malicious ones."""

    def setUp(self) -> None:
        """Set up test ecosystem."""
        self.eco_main = tests.nwdb.eco.createEcosystem()
        self.legitimate_repo = GitHubRepository("billynewport/legitimate", "main")
        self.attacker_repo = GitHubRepository("attacker/malicious", "main")

    def test_zone_replacement_with_different_owner(self) -> None:
        """Test replacing a zone with same name but different owning repository."""
        eco_proposed = copy.deepcopy(self.eco_main)

        # Get the USA zone
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        original_repo = usa_zone.owningRepo

        # Create a malicious replacement zone with same name but different repo
        malicious_zone = GovernanceZone("USA", self.attacker_repo)

        # Replace the zone in the proposed ecosystem
        eco_proposed.zones.authorizedObjects["USA"] = malicious_zone

        # Check authorization - this should fail
        problems = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.attacker_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "Replacing zone with different owner should be unauthorized")

        # Also test with the original repo - should also fail since it's trying to change ownership
        problems2 = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, original_repo, problems2)

        self.assertTrue(problems2.hasErrors(),
                        "Original repo should not be able to change zone ownership")

    def test_team_replacement_attack(self) -> None:
        """Test replacing a team with same name but different repository."""
        eco_proposed = copy.deepcopy(self.eco_main)

        # Add a team first
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        usa_zone.add(TeamDeclaration("TestTeam", self.legitimate_repo))

        # Update main ecosystem
        self.eco_main = copy.deepcopy(eco_proposed)
        eco_proposed = copy.deepcopy(self.eco_main)

        # Create malicious team with same name but different repo
        usa_zone_proposed = eco_proposed.getZoneOrThrow("USA")
        malicious_team_decl = TeamDeclaration("TestTeam", self.attacker_repo)

        # Replace in authorized names
        usa_zone_proposed.teams.authorizedNames["TestTeam"] = malicious_team_decl

        problems = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.attacker_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "Team replacement with different repo should be unauthorized")

    def test_ecosystem_ownership_hijack_attempt(self) -> None:
        """Test attempting to change ecosystem ownership."""
        eco_proposed = copy.deepcopy(self.eco_main)

        # Try to change the ecosystem's owning repository
        eco_proposed.owningRepo = self.attacker_repo

        # Test with attacker repo
        problems = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.attacker_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "Attacker should not be able to change ecosystem ownership")

        # Test with legitimate repo - should be allowed since owner can change ownership
        problems2 = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.eco_main.owningRepo, problems2)

        self.assertFalse(problems2.hasErrors(),
                         "Legitimate repo CAN change its own ownership")


class TestNullAndMalformedRepositories(unittest.TestCase):
    """Test handling of null, None, or malformed repositories."""

    def setUp(self) -> None:
        """Set up test ecosystem."""
        self.eco_main = tests.nwdb.eco.createEcosystem()

    def test_null_change_source(self) -> None:
        """Test authorization with None changeSource."""
        eco_proposed = copy.deepcopy(self.eco_main)
        eco_proposed.name = "Changed"

        problems = ValidationTree(self.eco_main)

        # This should handle None gracefully
        try:
            self.eco_main.checkIfChangesAreAuthorized(eco_proposed, None, problems)  # type: ignore
            # Should have errors due to None changeSource
            self.assertTrue(problems.hasErrors(),
                            "None changeSource should cause authorization failure")
        except (TypeError, AttributeError):
            # If it throws an exception, that's also a valid response
            pass

    def test_malformed_repository_names(self) -> None:
        """Test with malformed repository names."""
        malformed_repos = [
            GitHubRepository("", "main"),  # Empty repo name
            GitHubRepository("no-slash", "main"),  # Missing slash
            GitHubRepository("too/many/slashes", "main"),  # Too many slashes
            GitHubRepository("owner/", "main"),  # Missing repo part
            GitHubRepository("/repo", "main"),  # Missing owner part
        ]

        for malformed_repo in malformed_repos:
            with self.subTest(repo=str(malformed_repo)):
                # Test repository validation
                problems = ValidationTree(malformed_repo)
                malformed_repo.lint(problems)

                # Most of these should have validation errors
                # (Some might be caught by the validation logic)

                # Test using malformed repo as changeSource
                eco_proposed = copy.deepcopy(self.eco_main)
                eco_proposed.name = "Changed"

                auth_problems = ValidationTree(self.eco_main)
                self.eco_main.checkIfChangesAreAuthorized(eco_proposed, malformed_repo, auth_problems)

                # Should fail authorization
                self.assertTrue(auth_problems.hasErrors(),
                                f"Malformed repo {malformed_repo} should fail authorization")

    def test_repository_with_special_characters(self) -> None:
        """Test repositories with special characters that might bypass validation."""
        special_repos = [
            GitHubRepository("owner/../attacker", "main"),
            GitHubRepository("owner/repo;malicious", "main"),
            GitHubRepository("owner/repo\x00null", "main"),
        ]

        for special_repo in special_repos:
            with self.subTest(repo=str(special_repo)):
                # These should be caught by validation
                problems = ValidationTree(special_repo)
                special_repo.lint(problems)

                # Most should have validation errors
                if not problems.hasErrors():
                    # If validation passes, test authorization
                    eco_proposed = copy.deepcopy(self.eco_main)
                    eco_proposed.name = "Changed"

                    auth_problems = ValidationTree(self.eco_main)
                    self.eco_main.checkIfChangesAreAuthorized(eco_proposed, special_repo, auth_problems)

                    self.assertTrue(auth_problems.hasErrors(),
                                    "Special character repo should fail authorization")


class TestRepositorySpoofingAttacks(unittest.TestCase):
    """Test attempts to spoof legitimate repositories."""

    def setUp(self) -> None:
        """Set up test ecosystem."""
        self.eco_main = tests.nwdb.eco.createEcosystem()
        self.legitimate_repo = self.eco_main.owningRepo

    def test_similar_repository_names(self) -> None:
        """Test repositories with names similar to legitimate ones."""
        # Get legitimate repo details
        if isinstance(self.legitimate_repo, GitHubRepository):
            legit_name = self.legitimate_repo.repositoryName
            legit_branch = self.legitimate_repo.branchName

            spoofed_repos = [
                GitHubRepository(legit_name + "x", legit_branch),  # Extra character
                GitHubRepository(legit_name.replace("o", "0"), legit_branch),  # Character substitution
                GitHubRepository(legit_name.upper(), legit_branch),  # Case change
                GitHubRepository(legit_name, legit_branch + "_malicious"),  # Different branch
            ]

            for spoofed_repo in spoofed_repos:
                with self.subTest(repo=str(spoofed_repo)):
                    eco_proposed = copy.deepcopy(self.eco_main)
                    eco_proposed.name = "Spoofed Change"

                    problems = ValidationTree(self.eco_main)
                    self.eco_main.checkIfChangesAreAuthorized(eco_proposed, spoofed_repo, problems)

                    self.assertTrue(problems.hasErrors(),
                                    f"Spoofed repo {spoofed_repo} should not be authorized")

    def test_case_sensitivity_bypass_attempt(self) -> None:
        """Test if case differences can bypass authorization."""
        if isinstance(self.legitimate_repo, GitHubRepository):
            # Create repos with different casing
            upper_repo = GitHubRepository(
                self.legitimate_repo.repositoryName.upper(),
                self.legitimate_repo.branchName.upper()
            )

            eco_proposed = copy.deepcopy(self.eco_main)
            eco_proposed.name = "Case Bypass Attempt"

            problems = ValidationTree(self.eco_main)
            self.eco_main.checkIfChangesAreAuthorized(eco_proposed, upper_repo, problems)

            self.assertTrue(problems.hasErrors(),
                            "Case-different repository should not be authorized")


class TestTypeConfusionAttacks(unittest.TestCase):
    """Test attacks using type confusion."""

    def setUp(self) -> None:
        """Set up test ecosystem."""
        self.eco_main = tests.nwdb.eco.createEcosystem()
        self.attacker_repo = GitHubRepository("attacker/malicious", "main")

    def test_zone_type_replacement(self) -> None:
        """Test replacing a GovernanceZone with a different object type."""
        eco_proposed = copy.deepcopy(self.eco_main)

        # Try to replace a zone with a different type of object
        # Note: This might not be possible due to type checking, but worth testing
        fake_zone = FakeRepository("USA")  # Different type, same name

        # This test verifies that type safety prevents unauthorized replacements
        try:
            # This should fail at the Python type level or in validation
            eco_proposed.zones.authorizedObjects["USA"] = fake_zone  # type: ignore

            problems = ValidationTree(self.eco_main)
            self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.attacker_repo, problems)

            # If it doesn't fail due to types, it should fail authorization
            self.assertTrue(problems.hasErrors(),
                            "Type confusion should be caught by authorization")
        except (TypeError, AttributeError):
            # Type system correctly prevents this - good!
            pass


class TestHierarchicalAuthorizationBypass(unittest.TestCase):
    """Test attempts to bypass authorization through hierarchy manipulation."""

    def setUp(self) -> None:
        """Set up test ecosystem."""
        self.eco_main = tests.nwdb.eco.createEcosystem()
        self.attacker_repo = GitHubRepository("attacker/malicious", "main")

    def test_deep_object_modification_bypass(self) -> None:
        """Test modifying deep objects without proper authorization."""
        eco_proposed = copy.deepcopy(self.eco_main)

        # Try to modify a team that should be protected
        usa_zone = eco_proposed.getZoneOrThrow("USA")

        # Add a legitimate team first
        usa_zone.add(TeamDeclaration("LegitTeam", GitHubRepository("legit/repo", "main")))

        # Update main ecosystem
        self.eco_main = copy.deepcopy(eco_proposed)
        eco_proposed = copy.deepcopy(self.eco_main)

        # Now try to modify the team from an unauthorized repo
        usa_zone_proposed = eco_proposed.getZoneOrThrow("USA")
        legit_team = usa_zone_proposed.getTeamOrThrow("LegitTeam")

        # Try to add data stores to the team (should require team's repo)
        legit_team.documentation = PlainTextDocumentation("Unauthorized modification")

        problems = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.attacker_repo, problems)

        self.assertTrue(problems.hasErrors(),
                        "Deep object modification should require proper authorization")

    def test_authorization_chain_verification(self) -> None:
        """Test that authorization is verified at each level of the hierarchy."""
        eco_proposed = copy.deepcopy(self.eco_main)

        # Get USA zone (owned by USA repo)
        usa_zone = eco_proposed.getZoneOrThrow("USA")
        usa_repo = usa_zone.owningRepo

        # Add team declaration (should be allowed by USA zone repo)
        new_team_decl = TeamDeclaration("NewTeam", GitHubRepository("team/repo", "main"))
        usa_zone.add(new_team_decl)

        # Test that ecosystem repo cannot add team declaration
        problems = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.eco_main.owningRepo, problems)

        self.assertTrue(problems.hasErrors(),
                        "Ecosystem repo should not be able to add teams to zones")

        # Test that zone repo CAN add team declaration
        problems2 = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, usa_repo, problems2)

        self.assertFalse(problems2.hasErrors(),
                         "Zone repo should be able to add team declarations")


class TestAuthorizationEdgeCases(unittest.TestCase):
    """Test edge cases in authorization logic."""

    def setUp(self) -> None:
        """Set up test ecosystem."""
        self.eco_main = tests.nwdb.eco.createEcosystem()

    def test_self_authorization_edge_case(self) -> None:
        """Test authorization when object authorizes changes to itself."""
        eco_proposed = copy.deepcopy(self.eco_main)

        # Change something that the ecosystem repo should be able to change
        eco_proposed.name = "New Name"

        # Should be authorized by its own repo
        problems = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.eco_main.owningRepo, problems)

        self.assertFalse(problems.hasErrors(),
                         "Object should be able to authorize changes from its own repo")

    def test_circular_repository_references(self) -> None:
        """Test handling of circular repository references."""
        # Create a scenario where repos might reference each other
        repo_a = GitHubRepository("owner/repo-a", "main")
        repo_b = GitHubRepository("owner/repo-b", "main")

        # Create objects that reference each other's repos
        zone_a = GovernanceZone("ZoneA", repo_a)
        zone_b = GovernanceZone("ZoneB", repo_b)

        # Test authorization doesn't get into infinite loops
        problems = ValidationTree(zone_a)
        try:
            zone_a.checkIfChangesAreAuthorized(zone_b, repo_a, problems)
            # Should complete without infinite loop
            self.assertTrue(True, "Authorization check completed")
        except RecursionError:
            self.fail("Authorization check resulted in infinite recursion")

    def test_empty_change_authorization(self) -> None:
        """Test authorization when no changes are made."""
        eco_proposed = copy.deepcopy(self.eco_main)

        # No changes made
        problems = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.eco_main.owningRepo, problems)

        self.assertFalse(problems.hasErrors(),
                         "No changes should not cause authorization errors")

    def test_multiple_simultaneous_changes(self) -> None:
        """Test authorization of multiple changes from different repos."""
        eco_proposed = copy.deepcopy(self.eco_main)

        # Make changes that would require different repos to authorize
        eco_proposed.name = "Changed Ecosystem"  # Requires ecosystem repo

        usa_zone = eco_proposed.getZoneOrThrow("USA")
        usa_zone.add(TeamDeclaration("NewTeam", GitHubRepository("team/repo", "main")))  # Requires USA zone repo

        # Test with ecosystem repo (should fail because of zone change)
        problems = ValidationTree(self.eco_main)
        self.eco_main.checkIfChangesAreAuthorized(eco_proposed, self.eco_main.owningRepo, problems)

        self.assertTrue(problems.hasErrors(),
                        "Multiple changes requiring different repos should fail from single repo")


class TestRepositoryValidationBypass(unittest.TestCase):
    """Test attempts to bypass repository validation."""

    def test_repository_with_unicode_attack(self) -> None:
        """Test repositories with unicode characters that might bypass validation."""
        unicode_repos = [
            GitHubRepository("owner/repo\u202e", "main"),  # Right-to-left override
            GitHubRepository("owner/repo\u2060", "main"),  # Word joiner
            GitHubRepository("owner/rep\u00ado", "main"),  # Soft hyphen
        ]

        for unicode_repo in unicode_repos:
            with self.subTest(repo=str(unicode_repo)):
                # Test repository validation
                problems = ValidationTree(unicode_repo)
                unicode_repo.lint(problems)

                # Most unicode attacks should be caught by validation
                # If not caught, test authorization
                eco_main = tests.nwdb.eco.createEcosystem()
                eco_proposed = copy.deepcopy(eco_main)
                eco_proposed.name = "Unicode Attack"

                auth_problems = ValidationTree(eco_main)
                eco_main.checkIfChangesAreAuthorized(eco_proposed, unicode_repo, auth_problems)

                self.assertTrue(auth_problems.hasErrors(),
                                "Unicode repository should not be authorized")

    def test_repository_equality_edge_cases(self) -> None:
        """Test edge cases in repository equality checking."""
        repo1 = GitHubRepository("owner/repo", "main")
        repo2 = GitHubRepository("owner/repo", "main")
        repo3 = GitHubRepository("owner/repo", "MAIN")  # Different case

        # Test that identical repos are equal
        self.assertEqual(repo1, repo2, "Identical repos should be equal")

        # Test that case-different repos are not equal
        self.assertNotEqual(repo1, repo3, "Case-different repos should not be equal")

        # Test authorization behavior with actual ecosystem repo
        eco_main = tests.nwdb.eco.createEcosystem()
        eco_proposed = copy.deepcopy(eco_main)
        eco_proposed.name = "MODIFIED_NAME"  # Make an actual change

        # Get actual repo and create case-different version
        if isinstance(eco_main.owningRepo, GitHubRepository):
            actual_repo = eco_main.owningRepo
            case_different_repo = GitHubRepository(
                actual_repo.repositoryName.upper(),
                actual_repo.branchName.upper()
            )

            # Verify they're not equal
            self.assertNotEqual(actual_repo, case_different_repo,
                                "Case-different repos should not be equal")

            # Test authorization with case-different repo (should fail)
            problems = ValidationTree(eco_main)
            eco_main.checkIfChangesAreAuthorized(eco_proposed, case_different_repo, problems)

            # Authorization IS case-sensitive and properly rejects case-different repos
            self.assertTrue(problems.hasErrors(),
                            "CONFIRMED: Case-different repos are properly rejected by authorization")


if __name__ == '__main__':
    unittest.main()
