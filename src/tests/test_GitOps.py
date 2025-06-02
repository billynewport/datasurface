"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from typing import Dict, Set, Any, List
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository, GitLabRepository
from datasurface.md import ValidationTree, UserDSLObject, Ecosystem
from datasurface.md.lint import ValidationProblem

from tests.nwdb.eco import createEcosystem


class TestGitOps(unittest.TestCase):
    """Test class for GitOps functionality including dictionary and set change validation."""

    def setUp(self) -> None:
        self.git_ops: Ecosystem = createEcosystem()

    def test_showDictChangesAsProblems_deletedKeys(self) -> None:
        """Test that deleted keys are properly reported as problems."""
        current: Dict[str, str] = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        proposed: Dict[str, str] = {
            "key1": "value1",
            "key3": "value3"
        }
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "value2 has been deleted")

    def test_showDictChangesAsProblems_addedKeys(self) -> None:
        """Test that added keys are properly reported as problems."""
        current: Dict[str, str] = {
            "key1": "value1",
            "key3": "value3"
        }
        proposed: Dict[str, str] = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "value2 has been added")

    def test_showDictChangesAsProblems_modifiedKeys(self) -> None:
        """Test that modified keys are properly reported as problems."""
        current: Dict[str, str] = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        proposed: Dict[str, str] = {
            "key1": "value1",
            "key2": "updated_value",
            "key3": "value3"
        }
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "updated_value has been modified")

    def test_showDictChangesAsProblems_multipleChanges(self) -> None:
        """Test that multiple changes (added, deleted, modified) are all reported."""
        current: Dict[str, str] = {
            "key1": "value1",
            "key2": "value2",
            "key4": "value4"
        }
        proposed: Dict[str, str] = {
            "key1": "value1_modified",
            "key3": "value3",
            "key4": "value4"
        }
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 3)
        problem_descriptions: Set[str] = {p.description for p in problems}
        self.assertIn("value2 has been deleted", problem_descriptions)
        self.assertIn("value3 has been added", problem_descriptions)
        self.assertIn("value1_modified has been modified", problem_descriptions)

    def test_showDictChangesAsProblems_noChanges(self) -> None:
        """Test that identical dictionaries produce no problems."""
        current: Dict[str, str] = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        proposed: Dict[str, str] = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 0)

    def test_showDictChangesAsProblems_emptyDictionaries(self) -> None:
        """Test behavior with empty dictionaries."""
        current: Dict[str, str] = {}
        proposed: Dict[str, str] = {}
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 0)

    def test_showSetChangesAsProblems_deletedKeys(self) -> None:
        """Test that deleted set elements are properly reported as problems."""
        current: Set[int] = {1, 2, 3}
        proposed: Set[int] = {1, 3}
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showSetChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "2 has been deleted")

    def test_showSetChangesAsProblems_addedKeys(self) -> None:
        """Test that added set elements are properly reported as problems."""
        current: Set[int] = {1, 3}
        proposed: Set[int] = {1, 2, 3}
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showSetChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "2 has been added")

    def test_showSetChangesAsProblems_multipleChanges(self) -> None:
        """Test that multiple set changes are all reported."""
        current: Set[int] = {1, 2, 4}
        proposed: Set[int] = {1, 3, 5}
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showSetChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 4)
        problem_descriptions: Set[str] = {p.description for p in problems}
        self.assertIn("2 has been deleted", problem_descriptions)
        self.assertIn("4 has been deleted", problem_descriptions)
        self.assertIn("3 has been added", problem_descriptions)
        self.assertIn("5 has been added", problem_descriptions)

    def test_showSetChangesAsProblems_noChanges(self) -> None:
        """Test that identical sets produce no problems."""
        current: Set[int] = {1, 2, 3}
        proposed: Set[int] = {1, 2, 3}
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showSetChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 0)

    def test_showSetChangesAsProblems_emptySets(self) -> None:
        """Test behavior with empty sets."""
        current: Set[int] = set()
        proposed: Set[int] = set()
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        self.git_ops.showSetChangesAsProblems(current, proposed, vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 0)


class TestGitHubRepository(unittest.TestCase):
    """Test class for GitHubRepository functionality."""

    def test_basic_instantiation_and_equality(self) -> None:
        """Test basic GitHubRepository instantiation and equality."""
        repo1: GitHubRepository = GitHubRepository("billynewport/repo", "main")
        repo2: GitHubRepository = GitHubRepository("billynewport/repo", "main")
        repo3: GitHubRepository = GitHubRepository("billynewport/repo", "develop")
        repo4: GitHubRepository = GitHubRepository("different/repo", "main")

        # Test equality
        self.assertEqual(repo1, repo1)
        self.assertEqual(repo1, repo2)
        self.assertNotEqual(repo1, repo3)
        self.assertNotEqual(repo1, repo4)

        # Test basic attributes
        self.assertEqual(repo1.repositoryName, "billynewport/repo")
        self.assertEqual(repo1.branchName, "main")

    def test_with_documentation(self) -> None:
        """Test GitHubRepository with documentation."""
        doc1: PlainTextDocumentation = PlainTextDocumentation("Test documentation")
        doc2: PlainTextDocumentation = PlainTextDocumentation("Different documentation")

        repo_no_doc: GitHubRepository = GitHubRepository("billynewport/repo", "main")
        repo_with_doc1: GitHubRepository = GitHubRepository("billynewport/repo", "main", doc1)
        repo_with_doc2: GitHubRepository = GitHubRepository("billynewport/repo", "main", doc2)
        repo_with_same_doc: GitHubRepository = GitHubRepository("billynewport/repo", "main", doc1)

        # Test equality with documentation
        self.assertNotEqual(repo_no_doc, repo_with_doc1)
        self.assertNotEqual(repo_with_doc1, repo_with_doc2)
        self.assertEqual(repo_with_doc1, repo_with_same_doc)

        # Test documentation access
        self.assertEqual(repo_with_doc1.documentation, doc1)
        self.assertEqual(repo_with_doc2.documentation, doc2)

    def test_hash_functionality(self) -> None:
        """Test GitHubRepository hashing for use in sets and dictionaries."""
        repo1: GitHubRepository = GitHubRepository("billynewport/repo", "main")
        repo2: GitHubRepository = GitHubRepository("billynewport/repo", "main")
        repo3: GitHubRepository = GitHubRepository("billynewport/repo", "develop")

        # Test that equal objects have equal hashes
        self.assertEqual(hash(repo1), hash(repo2))
        self.assertNotEqual(hash(repo1), hash(repo3))

        # Test use in set
        repo_set: Set[GitHubRepository] = {repo1, repo2, repo3}
        self.assertEqual(len(repo_set), 2)  # repo1 and repo2 are equal

    def test_string_representation(self) -> None:
        """Test GitHubRepository string representation."""
        repo: GitHubRepository = GitHubRepository("billynewport/repo", "main")
        expected_str: str = "GitRepository(billynewport/repo/main)"
        self.assertEqual(str(repo), expected_str)

    def test_json_serialization(self) -> None:
        """Test GitHubRepository JSON serialization."""
        repo: GitHubRepository = GitHubRepository("billynewport/repo", "main")
        json_data: Dict[str, Any] = repo.to_json()

        expected_json: Dict[str, Any] = {
            "_type": "GitHubRepository",
            "repositoryName": "billynewport/repo",
            "branchName": "main"
        }
        self.assertEqual(json_data, expected_json)

    def test_valid_repository_names(self) -> None:
        """Test validation of valid GitHub repository names."""
        valid_names: List[str] = [
            "user/repo",
            "organization/project-name",
            "user123/repo_name",
            "org.name/repo.name",
            "user-name/repo123"
        ]

        repo: GitHubRepository = GitHubRepository("dummy/dummy", "main")
        for name in valid_names:
            with self.subTest(name=name):
                self.assertTrue(repo.is_valid_github_repo_name(name))

    def test_invalid_repository_names(self) -> None:
        """Test validation of invalid GitHub repository names."""
        invalid_names: List[str] = [
            "",
            "repo",  # Missing owner
            "/repo",  # Empty owner
            "user/",  # Empty repo
            "user//repo",  # Double slash
            "-user/repo",  # Starts with dash
            "user/repo-",  # Ends with dash
            "user/../repo",  # Contains ..
            "user/repo with spaces",  # Contains spaces
            "a" * 101,  # Too long
            "user/repo@special"  # Invalid character
        ]

        repo: GitHubRepository = GitHubRepository("dummy/dummy", "main")
        for name in invalid_names:
            with self.subTest(name=name):
                self.assertFalse(repo.is_valid_github_repo_name(name))

    def test_valid_branch_names(self) -> None:
        """Test validation of valid GitHub branch names."""
        valid_branches: List[str] = [
            "main",
            "develop",
            "feature-branch",
            "feature_branch",
            "feature.branch",
            "release-1.0",
            "hotfix123"
        ]

        repo: GitHubRepository = GitHubRepository("dummy/dummy", "main")
        for branch in valid_branches:
            with self.subTest(branch=branch):
                self.assertTrue(repo.is_valid_github_branch(branch))

    def test_invalid_branch_names(self) -> None:
        """Test validation of invalid GitHub branch names."""
        invalid_branches: List[str] = [
            "",
            "branch.",  # Ends with dot
            "branch..name",  # Contains ..
            "branch~name",  # Contains ~
            "branch^name",  # Contains ^
            "branch:name",  # Contains :
            "branch\\name",  # Contains backslash
            "branch*name",  # Contains *
            "branch?name",  # Contains ?
            "branch[name]",  # Contains brackets
            "branch/name",  # Contains slash
            "-branch"  # Starts with dash
        ]

        repo: GitHubRepository = GitHubRepository("dummy/dummy", "main")
        for branch in invalid_branches:
            with self.subTest(branch=branch):
                self.assertFalse(repo.is_valid_github_branch(branch))

    def test_lint_validation(self) -> None:
        """Test GitHubRepository lint validation."""
        # Test valid repository
        valid_repo: GitHubRepository = GitHubRepository("billynewport/valid-repo", "main")
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        valid_repo.lint(vTree)
        self.assertEqual(len(vTree.getProblems()), 0)

        # Test invalid repository name
        invalid_repo_name: GitHubRepository = GitHubRepository("invalid repo name", "main")
        vTree = ValidationTree(UserDSLObject())
        invalid_repo_name.lint(vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertIn("Repository name", problems[0].description)

        # Test invalid branch name
        invalid_branch: GitHubRepository = GitHubRepository("billynewport/repo", "invalid..branch")
        vTree = ValidationTree(UserDSLObject())
        invalid_branch.lint(vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertIn("Branch name", problems[0].description)

        # Test both invalid
        invalid_both: GitHubRepository = GitHubRepository("invalid repo", "invalid..branch")
        vTree = ValidationTree(UserDSLObject())
        invalid_both.lint(vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 2)

    def test_edge_case_repositories(self) -> None:
        """Test edge cases for repository naming."""
        repo1: GitHubRepository = GitHubRepository("billynewport/testsurface", "eco_edits")
        repo2: GitHubRepository = GitHubRepository("billynewport/test-surface", "eco_edits")

        self.assertNotEqual(repo1, repo2)
        self.assertNotEqual(repo2, repo1)
        self.assertNotEqual(hash(repo1), hash(repo2))


class TestGitLabRepository(unittest.TestCase):
    """Test class for GitLabRepository functionality."""

    def test_basic_instantiation_and_equality(self) -> None:
        """Test basic GitLabRepository instantiation and equality."""
        repo1: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "main")
        repo2: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "main")
        repo3: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "develop")
        repo4: GitLabRepository = GitLabRepository("https://other-gitlab.com", "user/repo", "main")

        # Test equality
        self.assertEqual(repo1, repo1)
        self.assertEqual(repo1, repo2)
        self.assertNotEqual(repo1, repo3)
        self.assertNotEqual(repo1, repo4)

        # Test basic attributes
        self.assertEqual(repo1.repoUrl, "https://gitlab.com")
        self.assertEqual(repo1.repositoryName, "user/repo")
        self.assertEqual(repo1.branchName, "main")

    def test_with_documentation(self) -> None:
        """Test GitLabRepository with documentation."""
        doc: PlainTextDocumentation = PlainTextDocumentation("Test documentation")
        repo: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "main", doc)

        self.assertEqual(repo.documentation, doc)

    def test_hash_functionality(self) -> None:
        """Test GitLabRepository hashing."""
        repo1: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "main")
        repo2: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "main")
        repo3: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "develop")

        # Test that equal objects have equal hashes
        self.assertEqual(hash(repo1), hash(repo2))
        self.assertNotEqual(hash(repo1), hash(repo3))

    def test_string_representation(self) -> None:
        """Test GitLabRepository string representation."""
        repo: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "main")
        expected_str: str = "GitLabRepository(https://gitlab.com/user/repo/main)"
        self.assertEqual(str(repo), expected_str)

    def test_json_serialization(self) -> None:
        """Test GitLabRepository JSON serialization."""
        repo: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "main")
        json_data: Dict[str, Any] = repo.to_json()

        expected_json: Dict[str, Any] = {
            "_type": "GitLabRepository",
            "repoUrl": "https://gitlab.com",
            "repositoryName": "user/repo",
            "branchName": "main"
        }
        self.assertEqual(json_data, expected_json)

    def test_valid_urls(self) -> None:
        """Test validation of valid URLs."""
        valid_urls: List[str] = [
            "https://gitlab.com",
            "http://gitlab.example.com",
            "https://git.company.internal",
            "https://gitlab.company.com:8080"
        ]

        repo: GitLabRepository = GitLabRepository("https://dummy.com", "user/repo", "main")
        for url in valid_urls:
            with self.subTest(url=url):
                self.assertTrue(repo.is_valid_url(url))

    def test_invalid_urls(self) -> None:
        """Test validation of invalid URLs."""
        invalid_urls: List[str] = [
            "",
            "not-a-url",
            "gitlab.com",  # Missing scheme
            "http://",  # Missing netloc
            "://gitlab.com"  # Missing scheme
        ]

        repo: GitLabRepository = GitLabRepository("https://dummy.com", "user/repo", "main")
        for url in invalid_urls:
            with self.subTest(url=url):
                self.assertFalse(repo.is_valid_url(url))

    def test_ftp_urls_considered_valid(self) -> None:
        """Test that FTP URLs are considered valid by the current implementation."""
        repo: GitLabRepository = GitLabRepository("https://dummy.com", "user/repo", "main")
        # Note: The current implementation accepts any URL with scheme and netloc
        self.assertTrue(repo.is_valid_url("ftp://gitlab.com"))

    def test_lint_validation(self) -> None:
        """Test GitLabRepository lint validation."""
        # Test valid repository
        valid_repo: GitLabRepository = GitLabRepository("https://gitlab.com", "user/valid-repo", "main")
        vTree: ValidationTree = ValidationTree(UserDSLObject())
        valid_repo.lint(vTree)
        self.assertEqual(len(vTree.getProblems()), 0)

        # Test invalid URL
        invalid_url: GitLabRepository = GitLabRepository("invalid-url", "user/repo", "main")
        vTree = ValidationTree(UserDSLObject())
        invalid_url.lint(vTree)
        problems: List[ValidationProblem] = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertIn("Repository url", problems[0].description)

        # Test invalid repository name
        invalid_repo_name: GitLabRepository = GitLabRepository("https://gitlab.com", "invalid repo", "main")
        vTree = ValidationTree(UserDSLObject())
        invalid_repo_name.lint(vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertIn("Repository name", problems[0].description)

        # Test invalid branch name
        invalid_branch: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "invalid..branch")
        vTree = ValidationTree(UserDSLObject())
        invalid_branch.lint(vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertIn("Branch name", problems[0].description)


class TestRepositoryComparison(unittest.TestCase):
    """Test comparison between different repository types."""

    def test_different_repository_types_not_equal(self) -> None:
        """Test that different repository types are not equal."""
        github_repo: GitHubRepository = GitHubRepository("user/repo", "main")
        gitlab_repo: GitLabRepository = GitLabRepository("https://gitlab.com", "user/repo", "main")

        self.assertNotEqual(github_repo, gitlab_repo)
        self.assertNotEqual(gitlab_repo, github_repo)


if __name__ == '__main__':
    unittest.main()
