import unittest
from datasurface.md.GitOps import GitHubRepository

from datasurface.md.Lint import ProblemSeverity, ValidationTree


class TestLint(unittest.TestCase):
    def test_RepositoryLint(self):
        r: GitHubRepository = GitHubRepository("billynewport/repo", "FOmain")
        tree: ValidationTree = ValidationTree(r)
        r.lint(tree)
        self.assertFalse(tree.hasErrors())

        r: GitHubRepository = GitHubRepository("billynewport/repo", "FOmain")
        tree: ValidationTree = ValidationTree(r)
        r.lint(tree)
        self.assertFalse(tree.hasErrors())

        # If tree has any problems marked as ERROR then hasErrors return true
        tree = ValidationTree("")
        tree.addProblem("This is a problem")
        self.assertTrue(tree.hasErrors())
        self.assertFalse(tree.hasIssues())

        tree = ValidationTree("")
        tree.addProblem("This is a problem", sev=ProblemSeverity.WARNING)
        self.assertFalse(tree.hasErrors())
        self.assertTrue(tree.hasIssues())

        # Tree with a non error has issues, tree with an error has errors also
        tree.addProblem("This is a problem", sev=ProblemSeverity.INFO)
        self.assertFalse(tree.hasErrors())
        self.assertTrue(tree.hasIssues())
        tree.addProblem("This is a problem", sev=ProblemSeverity.ERROR)
        self.assertTrue(tree.hasErrors())
