import unittest
from datasurface.md.GitOps import GitHubRepository

from datasurface.md.Lint import ProblemSeverity, ValidationProblem, ValidationTree


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
        tree.printTree()
        self.assertEqual(tree.numErrors, 1)
        self.assertEqual(tree.numWarnings, 0)

        tree = ValidationTree("")
        tree.addProblem("This is a problem", sev=ProblemSeverity.WARNING)
        self.assertFalse(tree.hasErrors())
        self.assertTrue(tree.hasIssues())
        tree.printTree()
        self.assertEqual(tree.numErrors, 0)
        self.assertEqual(tree.numWarnings, 1)

        # Tree with a non error has issues, tree with an error has errors also
        tree.addProblem("This is a problem", sev=ProblemSeverity.INFO)
        self.assertFalse(tree.hasErrors())
        self.assertTrue(tree.hasIssues())
        tree.addProblem("This is a problem", sev=ProblemSeverity.ERROR)
        self.assertTrue(tree.hasErrors())

    def test_ValidationProblem(self):
        v: ValidationProblem = ValidationProblem("This is a problem", ProblemSeverity.ERROR)
        self.assertEqual(str(v), "ERROR:This is a problem")

        v = ValidationProblem("This is a problem", ProblemSeverity.WARNING)
        self.assertEqual(str(v), "WARNING:This is a problem")
