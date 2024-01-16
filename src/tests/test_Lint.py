import unittest

from datasurface.md.Governance import GitRepository
from datasurface.md.Lint import ValidationTree

class TestLint(unittest.TestCase):
    def test_RepositoryLint(self):
        r : GitRepository = GitRepository("https://github.com/billynewport/fo.git", "main")
        tree : ValidationTree = ValidationTree(r)
        r.lint(tree)
        self.assertFalse(tree.hasErrors())

        r : GitRepository = GitRepository("github.com/billynewport/fo.git", "main")
        tree : ValidationTree = ValidationTree(r)
        r.lint(tree)
        self.assertTrue(tree.hasErrors())        