import unittest
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.GitOps import GitHubRepository
from datasurface.md.Lint import ValidationTree

from tests.nwdb.eco import createEcosystem


class TestGitOps(unittest.TestCase):
    def setUp(self):
        self.git_ops = createEcosystem()  # Just to get the methods to test

    def test_showDictChangesAsProblems_deletedKeys(self):
        current = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        proposed = {
            "key1": "value1",
            "key3": "value3"
        }
        vTree: ValidationTree = ValidationTree("test")
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "value2 has been deleted")

    def test_showDictChangesAsProblems_addedKeys(self):
        current = {
            "key1": "value1",
            "key3": "value3"
        }
        proposed = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        vTree = ValidationTree("test")
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "value2 has been added")

    def test_showDictChangesAsProblems_modifiedKeys(self):
        current = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        proposed = {
            "key1": "value1",
            "key2": "updated_value",
            "key3": "value3"
        }
        vTree = ValidationTree("test")
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "updated_value has been modified")

    def test_showDictChangesAsProblems_noChanges(self):
        current = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        proposed = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
        vTree = ValidationTree("test")
        self.git_ops.showDictChangesAsProblems(current, proposed, vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 0)

    def test_showSetChangesAsProblems_deletedKeys(self):
        current = {1, 2, 3}
        proposed = {1, 3}
        vTree = ValidationTree("test")
        self.git_ops.showSetChangesAsProblems(current, proposed, vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "2 has been deleted")

    def test_showSetChangesAsProblems_addedKeys(self):
        current = {1, 3}
        proposed = {1, 2, 3}
        vTree = ValidationTree("test")
        self.git_ops.showSetChangesAsProblems(current, proposed, vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0].description, "2 has been added")

    def test_showSetChangesAsProblems_noChanges(self):
        current = {1, 2, 3}
        proposed = {1, 2, 3}
        vTree = ValidationTree("test")
        self.git_ops.showSetChangesAsProblems(current, proposed, vTree)
        problems = vTree.getProblems()
        self.assertEqual(len(problems), 0)


class TestRepository(unittest.TestCase):
    def test_GitHubRepository(self):
        g: GitHubRepository = GitHubRepository("billynewport/repo", "main")
        gdoc: GitHubRepository = GitHubRepository("billynewport/repo", "main", PlainTextDocumentation("Test"))
        gdoc2: GitHubRepository = GitHubRepository("billynewport/repo", "main", PlainTextDocumentation("Test2"))
        g2: GitHubRepository = GitHubRepository("billynewport/repo", "main2")

        self.assertEqual(g, g)
        self.assertNotEqual(g, gdoc)
        self.assertNotEqual(g, g2)
        self.assertEqual(gdoc, gdoc)

        self.assertEqual(g.repositoryName, "billynewport/repo")
        self.assertEqual(g.branchName, "main")

        self.assertEqual(gdoc.documentation, PlainTextDocumentation("Test"))
        self.assertEqual(gdoc2.documentation, PlainTextDocumentation("Test2"))
        self.assertNotEqual(gdoc.documentation, gdoc2.documentation)

        g = GitHubRepository("billynewport/testsurface", "eco_edits")
        g2 = GitHubRepository("billynewport/test-surface", "eco_edits")

        self.assertNotEqual(g, g2)
        self.assertNotEqual(g2, g)


if __name__ == '__main__':
    unittest.main()
