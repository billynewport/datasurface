import unittest
from datasurface.md.Lint import ValidationTree

from tests.nwdb.eco import createEcosystem

class TestGitOps(unittest.TestCase):
    def setUp(self):
        self.git_ops = createEcosystem() # Just to get the methods to test

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
        vTree : ValidationTree = ValidationTree("test")
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

if __name__ == '__main__':
    unittest.main()