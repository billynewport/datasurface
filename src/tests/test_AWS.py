import unittest

from datasurface.md.AmazonAWS import AWSSecret
from datasurface.md.Governance import Ecosystem
from datasurface.md.Lint import NameHasBadSynthax, ValidationTree
from tests.nwdb.eco import createEcosystem


class TestAWS(unittest.TestCase):
    def test_AWSSecret(self):
        e: Ecosystem = createEcosystem()
        s: AWSSecret = AWSSecret("name", e.getLocationOrThrow("AWS", ["USA", "us-east-1"]))

        t: ValidationTree = ValidationTree(s)
        s.lint(e, t)
        self.assertFalse(t.hasErrors())

        s.secretName = "Secrets cannot have spaces"
        s.lint(e, t)
        self.assertTrue(t.containsProblemType(NameHasBadSynthax))
