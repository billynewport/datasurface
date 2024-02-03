import unittest
from datasurface.md.Governance import DataPlatform, DataPlatformGraph, Ecosystem

from tests.nwdb.eco import createEcosystem


class Test_PlatformGraphs(unittest.TestCase):

    def test_CreateGraph(self):
        eco : Ecosystem = createEcosystem()

        self.assertEqual(eco.getDefaultDataPlatform(), eco.getDataPlatformOrThrow("Azure Platform"))

        graph : DataPlatformGraph = DataPlatformGraph(eco)

        platformSet : set[DataPlatform] = set(graph.roots.keys())

        