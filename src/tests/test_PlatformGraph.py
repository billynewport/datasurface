import unittest
from datasurface.md.Governance import DataPlatform, DataPlatformGraph, Ecosystem, PipelineStep, PlatformInformation

from tests.nwdb.eco import createEcosystem


class Test_PlatformGraphs(unittest.TestCase):

    def test_CreateGraph(self):
        eco : Ecosystem = createEcosystem()

        azurePlatform : DataPlatform = eco.getDataPlatformOrThrow("Azure Platform")
        self.assertEqual(eco.getDefaultDataPlatform(), azurePlatform)

        graph : DataPlatformGraph = DataPlatformGraph(eco)

        self.assertIsNotNone(graph.roots.get(azurePlatform))

        pi : PlatformInformation = graph.roots[azurePlatform]
        self.assertEqual(len(pi.workspaces), 3)

        self.assertEqual(len(pi.assetExports), 1)

        ingestionRoots : set[PipelineStep] = pi.getIngestionRoots()
        for ir in ingestionRoots:
            self.assertTrue(str(ir).startswith("Ingest"))
            




        