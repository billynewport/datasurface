import unittest
from datasurface.md.Governance import DataPlatform, DataPlatformGraph, DataTransformerStep, Ecosystem, ExportStep, IngestionStep, PipelineStep, PlatformPipelineGraph, TriggerStep

from tests.nwdb.eco import createEcosystem


class Test_PlatformGraphs(unittest.TestCase):

    def test_PipelineGraph(self):
        eco : Ecosystem = createEcosystem()

        azurePlatform : DataPlatform = eco.getDataPlatformOrThrow("Azure Platform")
        self.assertEqual(eco.getDefaultDataPlatform(), azurePlatform)

        graph : DataPlatformGraph = DataPlatformGraph(eco)

        self.assertIsNotNone(graph.roots.get(azurePlatform))

        pi : PlatformPipelineGraph = graph.roots[azurePlatform]
        self.assertEqual(len(pi.workspaces), 3)

        self.assertEqual(len(pi.assetExports), 1)

        # Left hand side of pipeline graph should just be ingestions
        ingestionRoots : set[PipelineStep] = pi.getLeftSideOfGraph()
        for ir in ingestionRoots:
            self.assertTrue(str(ir).startswith("Ingest"))

        # Right hand side of pipeline graph should be exports to assets
        rightHandLeafs : set[PipelineStep] = pi.getRightSideOfGraph()
        for rh in rightHandLeafs:
            self.assertTrue(str(rh).startswith("Export"))

        # Check every ingest is followed by an export
        self.assertTrue(pi.checkNextStepsForStepType(IngestionStep, ExportStep))
        # Check exports are only followed by a trigger
        self.assertTrue(pi.checkNextStepsForStepType(ExportStep, TriggerStep))
        # Check triggers are only followed by a datatransformer
        self.assertTrue(pi.checkNextStepsForStepType(TriggerStep, DataTransformerStep))
        # Check DataTransformers are followed by ingestion
        self.assertTrue(pi.checkNextStepsForStepType(DataTransformerStep, IngestionStep))
            




        