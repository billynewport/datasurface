import unittest
from datasurface.md.Governance import DataPlatform, EcosystemPipelineGraph, DataTransformerNode, Ecosystem, ExportNode, IngestionNode, PipelineNode, PlatformPipelineGraph, TriggerNode
from datasurface.md.utils import validate_cron_string

from tests.nwdb.eco import createEcosystem


class Test_PlatformGraphs(unittest.TestCase):

    def test_PipelineGraph(self):
        eco : Ecosystem = createEcosystem()

        azurePlatform : DataPlatform = eco.getDataPlatformOrThrow("Azure Platform")
        self.assertEqual(eco.getDefaultDataPlatform(), azurePlatform)

        graph : EcosystemPipelineGraph = EcosystemPipelineGraph(eco)

        self.assertIsNotNone(graph.roots.get(azurePlatform))

        pi : PlatformPipelineGraph = graph.roots[azurePlatform]
        self.assertEqual(len(pi.workspaces), 3)

        self.assertEqual(len(pi.assetExports), 1)

        # Left hand side of pipeline graph should just be ingestions
        ingestionRoots : set[PipelineNode] = pi.getLeftSideOfGraph()
        for ir in ingestionRoots:
            self.assertTrue(str(ir).startswith("Ingest"))

        # Right hand side of pipeline graph should be exports to assets
        rightHandLeafs : set[PipelineNode] = pi.getRightSideOfGraph()
        for rh in rightHandLeafs:
            self.assertTrue(str(rh).startswith("Export"))

        # Check every ingest is followed by an export
        self.assertTrue(pi.checkNextStepsForStepType(IngestionNode, ExportNode))
        # Check exports are only followed by a trigger
        self.assertTrue(pi.checkNextStepsForStepType(ExportNode, TriggerNode))
        # Check triggers are only followed by a datatransformer
        self.assertTrue(pi.checkNextStepsForStepType(TriggerNode, DataTransformerNode))
        # Check DataTransformers are followed by ingestion
        self.assertTrue(pi.checkNextStepsForStepType(DataTransformerNode, IngestionNode))
            
class TestCronStringValidator(unittest.TestCase):
    def test_valid_cron_strings(self):
        self.assertTrue(validate_cron_string('* * * * *'))
        self.assertTrue(validate_cron_string('0 0 1 1 *'))
        self.assertTrue(validate_cron_string('1,15,30 * * * *'))
        self.assertTrue(validate_cron_string('0-59 0-23 1-31 1-12 0-7'))

    def test_invalid_cron_strings(self):
        self.assertFalse(validate_cron_string('invalid cron string'))
        self.assertFalse(validate_cron_string('* * * *'))  # Not enough fields
        self.assertFalse(validate_cron_string('* * * * * *'))  # Too many fields
        self.assertFalse(validate_cron_string('60 * * * *'))  # Minute out of range
        self.assertFalse(validate_cron_string('* 24 * * *'))  # Hour out of range
        self.assertFalse(validate_cron_string('* * 32 * *'))  # Day of month out of range
        self.assertFalse(validate_cron_string('* * * 13 *'))  # Month out of range
        self.assertFalse(validate_cron_string('* * * * 8'))  # Day of week out of range
        self.assertFalse(validate_cron_string('1-2-3 * * * *'))  # Invalid range
        self.assertFalse(validate_cron_string('1,2,3, * * * *'))  # Invalid list
        self.assertFalse(validate_cron_string('*/5 * * * *')) # Every 5 notation not supported




        