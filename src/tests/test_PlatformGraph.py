"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest
from datasurface.md import DataPlatform, DataTransformerNode, Ecosystem, EcosystemPipelineGraph, \
    ExportNode, IngestionNode, PipelineNode, PlatformPipelineGraph, TriggerNode

from tests.nwdb.eco import createEcosystem


class Test_PlatformGraphs(unittest.TestCase):

    def test_PipelineGraph(self):
        eco: Ecosystem = createEcosystem()

        legacyA: DataPlatform = eco.getDataPlatformOrThrow("LegacyA")
        self.assertEqual(eco.getDefaultDataPlatform(), legacyA)

        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)

        self.assertIsNotNone(graph.roots.get(legacyA))

        pi: PlatformPipelineGraph = graph.roots[legacyA]
        self.assertEqual(len(pi.workspaces), 3)

        self.assertEqual(len(pi.dataContainerExports), 1)

        # Left hand side of pipeline graph should just be ingestions
        ingestionRoots: set[PipelineNode] = pi.getLeftSideOfGraph()
        for ir in ingestionRoots:
            self.assertTrue(str(ir).startswith("Ingest"))

        # Right hand side of pipeline graph should be exports to assets
        rightHandLeafs: set[PipelineNode] = pi.getRightSideOfGraph()
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

        graphStr: str = pi.graphToText()

        print(graphStr)
