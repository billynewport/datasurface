"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest
from datasurface.md import DataPlatform, DataTransformerNode, Ecosystem, EcosystemPipelineGraph, \
    ExportNode, IngestionNode, PipelineNode, PlatformPipelineGraph, TriggerNode, Workspace, PrioritizedWorkloadTier

from tests.nwdb.eco import createEcosystem
from typing import cast
from datasurface.md import WorkloadTier


class Test_PlatformGraphs(unittest.TestCase):

    def test_PipelineGraph(self):
        eco: Ecosystem = createEcosystem()

        legacyA: DataPlatform = eco.getDataPlatformOrThrow("LegacyA")
        self.assertEqual(eco.getDefaultDataPlatform(), legacyA)

        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)

        self.assertIsNotNone(graph.roots.get(legacyA))

        pi: PlatformPipelineGraph = graph.roots[legacyA]
        self.assertEqual(len(pi.workspaces), 3)

        self.assertEqual(len(pi.dataContainerConsumers), 1)

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

    def test_PipelineGraph_Priority(self):
        eco: Ecosystem = createEcosystem()

        legacyA: DataPlatform = eco.getDataPlatformOrThrow("LegacyA")
        self.assertEqual(eco.getDefaultDataPlatform(), legacyA)

        # There are already 3 workspaces in the ecosystem
        workspaceA: Workspace = eco.cache_getWorkspaceOrThrow("ProductLiveAdhocReporting").workspace
        workspaceB: Workspace = eco.cache_getWorkspaceOrThrow("MaskCustomersWorkSpace").workspace
        workspaceC: Workspace = eco.cache_getWorkspaceOrThrow("ProductLiveAdhocReporting").workspace

        # These all use the default priority initially so verify its UNKNOWN
        p: PrioritizedWorkloadTier = cast(PrioritizedWorkloadTier, workspaceA.priority)
        self.assertEqual(p.priority, WorkloadTier.UNKNOWN)
        p: PrioritizedWorkloadTier = cast(PrioritizedWorkloadTier, workspaceB.priority)
        self.assertEqual(p.priority, WorkloadTier.UNKNOWN)
        p: PrioritizedWorkloadTier = cast(PrioritizedWorkloadTier, workspaceC.priority)
        self.assertEqual(p.priority, WorkloadTier.UNKNOWN)

        # Now set the priority of workspaceA to CRITICAL
        workspaceA.add(PrioritizedWorkloadTier(WorkloadTier.CRITICAL))

        # Recalculate the graph priorities
        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)
        pi: PlatformPipelineGraph = graph.roots[legacyA]
        pi.propagateWorkspacePriorities()

        # Verify the priority of the pipeline nodes to the left of the DataContainer
        # used by workspaceA are all critical. Verify that the export nodes for masked_customers
        # are still unknown.

