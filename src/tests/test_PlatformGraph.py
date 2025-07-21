"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest
from datasurface.md import DataPlatform, DataTransformerNode, Ecosystem, EcosystemPipelineGraph, \
    ExportNode, IngestionNode, PipelineNode, PlatformPipelineGraph, TriggerNode, Workspace, PrioritizedWorkloadTier

from tests.nwdb.eco import createEcosystem
from typing import cast
from datasurface.md import WorkloadTier, WorkspacePriority


class Test_PlatformGraphs(unittest.TestCase):

    def test_PipelineGraph(self):
        eco: Ecosystem = createEcosystem()

        legacyA: DataPlatform = eco.getDataPlatformOrThrow("LegacyA")
        self.assertEqual(eco.getDefaultDataPlatform(), legacyA)

        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)

        self.assertIsNotNone(graph.roots.get(legacyA.name))

        pi: PlatformPipelineGraph = graph.roots[legacyA.name]
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

    def test_storesToIngestMatchesIngestionNodesInGraph(self):
        eco: Ecosystem = createEcosystem()
        legacyA: DataPlatform = eco.getDataPlatformOrThrow("LegacyA")
        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)
        pi: PlatformPipelineGraph = graph.roots[legacyA.name]

        # Ingestion nodes are all the nodes on the left hand side of the graph but can also occur within the graph.
        ingestionNodes: set[IngestionNode] = set()
        for node in pi.nodes.values():
            if isinstance(node, IngestionNode):
                ingestionNodes.add(node)

        # Just make a set of the store names
        ingestionStoreNames: set[str] = set()
        for node in ingestionNodes:
            ingestionStoreNames.add(node.storeName)

        self.assertEqual(len(pi.storesToIngest), len(ingestionStoreNames))

    def test_WorkspacePriority(self):
        p1: WorkspacePriority = PrioritizedWorkloadTier(WorkloadTier.CRITICAL)
        p2: WorkspacePriority = PrioritizedWorkloadTier(WorkloadTier.HIGH)
        self.assertTrue(p1.isMoreImportantThan(p2))
        self.assertFalse(p2.isMoreImportantThan(p1))

        p1 = PrioritizedWorkloadTier(WorkloadTier.MEDIUM)
        p2 = PrioritizedWorkloadTier(WorkloadTier.MEDIUM)
        self.assertFalse(p1.isMoreImportantThan(p2))

    def test_PipelineGraph_Priority(self):
        eco: Ecosystem = createEcosystem()

        legacyA: DataPlatform = eco.getDataPlatformOrThrow("LegacyA")
        self.assertEqual(eco.getDefaultDataPlatform(), legacyA)

        # There are already 3 workspaces in the ecosystem
        workspaceA: Workspace = eco.cache_getWorkspaceOrThrow("ProductLiveAdhocReporting").workspace
        workspaceB: Workspace = eco.cache_getWorkspaceOrThrow("MaskCustomersWorkSpace").workspace
        workspaceC: Workspace = eco.cache_getWorkspaceOrThrow("WorkspaceUsingTransformerOutput").workspace

        # A and B are both using the prime datastore, NW_Data.
        # C is using data produced by the transformer in B, Masked_NW_Data

        # These all use the default priority initially so verify its UNKNOWN
        p: PrioritizedWorkloadTier = cast(PrioritizedWorkloadTier, workspaceA.priority)
        self.assertEqual(p.priority, WorkloadTier.UNKNOWN)
        p: PrioritizedWorkloadTier = cast(PrioritizedWorkloadTier, workspaceB.priority)
        self.assertEqual(p.priority, WorkloadTier.UNKNOWN)
        p: PrioritizedWorkloadTier = cast(PrioritizedWorkloadTier, workspaceC.priority)
        self.assertEqual(p.priority, WorkloadTier.UNKNOWN)

        # Now set the priority of workspaceA and B to MEDIUM, set C to LOW
        workspaceA.add(PrioritizedWorkloadTier(WorkloadTier.MEDIUM))
        workspaceB.add(PrioritizedWorkloadTier(WorkloadTier.MEDIUM))
        workspaceC.add(PrioritizedWorkloadTier(WorkloadTier.LOW))

        # Recalculate the graph priorities
        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)
        pi: PlatformPipelineGraph = graph.roots[legacyA.name]
        pi.propagateWorkspacePriorities()

        # Check every node has a priority
        for node in pi.nodes.values():
            if node.priority is None:   
                self.fail(f"Node {node} has no priority")

        # Workspace A's left hand nodes (exports)should all be MEDIUM
        work_a_export_nodes: set[ExportNode] = pi.findAllExportNodesForWorkspace(workspaceA)
        for en in work_a_export_nodes:
            self.assertEqual(cast(PrioritizedWorkloadTier, en.priority).priority, WorkloadTier.MEDIUM)

        # Workspace B's left hand nodes (exports) should all be MEDIUM
        work_b_export_nodes: set[ExportNode] = pi.findAllExportNodesForWorkspace(workspaceB)
        for en in work_b_export_nodes:
            self.assertEqual(cast(PrioritizedWorkloadTier, en.priority).priority, WorkloadTier.MEDIUM)

        # Workspace C's left hand nodes should all be LOW
        work_c_export_nodes: set[ExportNode] = pi.findAllExportNodesForWorkspace(workspaceC)
        for en in work_c_export_nodes:
            self.assertEqual(cast(PrioritizedWorkloadTier, en.priority).priority, WorkloadTier.LOW)

        # Now set the priority of workspaceA and B to MEDIUM, set C to LOW
        workspaceA.add(PrioritizedWorkloadTier(WorkloadTier.MEDIUM))
        workspaceB.add(PrioritizedWorkloadTier(WorkloadTier.MEDIUM))
        workspaceC.add(PrioritizedWorkloadTier(WorkloadTier.CRITICAL))

        # Recalculate the graph priorities
        graph = EcosystemPipelineGraph(eco)
        pi = graph.roots[legacyA.name]
        pi.propagateWorkspacePriorities()

        # Workspace A's left hand nodes should all be MEDIUM
        work_a_export_nodes: set[ExportNode] = pi.findAllExportNodesForWorkspace(workspaceA)
        for en in work_a_export_nodes:
            # export customers is critical because the mask transformer uses it
            if en.datasetName == "customers":
                self.assertEqual(cast(PrioritizedWorkloadTier, en.priority).priority, WorkloadTier.CRITICAL)
            else:
                self.assertEqual(cast(PrioritizedWorkloadTier, en.priority).priority, WorkloadTier.MEDIUM)

        work_b_export_nodes: set[ExportNode] = pi.findAllExportNodesForWorkspace(workspaceB)
        for en in work_b_export_nodes:
            self.assertEqual(cast(PrioritizedWorkloadTier, en.priority).priority, WorkloadTier.CRITICAL)

        def checkLeftwardNodePriorityIs(node: PipelineNode, expectedPriority: WorkloadTier):
            for nn in node.leftHandNodes.values():
                if nn.priority is None:
                    self.fail(f"Node {nn} has no priority")
                else:
                    self.assertTrue(cast(PrioritizedWorkloadTier, nn.priority).priority == expectedPriority)
                    checkLeftwardNodePriorityIs(nn, expectedPriority)

        # Masked_NW_Data.customers uses NW_Data.customers
        # NW_Data is used by A, B and indirectly by C.
        # NW_Data is multi-dataset ingestion so all datasets use a single node/priority.
        work_c_export_nodes: set[ExportNode] = pi.findAllExportNodesForWorkspace(workspaceC)
        for en in work_c_export_nodes:
            self.assertEqual(cast(PrioritizedWorkloadTier, en.priority).priority, WorkloadTier.CRITICAL)
            # Walk leftwards checking all nodes are CRITICAL
            checkLeftwardNodePriorityIs(en, WorkloadTier.CRITICAL)

        # Check ingestion of NW_Data is CRITICAL
