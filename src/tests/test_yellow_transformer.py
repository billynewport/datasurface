"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from tests.test_MergeSnapshotLiveOnly import BaseSnapshotMergeJobTest
from datasurface.md import Ecosystem
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
from datasurface.platforms.yellow.yellow_dp import YellowMilestoneStrategy
from datasurface.md.governance import WorkspacePlatformConfig, DataMilestoningStrategy
from typing import Optional, cast
from datasurface.md.model_schema import addDatasurfaceModel
import unittest
from datasurface.md.lint import ValidationTree
from datasurface.platforms.yellow.transformerjob import DataTransformerJob
from datasurface.platforms.yellow.jobs import JobStatus


class TestYellowTransformers(BaseSnapshotMergeJobTest, unittest.TestCase):
    """Test the SnapshotMergeJob with a simple ecosystem (live-only)"""

    def __init__(self, methodName: str = "runTest") -> None:
        eco: Optional[Ecosystem] = BaseSnapshotMergeJobTest.loadEcosystem("src/tests/yellow_dp_tests")
        if eco is None:
            raise Exception("Ecosystem not loaded")
        addDatasurfaceModel(eco, eco.owningRepo)
        tree: ValidationTree = eco.lintAndHydrateCaches()
        if tree.hasErrors():
            tree.printTree()
            self.assertFalse(tree.hasErrors())
        dp: YellowDataPlatform = cast(YellowDataPlatform, eco.getDataPlatformOrThrow("Test_DP"))
        dp.milestoneStrategy = YellowMilestoneStrategy.SCD1

        # Set the consumer to live-only mode
        req: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, eco.cache_getWorkspaceOrThrow("Consumer1").workspace.dsgs["TestDSG"].platformMD)
        req.retention.milestoningStrategy = DataMilestoningStrategy.LIVE_ONLY
        BaseSnapshotMergeJobTest.__init__(self, eco, dp.name, "Store1")
        unittest.TestCase.__init__(self, methodName)

    def setUp(self) -> None:
        self.job = DataTransformerJob(self.eco, self.getDP().getCredentialStore(), self.dp, "Consumer1", "src/tests/yellow_dp_tests")
        self.common_setup_job(DataTransformerJob, self)

    def test_modelExternalizer(self) -> None:
        # I need to make sure the DT output tables are created.
        # then I want to run the externalizer and check the output tables are populated correctly.

        jobStatus: JobStatus = self.runJob()
        self.assertEqual(jobStatus, JobStatus.DONE)

        self.assertEqual(1, 1)

    def tearDown(self) -> None:
        self.baseTearDown()
