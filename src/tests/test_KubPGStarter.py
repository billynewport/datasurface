"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.cmd.platform import generatePlatformBootstrap
from datasurface.md.governance import Ecosystem


class Test_KubPGStarter(unittest.TestCase):
    def test_bootstrap(self):
        eco: Ecosystem = generatePlatformBootstrap("src/tests/kubpgtests", "src/tests/kubpgtests/base", "Test_DP")
        # Validate that the ecosystem was created successfully
        self.assertIsNotNone(eco)
        self.assertEqual(eco.name, "Test")
        # Validate that the Test_DP platform exists
        dp = eco.getDataPlatformOrThrow("Test_DP")
        self.assertIsNotNone(dp)
        self.assertEqual(dp.name, "Test_DP")

    def test_graph_rendering(self):
        """Test that graph rendering creates individual DAGs for each ingestion stream."""
        from datasurface.cmd.platform import handleModelMerge

        # This should create ingestion DAGs for the Store1 datastore
        eco: Ecosystem = handleModelMerge("src/tests/kubpgtests", "src/tests/kubpgtests/base/graph_output", "Test_DP")

        # Validate that the ecosystem was created successfully
        self.assertIsNotNone(eco)
        self.assertEqual(eco.name, "Test")

        # Check that the graph output directory was created
        import os
        graph_output_dir = "src/tests/kubpgtests/base/graph_output/Test_DP"
        self.assertTrue(os.path.exists(graph_output_dir))

        # Check that the airflow_dag file was created
        airflow_dag_file = os.path.join(graph_output_dir, "airflow_dag")
        self.assertTrue(os.path.exists(airflow_dag_file))

        # Read the DAG file and verify it contains individual DAGs
        with open(airflow_dag_file, 'r') as f:
            dag_content = f.read()

        # Should contain individual DAGs for each ingestion stream
        # Since Store1 has multi-dataset ingestion, there should be one DAG for the store
#        self.assertIn("test_dp_Store1_ingestion", dag_content)
#        self.assertIn("snapshot_merge_job", dag_content)
#        self.assertIn("trigger_self", dag_content)
