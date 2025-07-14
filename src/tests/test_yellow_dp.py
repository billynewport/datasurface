"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.cmd.platform import generatePlatformBootstrap
from datasurface.md.governance import Ecosystem


class Test_YellowDataPlatform(unittest.TestCase):
    def test_bootstrap(self):
        eco: Ecosystem = generatePlatformBootstrap("src/tests/yellow_dp_tests", "src/tests/yellow_dp_tests/base", "Test_DP")
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
        eco: Ecosystem = handleModelMerge("src/tests/yellow_dp_tests", "src/tests/yellow_dp_tests/base/graph_output", "Test_DP")

        # Validate that the ecosystem was created successfully
        self.assertIsNotNone(eco)
        self.assertEqual(eco.name, "Test")

        # Check that the graph output directory was created
        import os
        graph_output_dir = "src/tests/yellow_dp_tests/base/graph_output/Test_DP"
        self.assertTrue(os.path.exists(graph_output_dir))

        # Check that individual DAG files were created for each ingestion stream
        # Since Store1 has multi-dataset ingestion, there should be one DAG for the store
        store1_dag_file = os.path.join(graph_output_dir, "test-dp__Store1_ingestion.py")
        self.assertTrue(os.path.exists(store1_dag_file), f"Expected DAG file {store1_dag_file} to exist")

        # Read the DAG file and verify it contains the correct structure
        with open(store1_dag_file, 'r') as f:
            dag_content = f.read()

        # Should contain individual DAG for Store1 ingestion stream
        self.assertIn("test-dp__Store1_ingestion", dag_content)  # DAG name
        self.assertIn("snapshot_merge_job", dag_content)  # Job task
        self.assertIn("check_result", dag_content)  # Branch task
        self.assertIn("reschedule_immediately", dag_content)  # Reschedule task
        self.assertIn("wait_for_trigger", dag_content)  # Wait task

        # Check for SQL snapshot ingestion specific elements
        self.assertIn("--operation', 'snapshot-merge'", dag_content)
        self.assertIn("--store-name', 'Store1'", dag_content)

        # Check for source database credentials (SQL snapshot ingestion)
        self.assertIn("postgres_USER", dag_content)  # Source database credential
        self.assertIn("postgres_PASSWORD", dag_content)  # Source database credential

        # Check for git and slack credentials
        self.assertIn("git_TOKEN", dag_content)
        self.assertIn("slack_TOKEN", dag_content)

        # Check for proper task dependencies
        self.assertIn("job >> branch", dag_content)
        self.assertIn("branch >> [reschedule, wait]", dag_content)

        # Check for proper DAG configuration
        self.assertIn("schedule='@hourly'", dag_content)
        self.assertIn("catchup=False", dag_content)
        self.assertIn("tags=['datasurface', 'ingestion', 'test-dp', 'Store1']", dag_content)

        # Check for Kubernetes pod operator configuration
        self.assertIn("KubernetesPodOperator", dag_content)
        self.assertIn("datasurface/datasurface:latest", dag_content)
        self.assertIn("python', '-m', 'datasurface.platforms.kubpgstarter.jobs", dag_content)

        # Check for return code handling logic
        self.assertIn("determine_next_action", dag_content)
        self.assertIn("BranchPythonOperator", dag_content)

        # Check that reschedule triggers the same DAG
        self.assertIn("trigger_dag_id='test-dp__Store1_ingestion'", dag_content)

        # Verify no Kafka-specific elements (since this is SQL snapshot ingestion)
        self.assertNotIn("kafka_connect_credential_secret_name", dag_content)
        self.assertNotIn("kafka_topic", dag_content)
