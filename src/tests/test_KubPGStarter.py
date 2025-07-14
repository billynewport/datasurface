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
        # Since Store1 has multi-dataset ingestion, there should be one DAG for the platform
        self.assertIn("test-dp_ingestion", dag_content)
        self.assertIn("Store1_job", dag_content)
        self.assertIn("Store1_branch", dag_content)
        self.assertIn("Store1_reschedule", dag_content)
        self.assertIn("Store1_wait", dag_content)
        self.assertIn("Store1_fail", dag_content)
        
        # Check for SQL snapshot ingestion specific elements
        self.assertIn("--operation', 'snapshot-merge'", dag_content)
        self.assertIn("--store-name', 'Store1'", dag_content)
        
        # Check for source database credentials (SQL snapshot ingestion)
        self.assertIn("postgres_USER", dag_content)  # Source database credential
        self.assertIn("postgres_PASSWORD", dag_content)  # Source database credential
        
        # Check for merge store credentials (should use the same postgres_USER/PASSWORD for now)
        # Already checked above
        
        # Check for git and slack credentials
        self.assertIn("git_TOKEN", dag_content)
        self.assertIn("slack_TOKEN", dag_content)
        
        # Check for proper task dependencies
        self.assertIn("start_task >> [", dag_content)
        self.assertIn("Store1_job >> Store1_branch", dag_content)
        self.assertIn("Store1_branch >> [Store1_reschedule, Store1_wait, Store1_fail]", dag_content)
        
        # Check for proper DAG configuration
        self.assertIn("schedule='@hourly'", dag_content)
        self.assertIn("catchup=False", dag_content)
        self.assertIn("tags=['datasurface', 'ingestion', 'test-dp']", dag_content)
        
        # Check for Kubernetes pod operator configuration
        self.assertIn("KubernetesPodOperator", dag_content)
        self.assertIn("datasurface/datasurface:latest", dag_content)
        self.assertIn("python', '-m', 'datasurface.platforms.kubpgstarter.jobs", dag_content)
        
        # Check for return code handling logic
        self.assertIn("determine_next_action", dag_content)
        self.assertIn("BranchPythonOperator", dag_content)
        self.assertIn("TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS", dag_content)
        
        # Verify no Kafka-specific elements (since this is SQL snapshot ingestion)
        self.assertNotIn("kafka_connect_credential_secret_name", dag_content)
        self.assertNotIn("kafka_topic", dag_content)
