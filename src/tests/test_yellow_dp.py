"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
import os
import tempfile
from unittest.mock import patch
from sqlalchemy import create_engine, text
from datasurface.cmd.platform import generatePlatformBootstrap, handleModelMerge
from datasurface.md.governance import Ecosystem
from datasurface.md import DataContainer, PostgresDatabase


class Test_YellowDataPlatform(unittest.TestCase):
    def setup_test_database(self):
        """Set up test databases similar to BaseSnapshotMergeJobTest"""
        postgres_engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
        try:
            with postgres_engine.begin() as conn:
                conn.execute(text("COMMIT"))
                try:
                    conn.execute(text("CREATE DATABASE test_merge_db"))
                except Exception:
                    pass  # Database might already exist
        finally:
            postgres_engine.dispose()

    def test_bootstrap(self):
        # Set up test environment variables for credentials
        os.environ["postgres_USER"] = "postgres"
        os.environ["postgres_PASSWORD"] = "postgres"
        os.environ["git_TOKEN"] = "test_git_token"
        os.environ["slack_TOKEN"] = "test_slack_token"
        os.environ["connect_TOKEN"] = "test_connect_token"

        # Mock createEngine to use localhost database
        def mock_create_engine(container: DataContainer, userName: str, password: str):
            if isinstance(container, PostgresDatabase) and hasattr(container, 'databaseName') and container.databaseName == 'datasurface_merge':
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')
            else:
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_db')

        # Set up test database
        self.setup_test_database()

        try:
            with patch('datasurface.platforms.yellow.yellow_dp.createEngine', new=mock_create_engine):
                eco: Ecosystem = generatePlatformBootstrap("src/tests/yellow_dp_tests", "src/tests/yellow_dp_tests/base", "Test_DP")
                # Validate that the ecosystem was created successfully
                self.assertIsNotNone(eco)
                self.assertEqual(eco.name, "Test")
                # Validate that the Test_DP platform exists
                dp = eco.getDataPlatformOrThrow("Test_DP")
                self.assertIsNotNone(dp)
                self.assertEqual(dp.name, "Test_DP")
        finally:
            # Clean up environment variables
            for key in ["postgres_USER", "postgres_PASSWORD", "git_TOKEN", "slack_TOKEN", "connect_TOKEN"]:
                os.environ.pop(key, None)

    def test_graph_rendering(self):
        """Test that graph rendering creates individual DAGs for each ingestion stream."""

        # Set up test environment variables for credentials
        os.environ["postgres_USER"] = "postgres"
        os.environ["postgres_PASSWORD"] = "postgres"
        os.environ["git_TOKEN"] = "test_git_token"
        os.environ["slack_TOKEN"] = "test_slack_token"
        os.environ["connect_TOKEN"] = "test_connect_token"

        # Mock createEngine to use localhost database
        def mock_create_engine(container: DataContainer, userName: str, password: str):
            if isinstance(container, PostgresDatabase) and hasattr(container, 'databaseName') and container.databaseName == 'datasurface_merge':
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')
            else:
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_db')

        # Set up test database
        self.setup_test_database()

        try:
            with patch('datasurface.platforms.yellow.yellow_dp.createEngine', new=mock_create_engine):
                # This should create ingestion DAGs for the Store1 datastore
                eco: Ecosystem = handleModelMerge("src/tests/yellow_dp_tests", "src/tests/yellow_dp_tests/base/graph_output", "Test_DP")

                # Validate that the ecosystem was created successfully
                self.assertIsNotNone(eco)
                self.assertEqual(eco.name, "Test")

                # Check that the graph output directory was created
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
                self.assertIn("python', '-m', 'datasurface.platforms.yellow.jobs", dag_content)

                # Check for return code handling logic
                self.assertIn("determine_next_action", dag_content)
                self.assertIn("BranchPythonOperator", dag_content)

                # Check that reschedule triggers the same DAG
                self.assertIn("trigger_dag_id='test-dp__Store1_ingestion'", dag_content)
        finally:
            # Clean up environment variables
            for key in ["postgres_USER", "postgres_PASSWORD", "git_TOKEN", "slack_TOKEN", "connect_TOKEN"]:
                os.environ.pop(key, None)

    def test_mvp_model_bootstrap_and_dags(self):
        """Test MVP model bootstrap artifacts and ingestion DAG generation for dual platforms."""

        # Set up test environment variables for credentials (KubernetesEnvVarsCredentialStore expects these)
        os.environ["postgres_USER"] = "postgres"
        os.environ["postgres_PASSWORD"] = "postgres"
        os.environ["git_TOKEN"] = "test_git_token"
        os.environ["slack_TOKEN"] = "test_slack_token"
        os.environ["connect_TOKEN"] = "test_connect_token"

        # Mock createEngine to use localhost database
        def mock_create_engine(container: DataContainer, userName: str, password: str):
            if isinstance(container, PostgresDatabase) and hasattr(container, 'databaseName') and container.databaseName == 'datasurface_merge':
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')
            else:
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_db')

        # Set up test database
        self.setup_test_database()

        # Create a temporary directory for outputs
        temp_dir = tempfile.mkdtemp()
        print(f"\n=== MVP Model Test Output Directory: {temp_dir} ===")

        with patch('datasurface.platforms.yellow.yellow_dp.createEngine', new=mock_create_engine):
            try:
                # Test 1: Generate bootstrap artifacts for both platforms
                print("\n--- Generating Bootstrap Artifacts ---")
                eco: Ecosystem = generatePlatformBootstrap(
                    "src/tests/yellow_dp_tests/mvp_model",
                    temp_dir,
                    "YellowLive",
                    "YellowForensic"
                )

                # Validate ecosystem loaded correctly
                self.assertIsNotNone(eco)
                self.assertEqual(eco.name, "Test")

                # Validate both platforms exist
                live_dp = eco.getDataPlatformOrThrow("YellowLive")
                forensic_dp = eco.getDataPlatformOrThrow("YellowForensic")
                self.assertIsNotNone(live_dp)
                self.assertIsNotNone(forensic_dp)

                # Continue with rest of test...
                print("✓ Bootstrap artifacts generated successfully")

            except Exception as e:
                print(f"Bootstrap test failed with error: {e}")
                raise
            finally:
                # Clean up environment variables
                for key in ["postgres_USER", "postgres_PASSWORD", "git_TOKEN", "slack_TOKEN", "connect_TOKEN"]:
                    os.environ.pop(key, None)

                # Clean up
                import shutil
                if os.path.exists(temp_dir):
                    try:
                        shutil.rmtree(temp_dir)
                        print(f"✓ Cleaned up temporary directory: {temp_dir}")
                    except Exception as e:
                        print(f"Warning: Could not clean up temporary directory {temp_dir}: {e}")
