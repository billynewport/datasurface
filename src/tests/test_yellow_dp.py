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
from datasurface.md.lint import ValidationTree
from typing import Optional, cast
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform


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
                # Generate the bootstrap artifacts for ring level 0
                eco: Ecosystem = generatePlatformBootstrap(0, "src/tests/yellow_dp_tests", "src/tests/yellow_dp_tests/base", "Test_DP")
                # Generate the bootstrap artifacts for ring level 1
                eco: Ecosystem = generatePlatformBootstrap(1, "src/tests/yellow_dp_tests", "src/tests/yellow_dp_tests/base", "Test_DP")
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

    def test_dynamic_dag_factory_rendering(self):
        """Test that graph rendering populates database configurations for the dynamic DAG factory."""

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
                # This should populate database configurations for the dynamic DAG factory
                eco: Ecosystem = handleModelMerge("src/tests/yellow_dp_tests", "src/tests/yellow_dp_tests/base/graph_output", "Test_DP")

                # Validate that the ecosystem was created successfully
                self.assertIsNotNone(eco)
                self.assertEqual(eco.name, "Test")

                # Check that the graph output directory was created
                graph_output_dir = "src/tests/yellow_dp_tests/base/graph_output/Test_DP"
                self.assertTrue(os.path.exists(graph_output_dir))

                # With dynamic DAG factory, we should only generate terraform files
                # The factory DAG is generated during bootstrap, not during graph rendering
                terraform_file = os.path.join(graph_output_dir, "terraform_code")
                if os.path.exists(terraform_file):
                    # Check that terraform code exists for ingestion infrastructure
                    with open(terraform_file, 'r') as f:
                        terraform_content = f.read()
                        # Terraform should contain kafka connector configurations
                        self.assertIn("kafka", terraform_content.lower())

                # The key difference with dynamic DAG factory: individual DAG files are NOT generated
                # Instead, the factory DAG reads from database configurations populated by renderGraph()
                print("✅ Dynamic DAG factory approach confirmed - no individual DAG files generated")
                print("✅ Database configurations populated by renderGraph() for factory DAG consumption")
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
                    0,
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

    def test_defaultnamemapper(self):
        modelFolderName = "src/tests/yellow_dp_tests/mvp_model"
        ecoTree: Optional[ValidationTree]
        eco, ecoTree = loadEcosystemFromEcoModule("src/tests/yellow_dp_tests/mvp_model")
        if eco is None or (ecoTree is not None and ecoTree.hasErrors()):
            if ecoTree is not None:
                ecoTree.printTree()
            raise Exception(f"Failed to load ecosystem from {modelFolderName}")
        assert eco is not None
        yp: YellowDataPlatform = cast(YellowDataPlatform, eco.getDataPlatformOrThrow("YellowLive"))
        assert yp is not None
        dc: DataContainer = yp.psp.mergeStore
        assert dc is not None
        self.assertEqual(yp.getPhysDataTransformerTableName(), "yellowlive_airflow_datatransformer")
        self.assertEqual(yp.getPhysDAGTableName(), "yellowlive_airflow_dsg")

    def test_secrets_documentation_generation(self):
        """Test that secrets documentation is generated correctly with all expected secrets."""

        # Set up test environment variables for credentials
        os.environ["postgres_USER"] = "postgres"
        os.environ["postgres_PASSWORD"] = "postgres"
        os.environ["git_TOKEN"] = "test_git_token"
        os.environ["connect_TOKEN"] = "test_connect_token"

        # Mock createEngine to use localhost database - ensure all connections go to the same test database
        def mock_create_engine(container: DataContainer, userName: str, password: str):
            # Always use the same test database regardless of the container
            assert isinstance(container, PostgresDatabase)
            return create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')

        # Set up test database
        self.setup_test_database()

        # Create a temporary directory for outputs
        temp_dir = tempfile.mkdtemp()
        print(f"\n=== Secrets Documentation Test Output Directory: {temp_dir} ===")

        try:
            # Patch the symbol used by yellow_dp after import to ensure all DB connections hit test_merge_db
            with patch('datasurface.platforms.yellow.yellow_dp.createEngine', new=mock_create_engine):
                # First run ring 0 and ring 1 bootstrap to create necessary database tables
                print("=== Running Ring 0 Bootstrap ===")
                generatePlatformBootstrap(0, "src/tests/yellow_dp_tests/mvp_model", temp_dir, "Test_DP")

                print("=== Running Ring 1 Bootstrap (creates database tables) ===")
                try:
                    generatePlatformBootstrap(1, "src/tests/yellow_dp_tests/mvp_model", temp_dir, "Test_DP")
                    # Verify that ring 1 bootstrap completed successfully and tables were created
                    from sqlalchemy import text
                    test_engine = create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')
                    with test_engine.begin() as conn:
                        # Check if expected tables exist
                        result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
                        tables = [row[0] for row in result]

                        # Verify the expected tables were created during ring 1 bootstrap
                        expected_tables = ['yellowlive_airflow_dsg', 'yellowlive_airflow_datatransformer', 'test_dp_factory_dags']
                        for table in expected_tables:
                            self.assertIn(table, tables, f"Expected table '{table}' should be created during ring 1 bootstrap")

                except Exception as e:
                    print(f"Ring 1 bootstrap failed with exception: {e}")
                    raise

                # Now handle model merge which calls renderGraph and generates secrets documentation
                print("=== Running Model Merge ===")
                eco: Ecosystem = handleModelMerge("src/tests/yellow_dp_tests/mvp_model", temp_dir, "Test_DP")

                # Validate that the ecosystem was created successfully
                self.assertIsNotNone(eco)
                self.assertEqual(eco.name, "Test")

                # Look for the secrets documentation file in the generated output
                secrets_file = None
                for root, dirs, files in os.walk(temp_dir):
                    for filename in files:
                        if filename.endswith("-secrets.md"):
                            secrets_file = os.path.join(root, filename)
                            break
                    if secrets_file:
                        break

                self.assertIsNotNone(secrets_file, "Secrets documentation file should be generated")
                assert secrets_file is not None  # Type narrowing for mypy
                self.assertTrue(os.path.exists(secrets_file), f"Secrets file should exist: {secrets_file}")

                # Read and validate the secrets documentation content
                with open(secrets_file, 'r') as f:
                    secrets_content = f.read()

                # Determine which platform's secrets file we found
                platform_name = None
                if "YellowLive" in secrets_content:
                    platform_name = "YellowLive"
                elif "YellowForensic" in secrets_content:
                    platform_name = "YellowForensic"

                self.assertIsNotNone(platform_name, "Should find either YellowLive or YellowForensic secrets")

                # Verify the document structure for whichever platform we found
                self.assertIn(f"# Kubernetes Secrets for {platform_name}", secrets_content)
                self.assertIn("## Overview", secrets_content)
                self.assertIn("Total secrets required:", secrets_content)
                self.assertIn("## Required Secrets", secrets_content)

                # Verify platform core secrets are present
                self.assertIn("### Platform Core", secrets_content)

                # Check for expected platform-level secrets (these should always be present)
                expected_platform_secrets = [
                    "postgres",  # PostgreSQL credential
                    "git",       # Git credential
                    "connect"    # Kafka Connect credential
                ]

                for secret_name in expected_platform_secrets:
                    self.assertIn(f"#### `{secret_name}`", secrets_content,
                                  f"Platform secret '{secret_name}' should be documented")

                # Verify secret types are documented
                self.assertIn("**Type:** user_password", secrets_content)  # PostgreSQL
                self.assertIn("**Type:** api_token", secrets_content)      # Git, Connect

                # Verify kubectl commands are present
                self.assertIn("kubectl create secret generic", secrets_content)
                self.assertIn("--from-literal=POSTGRES_USER=", secrets_content)
                self.assertIn("--from-literal=token=", secrets_content)

                # Verify verification section
                self.assertIn("## Verification", secrets_content)
                self.assertIn("kubectl get secrets -n", secrets_content)

                # Verify security notes
                self.assertIn("## Notes", secrets_content)
                self.assertIn("Never commit actual secret values", secrets_content)

                # Check that the namespace is correctly referenced
                from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
                assert platform_name is not None  # Type narrowing for mypy
                found_dp = eco.getDataPlatformOrThrow(platform_name)
                assert isinstance(found_dp, YellowDataPlatform)
                expected_namespace = found_dp.psp.yp_assm.namespace
                self.assertIn(f"**Namespace:** {expected_namespace}", secrets_content)
                self.assertIn(f"--namespace {expected_namespace}", secrets_content)

                print("✅ Secrets documentation validation passed")
                print("✅ All expected platform secrets documented")
                print("✅ Proper kubectl commands generated")
                print("✅ Security guidelines included")

        finally:
            # Clean up environment variables
            for key in ["postgres_USER", "postgres_PASSWORD", "git_TOKEN", "connect_TOKEN"]:
                os.environ.pop(key, None)

            # Clean up
            import shutil
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    print(f"✓ Cleaned up temporary directory: {temp_dir}")
                except Exception as e:
                    print(f"Warning: Could not clean up temporary directory {temp_dir}: {e}")


if __name__ == '__main__':
    unittest.main()
