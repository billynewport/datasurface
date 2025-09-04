"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from tests.test_MergeSnapshotLiveOnly import BaseSnapshotMergeJobTest
from datasurface.md import Ecosystem
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
from datasurface.platforms.yellow.yellow_dp import YellowMilestoneStrategy
from datasurface.md.governance import DataTransformerOutput, WorkspacePlatformConfig, DataMilestoningStrategy
from typing import cast
from datasurface.md.model_schema import addDatasurfaceModel
import unittest
from datasurface.md.lint import ValidationTree
from datasurface.platforms.yellow.transformerjob import DataTransformerJob
from datasurface.platforms.yellow.jobs import JobStatus
from datasurface.platforms.yellow.yellow_dp import YellowDatasetUtilities
from datasurface.platforms.yellow.data_ops_factory import DatabaseOperationsFactory
from datasurface.platforms.yellow.db_utils import createInspector
from sqlalchemy import text
from datasurface.md.credential import CredentialStore, Credential
import os
from datasurface.md import PostgresDatabase, SQLServerDatabase, DB2Database, SnowFlakeDatabase, OracleDatabase
from datasurface.md import CaptureMetaData, Datastore
from datasurface.platforms.yellow import db_utils


class TestYellowTransformers(unittest.TestCase):
    """Test the SnapshotMergeJob with a simple ecosystem (live-only)"""

    def __init__(self, methodName: str = "runTest") -> None:
        unittest.TestCase.__init__(self, methodName)
        e = BaseSnapshotMergeJobTest.loadEcosystem("src/tests/yellow_dp_tests")
        if e is None:
            raise Exception("Ecosystem not loaded")
        self.eco: Ecosystem = e
        addDatasurfaceModel(self.eco, self.eco.owningRepo)
        tree: ValidationTree = self.eco.lintAndHydrateCaches()
        if tree.hasErrors():
            tree.printTree()
            self.assertFalse(tree.hasErrors())
        self.dp: YellowDataPlatform = cast(YellowDataPlatform, self.eco.getDataPlatformOrThrow("Test_DP"))
        self.dp.milestoneStrategy = YellowMilestoneStrategy.SCD1
        self.store: Datastore = self.eco.cache_getDatastoreOrThrow("Datasurface").datastore

        # Set the consumer to live-only mode
        req: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, self.eco.cache_getWorkspaceOrThrow("Consumer1").workspace.dsgs["TestDSG"].platformMD)
        req.retention.milestoningStrategy = DataMilestoningStrategy.LIVE_ONLY

    def baseSetUp(self) -> None:
        # Set the current test instance for the mock to use
        self.tree = None
        self.source_engine = None
        self.merge_engine = None

        self.overrideJobConnections()
        self.overrideCredentialStore()
        # Create YellowDatasetUtilities after credential store override to ensure it uses the mock
        self.ydu: YellowDatasetUtilities = YellowDatasetUtilities(self.eco, self.dp.getPSP().credStore, self.dp, self.store)

        # Initialize database operations for test utilities
        if self.ydu.mergeSchemaProjector is not None:
            self.db_ops = DatabaseOperationsFactory.create_database_operations(
                self.dp.getPSP().mergeStore, self.ydu.mergeSchemaProjector
            )

        self.setupDatabases()

    def baseTearDown(self) -> None:
        if self.merge_engine is not None:
            with self.merge_engine.begin() as conn:
                if self.db_ops is None:
                    raise Exception("Database operations not initialized - this is a critical system error")

                # Use database operations for cleanup
                tables_to_drop = []
                for dataset in self.store.datasets.values():
                    tables_to_drop.append(self.ydu.getPhysMergeTableNameForDataset(dataset))
                    tables_to_drop.append(self.ydu.getPhysStagingTableNameForDataset(dataset))
                    tables_to_drop.append(self.ydu.getPhysDataTransformerOutputTableNameForDatasetForIngestionOnly(self.store, dataset))

                for table_name in tables_to_drop:
                    drop_sql = self.db_ops.get_drop_table_sql(table_name)
                    try:
                        conn.execute(text(drop_sql))
                    except Exception:
                        pass  # Table might not exist
                conn.commit()
            self.merge_engine.dispose()

        # Reset any mocked credential stores to prevent test isolation issues
        if self.dp is not None:
            # Store original credential store if it exists, otherwise set to None
            if not hasattr(self, '_original_cred_store'):
                self._original_cred_store = None
            if self._original_cred_store is not None:
                self.dp.getPSP().credStore = self._original_cred_store

        # Clear the current test instance reference
        if BaseSnapshotMergeJobTest._current_test_instance == self:
            BaseSnapshotMergeJobTest._current_test_instance = None

    def overrideJobConnections(self) -> None:
        # This method is now a no-op since we only need to mock the credential store
        # The db_utils.createEngine function will automatically use the mocked credentials
        pass

    def overrideCredentialStore(self) -> None:
        class MockCredentialStore(CredentialStore):

            def __init__(self, user: str, password: str) -> None:
                super().__init__("MockCredentialStore", set())
                self.user = user
                self.password = password

            def checkCredentialIsAvailable(self, cred: Credential, tree) -> None:
                pass

            def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
                # Return appropriate credentials based on the credential name
                if cred.name == "postgres":
                    return "postgres", "postgres"
                elif cred.name == "sa":
                    return "sa", "pass@w0rd"
                else:
                    # Default fallback to the initialized credentials
                    return self.user, self.password

            def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
                raise NotImplementedError("MockCredentialStore does not support certificates")

            def getAsToken(self, cred: Credential) -> str:
                raise NotImplementedError("MockCredentialStore does not support tokens")

            def lintCredential(self, cred: Credential, tree) -> None:
                pass

        if isinstance(self.dp.getPSP().mergeStore, PostgresDatabase):
            mock_cred_store = MockCredentialStore("postgres", "postgres")
        elif isinstance(self.dp.getPSP().mergeStore, SQLServerDatabase):
            mock_cred_store = MockCredentialStore("sa", "pass@w0rd")
        elif isinstance(self.dp.getPSP().mergeStore, OracleDatabase):
            mock_cred_store = MockCredentialStore("system", "pass@w0rd")
        elif isinstance(self.dp.getPSP().mergeStore, DB2Database):
            mock_cred_store = MockCredentialStore("db2inst1", "pass@w0rd")
        elif isinstance(self.dp.getPSP().mergeStore, SnowFlakeDatabase):
            # Read password from environment variable, SNOWFLAKE_PASSWORD
            password = os.getenv("SNOWFLAKE_PASSWORD")
            if password is None:
                raise Exception("SNOWFLAKE_PASSWORD environment variable is not set")
            # Read username from environment variable, SNOWFLAKE_USERNAME
            username = os.getenv("SNOWFLAKE_USERNAME")
            if username is None:
                raise Exception("SNOWFLAKE_USERNAME environment variable is not set")
            mock_cred_store = MockCredentialStore(username, password)
        else:
            raise Exception(f"Unsupported merge store type: {type(self.dp.getPSP().mergeStore)}")

        if self.job is not None:
            self.job.credStore = mock_cred_store  # type: ignore[attr-defined]

        # Also mock the data platform's credential store for resetBatchState method

        # Store original credential store before overriding
        if hasattr(self.dp.getPSP(), 'credStore'):
            self._original_cred_store = self.dp.getPSP().credStore
        else:
            self._original_cred_store = None

        self.dp.getPSP().credStore = mock_cred_store  # type: ignore[attr-defined]

    def createMergeDatabase(self) -> None:
        databaseName = self.dp.getPSP().mergeStore.databaseName
        if self.merge_engine is None:
            raise Exception("Merge engine not initialized")
        with self.merge_engine.begin() as conn:
            conn.execute(text("COMMIT"))
            try:
                conn.execute(text(f"CREATE DATABASE {databaseName}"))
            except Exception:
                pass

    def setupDatabases(self) -> None:
        cmd: CaptureMetaData = self.store.getCMD()
        if not isinstance(cmd, DataTransformerOutput):
            raise ValueError("Store command is not an DataTransformerOutput")

        # Create merge engine using the actual merge store configuration
        username, password = self.dp.getPSP().credStore.getAsUserPassword(self.dp.getPSP().mergeRW_Credential)
        self.merge_engine = db_utils.createEngine(self.dp.getPSP().mergeStore, username, password)
        self.createMergeDatabase()
        inspector = createInspector(self.merge_engine)
        with self.merge_engine.begin() as mergeConnection:
            if self.job is not None:
                # Drop the meta tables so we can start fresh using database operations
                if self.db_ops is None:
                    raise Exception("Database operations not initialized - this is a critical system error")

                # Create the merge batch counter and metrics tables
                if not inspector.has_table(self.ydu.getPhysBatchCounterTableName()):
                    self.job.createBatchCounterTable(mergeConnection, inspector)
                if not inspector.has_table(self.ydu.getPhysBatchMetricsTableName()):
                    self.job.createBatchMetricsTable(mergeConnection, inspector)

                # Use database operations for table cleanup (only staging and merge tables, not batch counter/metrics)
                tables_to_drop = []
                # Add staging and merge tables for all datasets
                for dataset in self.store.datasets.values():
                    tables_to_drop.append(self.ydu.getPhysMergeTableNameForDataset(dataset))
                    tables_to_drop.append(self.ydu.getPhysStagingTableNameForDataset(dataset))

                for table_name in tables_to_drop:
                    drop_sql = self.db_ops.get_drop_table_sql(table_name)
                    # If it exists then just delete all the rows otherwise create the table
                    if inspector.has_table(table_name):
                        mergeConnection.execute(text(f"DELETE FROM {table_name}"))
                    else:
                        try:
                            mergeConnection.execute(text(drop_sql))
                        except Exception:
                            pass  # Table might not exist

    def setUp(self) -> None:
        self.job = DataTransformerJob(self.eco, self.dp.getPSP().credStore, self.dp, "Datasurface_ModelExternalization", "src/tests/yellow_dp_tests")
        self.baseSetUp()

    def test_modelExternalizer(self) -> None:
        # I need to make sure the DT output tables are created.
        # then I want to run the externalizer and check the output tables are populated correctly.

        jobStatus: JobStatus = self.job.run(self.dp.getPSP().credStore)
        self.assertEqual(jobStatus, JobStatus.DONE)

        self.assertEqual(1, 1)

    def tearDown(self) -> None:
        self.baseTearDown()
