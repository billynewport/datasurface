"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest

from typing import Optional
from sqlalchemy import text, MetaData
from sqlalchemy.engine import Engine
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
from datasurface.platforms.yellow import db_utils
from datasurface.platforms.yellow.jobs import Job, JobStatus
from datasurface.platforms.yellow.yellow_dp import BatchState, BatchStatus
from datasurface.md import Ecosystem, PostgresDatabase, SQLServerDatabase, DB2Database
from datasurface.md import Datastore, SQLIngestion
from datasurface.md.governance import DatastoreCacheEntry, DataMilestoningStrategy, OracleDatabase, WorkspacePlatformConfig
from datasurface.md import DataPlatform, HostPortSQLDatabase
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform, YellowMilestoneStrategy, YellowDatasetUtilities
from datasurface.md.lint import ValidationTree
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md.credential import CredentialStore, Credential
from typing import cast
from abc import ABC
from datasurface.platforms.yellow.merge_live import SnapshotMergeJobLiveOnly
from datasurface.platforms.yellow.data_ops_factory import DatabaseOperationsFactory
from datasurface.platforms.yellow.db_utils import createInspector
import os
from datasurface.md import SnowFlakeDatabase


class BaseMergeJobTest(ABC):
    """Base class for SnapshotMergeJob tests (live-only and forensic)"""
    _current_test_instance: Optional['BaseMergeJobTest'] = None

    tree: Optional[ValidationTree]
    dp: Optional[YellowDataPlatform]
    store_entry: Optional[DatastoreCacheEntry]
    store: Optional[Datastore]
    job: Optional[Job]
    source_engine: Optional[Engine]
    merge_engine: Optional[Engine]
    ydu: YellowDatasetUtilities

    def __init__(self, eco: Ecosystem, dpName: str, storeName: str = "Store1") -> None:
        self.eco: Ecosystem = eco
        self.dpName: str = dpName
        self.storeName: str = storeName
        dp: Optional[DataPlatform] = self.eco.getDataPlatform(self.dpName)
        if dp is None:
            raise Exception("Platform not found")
        self.dp = cast(YellowDataPlatform, dp)

        store_entry: Optional[DatastoreCacheEntry] = self.eco.cache_getDatastore(storeName)
        if store_entry is None:
            raise Exception(f"Store {storeName} not found")
        self.store_entry = store_entry
        self.store = self.store_entry.datastore

        # Initialize database operations for test cleanup
        self.db_ops = None

    @staticmethod
    def loadEcosystem(path: str) -> Optional[Ecosystem]:
        eco: Optional[Ecosystem] = None
        tree: Optional[ValidationTree] = None
        eco, tree = loadEcosystemFromEcoModule(path)
        if eco is None or tree is None:
            raise Exception("Failed to load ecosystem")
        if tree.hasErrors():
            tree.printTree()
            raise Exception("Ecosystem validation failed")
        return eco

    def baseSetUp(self) -> None:
        # Set the current test instance for the mock to use
        BaseSnapshotMergeJobTest._current_test_instance = self

        self.tree = None
        self.source_engine = None
        self.merge_engine = None

        self.overrideJobConnections()
        self.overrideCredentialStore()
        assert self.dp is not None
        assert self.store is not None
        # Create YellowDatasetUtilities after credential store override to ensure it uses the mock
        self.ydu: YellowDatasetUtilities = YellowDatasetUtilities(self.eco, self.dp.psp.credStore, self.dp, self.store)

        # Initialize database operations for test utilities
        if self.ydu.schemaProjector is not None:
            self.db_ops = DatabaseOperationsFactory.create_database_operations(
                self.dp.psp.mergeStore, self.ydu.schemaProjector
            )

        self.setupDatabases()

    def baseTearDown(self) -> None:
        self.cleanupBatchTables()
        if hasattr(self, 'source_engine') and self.source_engine is not None:
            with self.source_engine.begin() as conn:
                if self.db_ops is None:
                    raise Exception("Database operations not initialized - this is a critical system error")

                # Use database operations for cleanup
                for dataset in self.store.datasets.values():
                    drop_sql = self.db_ops.get_drop_table_sql(dataset.name)
                    try:
                        conn.execute(text(drop_sql))
                    except Exception:
                        pass  # Table might not exist
                conn.commit()
            self.source_engine.dispose()
        if hasattr(self, 'merge_engine') and self.merge_engine is not None:
            with self.merge_engine.begin() as conn:
                if self.db_ops is None:
                    raise Exception("Database operations not initialized - this is a critical system error")

                # Use database operations for cleanup
                tables_to_drop = [
                    self.ydu.getPhysBatchCounterTableName(),
                    self.ydu.getPhysBatchMetricsTableName()
                ]
                for dataset in self.store.datasets.values():
                    tables_to_drop.append(self.ydu.getPhysMergeTableNameForDataset(dataset))
                    tables_to_drop.append(self.ydu.getPhysStagingTableNameForDataset(dataset))

                for table_name in tables_to_drop:
                    drop_sql = self.db_ops.get_drop_table_sql(table_name)
                    try:
                        conn.execute(text(drop_sql))
                    except Exception:
                        pass  # Table might not exist
                conn.commit()
            self.merge_engine.dispose()

        # Reset any mocked credential stores to prevent test isolation issues
        if hasattr(self, 'dp') and self.dp is not None and hasattr(self.dp, 'psp'):
            # Store original credential store if it exists, otherwise set to None
            if not hasattr(self, '_original_cred_store'):
                self._original_cred_store = None
            if self._original_cred_store is not None:
                self.dp.psp.credStore = self._original_cred_store

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

        if isinstance(self.dp.psp.mergeStore, PostgresDatabase):
            mock_cred_store = MockCredentialStore("postgres", "postgres")
        elif isinstance(self.dp.psp.mergeStore, SQLServerDatabase):
            mock_cred_store = MockCredentialStore("sa", "pass@w0rd")
        elif isinstance(self.dp.psp.mergeStore, OracleDatabase):
            mock_cred_store = MockCredentialStore("system", "pass@w0rd")
        elif isinstance(self.dp.psp.mergeStore, DB2Database):
            mock_cred_store = MockCredentialStore("db2inst1", "pass@w0rd")
        elif isinstance(self.dp.psp.mergeStore, SnowFlakeDatabase):
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
            raise Exception(f"Unsupported merge store type: {type(self.dp.psp.mergeStore)}")

        if self.job is not None:
            self.job.credStore = mock_cred_store  # type: ignore[attr-defined]

        # Also mock the data platform's credential store for resetBatchState method
        assert self.dp is not None

        # Store original credential store before overriding
        if hasattr(self.dp.psp, 'credStore'):
            self._original_cred_store = self.dp.psp.credStore
        else:
            self._original_cred_store = None

        self.dp.psp.credStore = mock_cred_store  # type: ignore[attr-defined]

    def setupDatabases(self) -> None:

        # Create source engine based on merge store type for consistency
        assert isinstance(self.store.cmd, SQLIngestion)
        assert self.store.cmd.credential is not None
        username, password = self.dp.psp.credStore.getAsUserPassword(self.store.cmd.credential)
        assert self.store.cmd.dataContainer is not None
        # Allow Snowflake containers in addition to host/port SQL databases
        assert isinstance(self.store.cmd.dataContainer, (HostPortSQLDatabase, SnowFlakeDatabase))
        self.source_engine = db_utils.createEngine(self.store.cmd.dataContainer, username, password)

        # Create merge engine using the actual merge store configuration
        username, password = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.mergeRW_Credential)
        self.merge_engine = db_utils.createEngine(self.dp.psp.mergeStore, username, password)
        self.createSourceTable()
        self.createMergeDatabase()
        inspector = createInspector(self.merge_engine)
        with self.merge_engine.begin() as mergeConnection:
            if self.job is not None:
                # Drop the meta tables so we can start fresh using database operations
                if self.db_ops is None:
                    raise Exception("Database operations not initialized - this is a critical system error")

                # Use database operations for table cleanup
                tables_to_drop = [
                    self.ydu.getPhysBatchCounterTableName(),
                    self.ydu.getPhysBatchMetricsTableName()
                ]
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

                if not inspector.has_table(self.ydu.getPhysBatchCounterTableName()):
                    self.job.createBatchCounterTable(mergeConnection, inspector)
                if not inspector.has_table(self.ydu.getPhysBatchMetricsTableName()):
                    self.job.createBatchMetricsTable(mergeConnection, inspector)

    def checkCurrentBatchIs(self, key: str, expected_batch: int, tc: unittest.TestCase) -> None:
        """Check the batch status for a given key"""
        with self.merge_engine.begin() as conn:
            nm = self.ydu.dp.psp.namingMapper
            result = conn.execute(
                text(f'SELECT {nm.fmtCol("currentBatch")} FROM {self.ydu.getPhysBatchCounterTableName()} WHERE {nm.fmtCol("key")} = \'' + key + '\''))
            row = result.fetchone()
            current_batch = row[0] if row else 0
            tc.assertEqual(current_batch, expected_batch)

    def checkSpecificBatchStatus(self, key: str, batch_id: int, expected_status: BatchStatus, tc: unittest.TestCase) -> None:
        """Check the batch status for a given batch id"""
        with self.merge_engine.begin() as conn:
            # Get batch status
            nm = self.ydu.dp.psp.namingMapper
            batch_status_col = nm.fmtCol("batch_status")
            table_name = self.ydu.getPhysBatchMetricsTableName()
            key_col = nm.fmtCol("key")
            batch_id_col = nm.fmtCol("batch_id")
            result = conn.execute(
                text(f'SELECT {batch_status_col} FROM {table_name} WHERE {key_col} = \'{key}\' AND {batch_id_col} = {batch_id}'))
            row = result.fetchone()
            batch_status = row[0] if row else "None"
            tc.assertEqual(batch_status, expected_status.value)

    def common_test_first_batch_started(self, tc: unittest.TestCase) -> None:
        """Test that the first batch is started"""
        assert self.job is not None
        assert self.merge_engine is not None

        # Ensure staging table exists before calling startBatch
        assert self.store is not None
        with self.merge_engine.begin() as mergeConnection:
            self.ydu.reconcileStagingTableSchemas(mergeConnection, createInspector(self.merge_engine), self.store)

        self.job.startBatch(self.merge_engine)  # type: ignore[attr-defined]
        self.checkSpecificBatchStatus(self.store.name, 1, BatchStatus.STARTED, tc)

    def common_test_BatchState(self, tc: unittest.TestCase) -> None:
        """Test the BatchState class"""
        state = BatchState(all_datasets=[self.store.datasets["people"].name])
        tc.assertEqual(state.all_datasets, ["people"])
        tc.assertEqual(state.current_dataset_index, 0)
        tc.assertEqual(state.current_offset, 0)

        tc.assertTrue(state.hasMoreDatasets())

        # Move to next dataset
        state.moveToNextDataset()
        tc.assertFalse(state.hasMoreDatasets())

    def createSourceTable(self) -> None:
        metadata = MetaData()
        if self.store is None:
            raise Exception("Store not set")
        assert self.source_engine is not None
        for dataset in self.store.datasets.values():
            datasetToSQLAlchemyTable(dataset, dataset.name, metadata, self.source_engine)
        metadata.create_all(self.source_engine)

    def createMergeDatabase(self) -> None:
        username, password = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.mergeRW_Credential)
        merge_engine = db_utils.createEngine(self.dp.psp.mergeStore, username, password)
        databaseName = self.dp.psp.mergeStore.databaseName
        with merge_engine.begin() as conn:
            conn.execute(text("COMMIT"))
            try:
                conn.execute(text(f"CREATE DATABASE {databaseName}"))
            except Exception:
                pass
        merge_engine.dispose()

    def cleanupBatchTables(self) -> None:

        if not hasattr(self, 'merge_engine'):
            # Use the same connection logic as setupDatabases
            username, password = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.mergeRW_Credential)
            self.merge_engine = db_utils.createEngine(self.dp.psp.mergeStore, username, password)

            # Initialize database operations if not already available
            if self.db_ops is None and hasattr(self, 'ydu') and self.ydu.schemaProjector is not None:
                self.db_ops = DatabaseOperationsFactory.create_database_operations(
                    self.dp.psp.mergeStore, self.ydu.schemaProjector
                )

        with self.merge_engine.begin() as conn:
            if self.db_ops is None:
                raise Exception("Database operations not initialized - this is a critical system error")

            # Use database operations for cleanup
            tables_to_drop = [
                self.ydu.getPhysBatchCounterTableName(),
                self.ydu.getPhysBatchMetricsTableName()
            ]
            for table_name in tables_to_drop:
                drop_sql = self.db_ops.get_drop_table_sql(table_name)
                try:
                    conn.execute(text(drop_sql))
                except Exception:
                    pass  # Table might not exist

    def common_setup_job(self, job_class, tc: unittest.TestCase) -> None:
        """Common job setup pattern"""
        # Call the base class setUp to initialize eco, dp, store, etc.
        assert self.eco is not None
        assert self.dp is not None
        assert self.store is not None
        self.job = job_class(
            self.eco,
            self.dp.getCredentialStore(),
            self.dp,
            self.store
        )
        self.baseSetUp()

    def runJob(self) -> JobStatus:
        if self.job is None:
            raise Exception("Job not set")
        status = self.job.run()  # type: ignore[attr-defined]
        if status != JobStatus.DONE:
            raise Exception("Job failed with ERROR status")
        return status

    def common_verify_batch_completion(self, batch_id: int, tc: unittest.TestCase) -> None:
        """Common pattern to verify a batch completed successfully"""
        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", batch_id, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs("Store1", batch_id, tc)

    def common_verify_record_exists(self, records: list, record_id: str, expected_values: dict, tc: unittest.TestCase) -> None:
        """Common pattern to verify a record exists with expected values"""
        record = next((r for r in records if r['id'] == record_id), None)
        tc.assertIsNotNone(record, f"Record {record_id} not found")
        for key, value in expected_values.items():
            tc.assertEqual(record[key], value, f"Record {record_id} {key} mismatch")

    def common_verify_record_absent(self, records: list, record_id: str, tc: unittest.TestCase) -> None:
        """Common pattern to verify a record is absent"""
        record = next((r for r in records if r['id'] == record_id), None)
        tc.assertIsNone(record, f"Record {record_id} should not exist")


class BaseSnapshotMergeJobTest(BaseMergeJobTest):

    def __init__(self, eco: Ecosystem, dpName: str, storeName: str = "Store1") -> None:
        super().__init__(eco, dpName, storeName)

    def common_clear_and_insert_data(self, test_data: list, tc: unittest.TestCase) -> None:
        """Common pattern to clear source and insert new test data"""
        with self.source_engine.begin() as conn:
            for dataset in self.store.datasets.values():
                conn.execute(text(f'DELETE FROM {dataset.name}'))
        self.insertTestData(test_data)

    def insertTestData(self, data: list[dict]) -> None:
        from datetime import datetime
        with self.source_engine.begin() as conn:
            for row in data:
                # Convert date strings to datetime objects for Oracle compatibility
                processed_row = row.copy()
                for key in ['dob', 'dod']:
                    if key in processed_row and processed_row[key] is not None:
                        if isinstance(processed_row[key], str):
                            processed_row[key] = datetime.strptime(processed_row[key], '%Y-%m-%d').date()

                conn.execute(text("""
                    INSERT INTO people ("id", "firstName", "lastName", "dob", "employer", "dod")
                    VALUES (:id, :firstName, :lastName, :dob, :employer, :dod)
                """), processed_row)

    def updateTestData(self, id_val: str, updates: dict) -> None:
        from datetime import datetime
        with self.source_engine.begin() as conn:
            # Convert date strings to datetime objects for Oracle compatibility
            processed_updates = updates.copy()
            for key in ['dob', 'dod']:
                if key in processed_updates and processed_updates[key] is not None:
                    if isinstance(processed_updates[key], str):
                        processed_updates[key] = datetime.strptime(processed_updates[key], '%Y-%m-%d').date()

            set_clause = ", ".join([f'"{k}" = :{k}' for k in processed_updates.keys()])
            query = f'UPDATE people SET {set_clause} WHERE "id" = :id'
            params = processed_updates.copy()
            params['id'] = id_val
            conn.execute(text(query), params)

    def deleteTestData(self, id_val: str) -> None:
        with self.source_engine.begin() as conn:
            conn.execute(text('DELETE FROM people WHERE "id" = :id'), {"id": id_val})

    def getMergeTableData(self) -> list:
        with self.merge_engine.begin() as conn:
            # Check if this is a forensic platform (has batch milestoning)
            if self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
                # Forensic table - no ds_surf_batch_id column
                nm = self.ydu.dp.psp.namingMapper
                all_hash_col = nm.fmtCol("ds_surf_all_hash")
                key_hash_col = nm.fmtCol("ds_surf_key_hash")
                batch_in_col = nm.fmtCol("ds_surf_batch_in")
                batch_out_col = nm.fmtCol("ds_surf_batch_out")
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           {all_hash_col}, {key_hash_col},
                           {batch_in_col}, {batch_out_col}
                    FROM {self.ydu.getPhysMergeTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id", {batch_in_col}
                """))
            elif self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
                # Live-only table - has ds_surf_batch_id column
                nm = self.ydu.dp.psp.namingMapper
                batch_id_col = nm.fmtCol("ds_surf_batch_id")
                all_hash_col = nm.fmtCol("ds_surf_all_hash")
                key_hash_col = nm.fmtCol("ds_surf_key_hash")
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           {batch_id_col}, {all_hash_col}, {key_hash_col}
                    FROM {self.ydu.getPhysMergeTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id"
                """))
            else:
                raise Exception(f"Unsupported milestone strategy: {self.dp.milestoneStrategy}")
            return [row._asdict() for row in result.fetchall()]

    def getLiveRecords(self) -> list:
        from datasurface.platforms.yellow.yellow_dp import YellowMilestoneStrategy

        with self.merge_engine.begin() as conn:
            if self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
                # Live-only schema: no batch_in/batch_out columns
                nm = self.ydu.dp.psp.namingMapper
                batch_id_col = nm.fmtCol("ds_surf_batch_id")
                all_hash_col = nm.fmtCol("ds_surf_all_hash")
                key_hash_col = nm.fmtCol("ds_surf_key_hash")
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           {batch_id_col}, {all_hash_col}, {key_hash_col}
                    FROM {self.ydu.getPhysMergeTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id"
                """))
            elif self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
                # Forensic schema: has batch_in/batch_out columns, filter for live records
                # Note: ds_surf_batch_id is not included in forensic tables
                nm = self.ydu.dp.psp.namingMapper
                all_hash_col = nm.fmtCol("ds_surf_all_hash")
                key_hash_col = nm.fmtCol("ds_surf_key_hash")
                batch_in_col = nm.fmtCol("ds_surf_batch_in")
                batch_out_col = nm.fmtCol("ds_surf_batch_out")
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           {all_hash_col}, {key_hash_col},
                           {batch_in_col}, {batch_out_col}
                    FROM {self.ydu.getPhysMergeTableNameForDataset(self.store.datasets["people"])}
                    WHERE {batch_out_col} = 2147483647
                    ORDER BY "id", {batch_in_col}
                """))
            else:
                raise Exception(f"Unsupported milestone strategy: {self.dp.milestoneStrategy}")

            return [row._asdict() for row in result.fetchall()]

    def common_test_batch_lifecycle_steps(self, tc: unittest.TestCase) -> None:
        """Common batch lifecycle test steps"""
        # Step 1: Run batch 1 with empty source table
        print("Step 1: Running batch 1 with empty source table")
        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 1, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 1, tc)

        # Verify merge table is empty
        merge_data = self.getMergeTableData()
        tc.assertEqual(len(merge_data), 0)

        # Step 2: Insert 5 rows and run batch 2
        print("Step 2: Inserting 5 rows and running batch 2")
        test_data = [
            {"id": "1", "firstName": "John", "lastName": "Doe", "dob": "1980-01-01", "employer": "Company A", "dod": None},
            {"id": "2", "firstName": "Jane", "lastName": "Smith", "dob": "1985-02-15", "employer": "Company B", "dod": None},
            {"id": "3", "firstName": "Bob", "lastName": "Johnson", "dob": "1975-03-20", "employer": "Company C", "dod": None},
            {"id": "4", "firstName": "Alice", "lastName": "Brown", "dob": "1990-04-10", "employer": "Company D", "dod": None},
            {"id": "5", "firstName": "Charlie", "lastName": "Wilson", "dob": "1982-05-25", "employer": "Company E", "dod": None}
        ]
        self.insertTestData(test_data)

        # Debug: Verify data was inserted
        with self.source_engine.begin() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM people'))
            count = result.fetchone()[0]
            print(f"DEBUG: After insert, people table has {count} rows")
            tc.assertEqual(count, 5)

        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 1, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 2, tc)
        self.checkSpecificBatchStatus(self.store.name, 2, BatchStatus.COMMITTED, tc)

        # Verify all 5 rows are in merge table with batch_id = 2
        merge_data = self.getMergeTableData()
        tc.assertEqual(len(merge_data), 5)
        for row in merge_data:
            tc.assertEqual(row['ds_surf_batch_id'], 2)

        # Step 3: Update a row and delete another, then run batch 3
        print("Step 3: Updating row 1 and deleting row 3, then running batch 3")
        self.updateTestData("1", {"employer": "Company X", "firstName": "Johnny"})
        self.deleteTestData("3")

        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 3, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 3, tc)

        # Verify updated row has new batch_id and new data
        merge_data = self.getMergeTableData()
        tc.assertEqual(len(merge_data), 4)  # One row deleted

        # Check updated row
        self.common_verify_record_exists(merge_data, "1", {
            'ds_surf_batch_id': 3,
            'firstName': 'Johnny',
            'employer': 'Company X'
        }, tc)

        # Verify deleted row is gone
        self.common_verify_record_absent(merge_data, "3", tc)

        # Step 4: Re-insert the deleted row and run batch 4
        print("Step 4: Re-inserting row 3 and running batch 4")
        self.insertTestData([{"id": "3", "firstName": "Bob", "lastName": "Johnson", "dob": "1975-03-20", "employer": "Company C", "dod": None}])

        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 4, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 4, tc)

        # Verify re-inserted row is present with batch_id = 4
        merge_data = self.getMergeTableData()
        tc.assertEqual(len(merge_data), 5)
        self.common_verify_record_exists(merge_data, "3", {'ds_surf_batch_id': 4}, tc)

        # Step 5: Run another batch with no changes
        print("Step 5: Running batch 5 with no changes")
        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 5, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 5, tc)

        # Verify no changes occurred - unchanged rows should keep their previous batch_ids
        merge_data_after = self.getMergeTableData()
        tc.assertEqual(len(merge_data_after), 5)

        # All rows should still have their previous batch_ids (unchanged rows keep original batch_id)
        for row in merge_data_after:
            if row['id'] == '1':
                tc.assertEqual(row['ds_surf_batch_id'], 3)  # Updated in batch 3
            elif row['id'] == '3':
                tc.assertEqual(row['ds_surf_batch_id'], 4)  # Re-inserted in batch 4
            else:
                tc.assertEqual(row['ds_surf_batch_id'], 2)  # Original batch 2, unchanged

        tc.assertEqual(self.job.numReconcileDDLs, 1)
        print("All batch lifecycle tests passed!")


class TestSnapshotMergeJob(BaseSnapshotMergeJobTest, unittest.TestCase):
    """Test the SnapshotMergeJob with a simple ecosystem (live-only)"""

    def __init__(self, methodName: str = "runTest") -> None:
        eco: Optional[Ecosystem] = BaseSnapshotMergeJobTest.loadEcosystem("src/tests/yellow_dp_tests")
        assert eco is not None
        dp: YellowDataPlatform = cast(YellowDataPlatform, eco.getDataPlatformOrThrow("Test_DP"))
        dp.milestoneStrategy = YellowMilestoneStrategy.SCD1

        # Set the consumer to live-only mode
        req: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, eco.cache_getWorkspaceOrThrow("Consumer1").workspace.dsgs["TestDSG"].platformMD)
        req.retention.milestoningStrategy = DataMilestoningStrategy.LIVE_ONLY
        BaseSnapshotMergeJobTest.__init__(self, eco, dp.name, "Store1")
        unittest.TestCase.__init__(self, methodName)

    def setUp(self) -> None:
        self.common_setup_job(SnapshotMergeJobLiveOnly, self)

    def tearDown(self) -> None:
        self.baseTearDown()

    def getMergeTableData(self) -> list:
        """Override to only select live-only columns (no batch_in/batch_out)"""
        with self.merge_engine.begin() as conn:
            # Try both possible table names
            try:
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM {self.ydu.getPhysMergeTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id"
                """))
            except Exception:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM {self.ydu.getPhysMergeTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id"
                """))
            return [row._asdict() for row in result.fetchall()]

    def test_BatchState(self) -> None:
        self.common_test_BatchState(self)

    def test_first_batch_started(self) -> None:
        self.common_test_first_batch_started(self)

    def test_full_batch_lifecycle(self) -> None:
        """Test the complete batch processing lifecycle"""
        self.common_test_batch_lifecycle_steps(self)

    def getStagingTableData(self) -> list:
        """Get all data from the staging table"""
        with self.merge_engine.begin() as conn:
            try:
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM {self.ydu.getPhysStagingTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id"
                """))
            except Exception:
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM {self.ydu.getPhysStagingTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id"
                """))
            return [row._asdict() for row in result.fetchall()]

    def test_reset_committed_batch_fails(self) -> None:
        """Test that trying to reset a committed batch fails"""
        # Step 1: Insert test data and run a complete batch to completion
        test_data = [
            {"id": "1", "firstName": "John", "lastName": "Doe", "dob": "1980-01-01",
             "employer": "Company A", "dod": None},
            {"id": "2", "firstName": "Jane", "lastName": "Smith", "dob": "1985-02-15",
             "employer": "Company B", "dod": None}
        ]
        self.insertTestData(test_data)

        # Run the job until completion (should commit the batch)
        self.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.COMMITTED, self)

        # Step 2: Try to reset the committed batch - this should fail
        assert self.eco is not None
        assert self.dp is not None
        result = self.dp.resetBatchState(self.eco, "Store1")

        # Check that the reset failed with the expected error message
        self.assertEqual(result, "ERROR: Batch is COMMITTED and cannot be reset")

        # Verify batch status is still COMMITTED (unchanged)
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.COMMITTED, self)

    def test_reset_nonexistent_datastore_fails(self) -> None:
        """Test that trying to reset a non-existent datastore fails"""
        assert self.eco is not None
        assert self.dp is not None

        # Try to reset a datastore that doesn't exist
        result = self.dp.resetBatchState(self.eco, "NonExistentStore")

        # Check that the reset failed with the expected error message
        self.assertEqual(result, "ERROR: Could not find datastore in ecosystem")


if __name__ == "__main__":
    unittest.main()
