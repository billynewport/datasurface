"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from unittest.mock import patch
from typing import Optional
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.engine import Engine
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
from datasurface.platforms.yellow.jobs import Job, JobStatus
from datasurface.platforms.yellow.yellow_dp import BatchState, BatchStatus
from datasurface.md import Ecosystem
from datasurface.md import Datastore, DataContainer, Dataset
from datasurface.md.governance import DatastoreCacheEntry, DataMilestoningStrategy, WorkspacePlatformConfig
from datasurface.md import DataPlatform
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform, YellowMilestoneStrategy
from datasurface.md.lint import ValidationTree
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md.credential import CredentialStore, Credential
from typing import cast
from abc import ABC, abstractmethod


class BaseSnapshotMergeJobTest(ABC):
    """Base class for SnapshotMergeJob tests (live-only and forensic)"""
    eco: Optional[Ecosystem]
    tree: Optional[ValidationTree]
    dp: Optional[YellowDataPlatform]
    store_entry: Optional[DatastoreCacheEntry]
    store: Optional[Datastore]
    job: Optional[Job]
    source_engine: Optional[Engine]
    merge_engine: Optional[Engine]

    @abstractmethod
    def preprocessEcosystemModel(self) -> None:
        pass

    def setUp(self) -> None:
        self.eco = None
        self.tree = None
        self.dp = None
        self.store_entry = None
        self.store = None
        self.job = None
        self.source_engine = None
        self.merge_engine = None
        self.eco, self.tree = loadEcosystemFromEcoModule("src/tests/yellow_dp_tests")
        if self.eco is None or self.tree is None:
            raise Exception("Failed to load ecosystem")
        if self.tree.hasErrors():
            self.tree.printTree()
            raise Exception("Ecosystem validation failed")
        dp: Optional[DataPlatform] = self.eco.getDataPlatform("Test_DP")
        if dp is None:
            raise Exception("Platform not found")
        self.dp = cast(YellowDataPlatform, dp)
        store_entry: Optional[DatastoreCacheEntry] = self.eco.cache_getDatastore("Store1")
        if store_entry is None:
            raise Exception("Store not found")
        self.store_entry = store_entry
        self.store = self.store_entry.datastore

        # Allow subclasses to postprocess the ecosystem model
        self.preprocessEcosystemModel()

        self.overrideJobConnections()
        self.overrideCredentialStore()
        self.setupDatabases()

    def tearDown(self) -> None:
        if hasattr(self, '_engine_patcher'):
            self._engine_patcher.stop()
        self.cleanupBatchTables()
        if hasattr(self, 'source_engine') and self.source_engine is not None:
            with self.source_engine.begin() as conn:
                conn.execute(text('DROP TABLE IF EXISTS people CASCADE'))
                conn.commit()
            self.source_engine.dispose()
        if hasattr(self, 'merge_engine') and self.merge_engine is not None:
            with self.merge_engine.begin() as conn:
                conn.execute(text('DROP TABLE IF EXISTS "test_dp_store1_people_merge" CASCADE'))
                conn.execute(text('DROP TABLE IF EXISTS "Test_DP_Store1_people_merge" CASCADE'))
                conn.execute(text('DROP TABLE IF EXISTS "test_dp_batch_counter" CASCADE'))
                conn.execute(text('DROP TABLE IF EXISTS "test_dp_batch_metrics" CASCADE'))
                conn.commit()
            self.merge_engine.dispose()

    def overrideJobConnections(self) -> None:
        def local_create_engine(container: DataContainer, userName: str, password: str) -> Engine:
            from datasurface.md import PostgresDatabase
            if isinstance(container, PostgresDatabase) and hasattr(container, 'databaseName') and container.databaseName == 'datasurface_merge':
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')
            else:
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_db')
        patcher = patch('datasurface.platforms.yellow.jobs.createEngine', new=local_create_engine)
        self._engine_patcher = patcher
        patcher.start()

    def overrideCredentialStore(self) -> None:
        class MockCredentialStore(CredentialStore):

            def __init__(self) -> None:
                super().__init__("MockCredentialStore", set())

            def checkCredentialIsAvailable(self, cred: Credential, tree) -> None:
                pass

            def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
                return "postgres", "postgres"

            def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
                raise NotImplementedError("MockCredentialStore does not support certificates")

            def getAsToken(self, cred: Credential) -> str:
                raise NotImplementedError("MockCredentialStore does not support tokens")

            def lintCredential(self, cred: Credential, tree) -> None:
                pass

        mock_cred_store = MockCredentialStore()
        
        if self.job is not None:
            self.job.credStore = mock_cred_store  # type: ignore[attr-defined]
            
        # Also mock the data platform's credential store for resetBatchState method
        if self.dp is not None:
            self.dp.credStore = mock_cred_store  # type: ignore[attr-defined]

    def setupDatabases(self) -> None:
        self.source_engine = create_engine('postgresql://postgres:postgres@localhost:5432/test_db')
        # This needs to used the values in the self.dp.mergeStore
        host = self.dp.mergeStore.hostPortPair.hostName
        port = self.dp.mergeStore.hostPortPair.port
        db_name = self.dp.mergeStore.databaseName
        self.merge_engine = create_engine(f'postgresql://postgres:postgres@{host}:{port}/{db_name}')
        self.createSourceTable()
        self.createMergeDatabase()
        if self.job is not None:
            self.job.createBatchCounterTable(self.merge_engine)  # type: ignore[attr-defined]
            self.job.createBatchMetricsTable(self.merge_engine)  # type: ignore[attr-defined]

    def checkCurrentBatchIs(self, key: str, expected_batch: int, tc: unittest.TestCase) -> None:
        """Check the batch status for a given key"""
        with self.merge_engine.begin() as conn:
            result = conn.execute(text('SELECT "currentBatch" FROM "test_dp_batch_counter" WHERE "key" = \'' + key + '\''))
            row = result.fetchone()
            current_batch = row[0] if row else 0
            tc.assertEqual(current_batch, expected_batch)

    def checkSpecificBatchStatus(self, key: str, batch_id: int, expected_status: BatchStatus, tc: unittest.TestCase) -> None:
        """Check the batch status for a given batch id"""
        with self.merge_engine.begin() as conn:
            # Get batch status
            result = conn.execute(text('SELECT "batch_status" FROM "test_dp_batch_metrics" WHERE "key" = \'' + key + '\' AND "batch_id" = ' + str(batch_id)))
            row = result.fetchone()
            batch_status = row[0] if row else "None"
            tc.assertEqual(batch_status, expected_status.value)

    def common_test_first_batch_started(self, tc: unittest.TestCase) -> None:
        """Test that the first batch is started"""
        assert self.job is not None
        assert self.merge_engine is not None
        self.job.startBatch(self.merge_engine)  # type: ignore[attr-defined]
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.STARTED, tc)

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
        people_dataset: Dataset = self.store.datasets["people"]
        datasetToSQLAlchemyTable(people_dataset, "people", metadata)
        if self.source_engine is not None:
            metadata.create_all(self.source_engine)

    def createMergeDatabase(self) -> None:
        postgres_engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
        with postgres_engine.begin() as conn:
            conn.execute(text("COMMIT"))
            try:
                conn.execute(text("CREATE DATABASE test_merge_db"))
            except Exception:
                pass
        postgres_engine.dispose()

    def insertTestData(self, data: list[dict]) -> None:
        with self.source_engine.begin() as conn:
            for row in data:
                conn.execute(text("""
                    INSERT INTO people ("id", "firstName", "lastName", "dob", "employer", "dod")
                    VALUES (:id, :firstName, :lastName, :dob, :employer, :dod)
                """), row)

    def updateTestData(self, id_val: str, updates: dict) -> None:
        with self.source_engine.begin() as conn:
            set_clause = ", ".join([f'"{k}" = :{k}' for k in updates.keys()])
            query = f'UPDATE people SET {set_clause} WHERE "id" = :id'
            params = updates.copy()
            params['id'] = id_val
            conn.execute(text(query), params)

    def deleteTestData(self, id_val: str) -> None:
        with self.source_engine.begin() as conn:
            conn.execute(text('DELETE FROM people WHERE "id" = :id'), {"id": id_val})

    def getMergeTableData(self) -> list:
        with self.merge_engine.begin() as conn:
            # Try both possible table names
            try:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash,
                           ds_surf_batch_in, ds_surf_batch_out
                    FROM test_dp_store1_people_merge
                    ORDER BY "id", ds_surf_batch_in
                """))
            except Exception:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM Test_DP_Store1_people_merge
                    ORDER BY "id"
                """))
            return [row._asdict() for row in result.fetchall()]

    def getLiveRecords(self) -> list:
        with self.merge_engine.begin() as conn:
            try:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash,
                           ds_surf_batch_in, ds_surf_batch_out
                    FROM test_dp_store1_people_merge
                    WHERE ds_surf_batch_out = 2147483647
                    ORDER BY "id"
                """))
            except Exception:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM Test_DP_Store1_people_merge
                    ORDER BY "id"
                """))
            return [row._asdict() for row in result.fetchall()]

    def runJob(self) -> JobStatus:
        max_iterations = 10
        iteration = 0
        while iteration < max_iterations:
            if self.job is None:
                raise Exception("Job not set")
            status = self.job.run()  # type: ignore[attr-defined]
            print(f"Job iteration {iteration + 1} returned status: {status}")
            if status == JobStatus.DONE:
                return status
            elif status == JobStatus.ERROR:
                raise Exception("Job failed with ERROR status")
            elif status == JobStatus.KEEP_WORKING:
                iteration += 1
                continue
            else:
                raise Exception(f"Unknown job status: {status}")
        raise Exception(f"Job did not complete after {max_iterations} iterations")

    def cleanupBatchTables(self) -> None:
        if not hasattr(self, 'merge_engine'):
            self.merge_engine = create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')
        with self.merge_engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS "test_dp_batch_counter" CASCADE'))
            conn.execute(text('DROP TABLE IF EXISTS "test_dp_batch_metrics" CASCADE'))

    def common_setup_job(self, job_class, tc: unittest.TestCase) -> None:
        """Common job setup pattern"""
        # Call the base class setUp to initialize eco, dp, store, etc.
        BaseSnapshotMergeJobTest.setUp(self)
        assert self.eco is not None
        assert self.dp is not None
        assert self.store is not None
        self.job = job_class(
            self.eco,
            self.dp.getCredentialStore(),
            self.dp,
            self.store
        )
        self.overrideCredentialStore()
        self.setupDatabases()

    def common_verify_batch_completion(self, batch_id: int, tc: unittest.TestCase) -> None:
        """Common pattern to verify a batch completed successfully"""
        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", batch_id, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs("Store1", batch_id, tc)

    def common_clear_and_insert_data(self, test_data: list, tc: unittest.TestCase) -> None:
        """Common pattern to clear source and insert new test data"""
        with self.source_engine.begin() as conn:
            conn.execute(text('DELETE FROM people'))
        self.insertTestData(test_data)

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

    def common_test_batch_lifecycle_steps(self, tc: unittest.TestCase) -> None:
        """Common batch lifecycle test steps"""
        # Step 1: Run batch 1 with empty source table
        print("Step 1: Running batch 1 with empty source table")
        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs("Store1", 1, tc)

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
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs("Store1", 2, tc)
        self.checkSpecificBatchStatus("Store1", 2, BatchStatus.COMMITTED, tc)

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
        self.checkSpecificBatchStatus("Store1", 3, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs("Store1", 3, tc)

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
        self.checkSpecificBatchStatus("Store1", 4, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs("Store1", 4, tc)

        # Verify re-inserted row is present with batch_id = 4
        merge_data = self.getMergeTableData()
        tc.assertEqual(len(merge_data), 5)
        self.common_verify_record_exists(merge_data, "3", {'ds_surf_batch_id': 4}, tc)

        # Step 5: Run another batch with no changes
        print("Step 5: Running batch 5 with no changes")
        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 5, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs("Store1", 5, tc)

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

        print("All batch lifecycle tests passed!")


class TestSnapshotMergeJob(BaseSnapshotMergeJobTest, unittest.TestCase):
    """Test the SnapshotMergeJob with a simple ecosystem (live-only)"""

    def preprocessEcosystemModel(self) -> None:
        # Set the dataplatform to live-only mode
        self.dp.milestoneStrategy = YellowMilestoneStrategy.LIVE_ONLY

        # Set the consumer to live-only mode
        req: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, self.eco.cache_getWorkspaceOrThrow("Consumer1").workspace.dsgs["TestDSG"].platformMD)
        req.retention.milestoningStrategy = DataMilestoningStrategy.LIVE_ONLY

    def setUp(self) -> None:
        from datasurface.platforms.yellow.jobs import SnapshotMergeJobLiveOnly
        self.common_setup_job(SnapshotMergeJobLiveOnly, self)

    def getMergeTableData(self) -> list:
        """Override to only select live-only columns (no batch_in/batch_out)"""
        with self.merge_engine.begin() as conn:
            # Try both possible table names
            try:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM test_dp_store1_people_merge
                    ORDER BY "id"
                """))
            except Exception:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM Test_DP_Store1_people_merge
                    ORDER BY "id"
                """))
            return [row._asdict() for row in result.fetchall()]

    def getLiveRecords(self) -> list:
        """Override to only select live-only columns (no batch_in/batch_out)"""
        with self.merge_engine.begin() as conn:
            try:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM test_dp_store1_people_merge
                    ORDER BY "id"
                """))
            except Exception:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM Test_DP_Store1_people_merge
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
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM test_dp_store1_people_staging
                    ORDER BY "id"
                """))
            except Exception:
                result = conn.execute(text("""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM Test_DP_Store1_people_staging
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

    def test_reset_ingested_batch_success(self) -> None:
        """Test that resetting a batch after ingesting data works correctly"""
        # Step 1: Insert test data 
        test_data = [
            {"id": "1", "firstName": "John", "lastName": "Doe", "dob": "1980-01-01", 
             "employer": "Company A", "dod": None},
            {"id": "2", "firstName": "Jane", "lastName": "Smith", "dob": "1985-02-15", 
             "employer": "Company B", "dod": None},
            {"id": "3", "firstName": "Bob", "lastName": "Johnson", "dob": "1975-03-20", 
             "employer": "Company C", "dod": None}
        ]
        self.insertTestData(test_data)
        
        # Step 2: Run job until it reaches INGESTED state (one iteration should do ingestion only)
        assert self.job is not None
        assert self.merge_engine is not None
        assert self.source_engine is not None
        
        # Run the job once - this should ingest data and set status to INGESTED
        job_status = self.job.run()
        self.assertEqual(job_status, JobStatus.KEEP_WORKING)  # Should continue to merge phase
        
        # The job should have ingested data and set status to INGESTED
        key = "Store1"
        batchId = 1  # First batch
        self.checkSpecificBatchStatus(key, batchId, BatchStatus.INGESTED, self)
        
        # Verify data is in staging table
        staging_data_before = self.getStagingTableData()
        self.assertEqual(len(staging_data_before), 3)  # 3 records ingested
        for row in staging_data_before:
            self.assertEqual(row['ds_surf_batch_id'], batchId)
        
        # Step 3: Reset the batch
        assert self.eco is not None
        assert self.dp is not None
        result = self.dp.resetBatchState(self.eco, "Store1")
        
        # Check that the reset was successful
        self.assertEqual(result, "SUCCESS")
        
        # Step 4: Verify the reset worked correctly
        
        # Check batch status is back to STARTED
        self.checkSpecificBatchStatus(key, batchId, BatchStatus.STARTED, self)
        
        # Check staging table is cleared (no records with this batch_id)
        staging_data_after = self.getStagingTableData()
        self.assertEqual(len(staging_data_after), 0)  # All staging records cleared
        
        # Verify batch state is reset by checking the state in batch_metrics
        with self.merge_engine.begin() as conn:
            result = conn.execute(text(f'''
                SELECT "state"
                FROM test_dp_batch_metrics
                WHERE "key" = '{key}' AND "batch_id" = {batchId}
            '''))
            row = result.fetchone()
            self.assertIsNotNone(row)
            
            # Parse the batch state and verify it's reset
            state = BatchState.model_validate_json(row[0])
            self.assertEqual(state.current_dataset_index, 0)  # Reset to start
            self.assertEqual(state.current_offset, 0)  # Reset to start
            self.assertTrue(state.hasMoreDatasets())  # Should have datasets to process
        
        # Step 5: Verify we can continue processing after reset
        # Re-run the job and it should process from the beginning again
        self.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(key, batchId, BatchStatus.COMMITTED, self)
        
        # Verify the data is now in the merge table (complete processing)
        merge_data = self.getMergeTableData()
        self.assertEqual(len(merge_data), 3)  # All 3 records should be in merge table
        for row in merge_data:
            self.assertEqual(row['ds_surf_batch_id'], batchId)

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
