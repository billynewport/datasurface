"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from typing import Optional
from sqlalchemy import create_engine, text, Table, MetaData, Column, String, Date
from sqlalchemy.engine import Engine
from datasurface.platforms.kubpgstarter.jobs import SnapshotMergeJob, JobStatus, BatchState, BatchStatus
from datasurface.md import Ecosystem
from datasurface.md import Datastore, DataContainer
from datasurface.md.governance import DatastoreCacheEntry
from datasurface.md import DataPlatform
from datasurface.platforms.kubpgstarter.kubpgstarter import KubernetesPGStarterDataPlatform
from datasurface.md.lint import ValidationTree
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md.credential import CredentialStore, Credential
from typing import cast


class TestSnapshotMergeJob(unittest.TestCase):
    """Test the SnapshotMergeJob with a simple ecosystem"""

    def setUp(self) -> None:
        """Set up test environment before each test"""

        # Create ecosystem using existing eco.py
        self.eco: Optional[Ecosystem] = None
        self.tree: Optional[ValidationTree] = None
        self.eco, self.tree = loadEcosystemFromEcoModule("src/tests/kubpgtests")

        if self.eco is None or self.tree is None:
            raise Exception("Failed to load ecosystem")
        if self.tree.hasErrors():
            self.tree.printTree()
            raise Exception("Ecosystem validation failed")

        # Get platform and store
        dp: Optional[DataPlatform] = self.eco.getDataPlatform("Test_DP")
        if dp is None:
            raise Exception("Platform not found")
        self.dp = dp

        store_entry: Optional[DatastoreCacheEntry] = self.eco.cache_getDatastore("Store1")
        if store_entry is None:
            raise Exception("Store not found")
        self.store_entry = store_entry
        self.store: Datastore = self.store_entry.datastore

        # Create job instance
        self.job: SnapshotMergeJob = SnapshotMergeJob(
            self.eco,
            self.dp.getCredentialStore(),
            cast(KubernetesPGStarterDataPlatform, self.dp),
            self.store
        )

        # Override the job's database connections to use localhost for testing
        self.overrideJobConnections()

        # Override the credential store to return local credentials
        self.overrideCredentialStore()

        self.setupDatabases()

    def tearDown(self) -> None:
        """Clean up test environment"""
        # Drop source table
        if hasattr(self, 'source_engine'):
            with self.source_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS people CASCADE'))
                conn.commit()
            self.source_engine.dispose()
        # Drop merge and batch tables
        if hasattr(self, 'merge_engine'):
            with self.merge_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS "Test_DP_Store1_people_merge" CASCADE'))
                conn.execute(text('DROP TABLE IF EXISTS "test_dp_batch_counter" CASCADE'))
                conn.execute(text('DROP TABLE IF EXISTS "test_dp_batch_metrics" CASCADE'))
                conn.commit()
            self.merge_engine.dispose()

    def overrideJobConnections(self) -> None:
        """Override the job's database connections to use localhost for testing"""
        # Monkey patch the job's createEngine method to use localhost
        def local_create_engine(container: DataContainer, userName: str, password: str) -> Engine:
            # Check if this is the merge store (PostgresDatabase with specific name)
            from datasurface.md import PostgresDatabase
            if isinstance(container, PostgresDatabase) and hasattr(container, 'databaseName') and container.databaseName == 'datasurface_merge':
                # Use local merge database
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')
            else:
                # Use local source database
                return create_engine('postgresql://postgres:postgres@localhost:5432/test_db')

        self.job.createEngine = local_create_engine

    def overrideCredentialStore(self) -> None:
        """Override the credential store to return local credentials for testing"""
        # Create a mock credential store that returns local credentials
        class MockCredentialStore(CredentialStore):
            def __init__(self):
                super().__init__("MockCredentialStore", set())

            def checkCredentialIsAvailable(self, cred: Credential, tree) -> None:
                pass

            def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
                # Return local PostgreSQL credentials
                return "postgres", "postgres"

            def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
                raise NotImplementedError("MockCredentialStore does not support certificates")

            def getAsToken(self, cred: Credential) -> str:
                raise NotImplementedError("MockCredentialStore does not support tokens")

            def lintCredential(self, cred: Credential, tree) -> None:
                pass

        # Replace the job's credential store
        self.job.credStore = MockCredentialStore()

    def setupDatabases(self) -> None:
        """Set up source and merge databases"""
        # Create source database
        self.source_engine = create_engine('postgresql://postgres:postgres@localhost:5432/test_db')

        # Create merge database
        self.merge_engine = create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')

        # Create source table
        self.createSourceTable()

        # Create merge database if it doesn't exist
        self.createMergeDatabase()
        self.job.createBatchCounterTable(self.merge_engine)
        self.job.createBatchMetricsTable(self.merge_engine)

    def createSourceTable(self) -> None:
        """Create the source table with test data"""
        metadata = MetaData()
        Table(
            'people', metadata,
            Column('id', String(20), primary_key=True),
            Column('firstName', String(100), nullable=False),
            Column('lastName', String(100), nullable=False),
            Column('dob', Date(), nullable=False),
            Column('employer', String(100), nullable=True),
            Column('dod', Date(), nullable=True)
        )

        # Create table
        metadata.create_all(self.source_engine)

    def createMergeDatabase(self) -> None:
        """Create the merge database"""
        # Connect to postgres to create database
        postgres_engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
        with postgres_engine.begin() as conn:
            conn.execute(text("COMMIT"))  # Close any open transaction
            try:
                conn.execute(text("CREATE DATABASE test_merge_db"))
            except Exception:
                # Database might already exist, that's okay
                pass
        postgres_engine.dispose()

    def insertTestData(self, data: list) -> None:
        """Insert test data into source table"""
        with self.source_engine.begin() as conn:
            for row in data:
                conn.execute(text("""
                    INSERT INTO people ("id", "firstName", "lastName", "dob", "employer", "dod")
                    VALUES (:id, :firstName, :lastName, :dob, :employer, :dod)
                """), row)

    def updateTestData(self, id_val: str, updates: dict) -> None:
        """Update test data in source table"""
        with self.source_engine.begin() as conn:
            set_clause = ", ".join([f'"{k}" = :{k}' for k in updates.keys()])
            query = f'UPDATE people SET {set_clause} WHERE "id" = :id'
            params = updates.copy()
            params['id'] = id_val
            conn.execute(text(query), params)

    def deleteTestData(self, id_val: str) -> None:
        """Delete test data from source table"""
        with self.source_engine.begin() as conn:
            conn.execute(text('DELETE FROM people WHERE "id" = :id'), {"id": id_val})

    def getMergeTableData(self) -> list:
        """Get data from merge table"""
        with self.merge_engine.begin() as conn:
            result = conn.execute(text("""
                SELECT "id", "firstName", "lastName", "dob", "employer", "dod",
                       ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                FROM Test_DP_Store1_people_merge
                ORDER BY "id"
            """))
            return [row._asdict() for row in result.fetchall()]

    def runJob(self) -> JobStatus:
        """Run the snapshot merge job until completion"""
        max_iterations = 10  # Prevent infinite loops
        iteration = 0

        while iteration < max_iterations:
            status = self.job.run()
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

    def test_BatchState(self) -> None:
        """Test the BatchState class"""
        state = BatchState([self.store.datasets["people"].name])
        self.assertEqual(state.all_datasets, ["people"])
        self.assertEqual(state.current_dataset_index, 0)
        self.assertEqual(state.current_offset, 0)

        self.assertTrue(state.hasMoreDatasets())

        # Move to next dataset
        state.moveToNextDataset()
        self.assertFalse(state.hasMoreDatasets())

    def checkCurrentBatchIs(self, key: str, expected_batch: int) -> None:
        """Check the batch status for a given key"""
        with self.merge_engine.begin() as conn:
            result = conn.execute(text('SELECT "currentBatch" FROM "test_dp_batch_counter" WHERE "key" = \'' + key + '\''))
            row = result.fetchone()
            current_batch = row[0] if row else 0
            self.assertEqual(current_batch, expected_batch)

    def checkSpecificBatchStatus(self, key: str, batch_id: int, expected_status: BatchStatus) -> None:
        """Check the batch status for a given batch id"""
        with self.merge_engine.begin() as conn:
            # Get batch status
            result = conn.execute(text('SELECT "batch_status" FROM "test_dp_batch_metrics" WHERE "key" = \'' + key + '\' AND "batch_id" = ' + str(batch_id)))
            row = result.fetchone()
            batch_status = row[0] if row else "None"
            self.assertEqual(batch_status, expected_status.value)

    def test_first_batch_started(self) -> None:
        """Test that the first batch is started"""
        self.job.startBatch(self.merge_engine)
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.STARTED)

    def test_full_batch_lifecycle(self) -> None:
        """Test the complete batch processing lifecycle"""

        # Step 1: Run batch 1 with empty source table
        print("Step 1: Running batch 1 with empty source table")
        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)

        # Check batch status - first actual batch is batch 1
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.COMMITTED)
        self.checkCurrentBatchIs("Store1", 1)

        # Verify merge table is empty
        merge_data = self.getMergeTableData()
        self.assertEqual(len(merge_data), 0)

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
            self.assertEqual(count, 5)

        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)

        # Check batch status
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.COMMITTED)
        self.checkCurrentBatchIs("Store1", 2)
        self.checkSpecificBatchStatus("Store1", 2, BatchStatus.COMMITTED)

        # Verify all 5 rows are in merge table with batch_id = 2
        # The records were first ingested in batch 2 (batch 1 had empty source), so they should have batch_id = 2
        merge_data = self.getMergeTableData()
        self.assertEqual(len(merge_data), 5)
        for row in merge_data:
            self.assertEqual(row['ds_surf_batch_id'], 2)

        # Step 3: Update a row and delete another, then run batch 3
        print("Step 3: Updating row 1 and deleting row 3, then running batch 3")
        self.updateTestData("1", {"employer": "Company X", "firstName": "Johnny"})
        self.deleteTestData("3")

        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)

        # Check batch status
        self.checkSpecificBatchStatus("Store1", 3, BatchStatus.COMMITTED)
        self.checkCurrentBatchIs("Store1", 3)

        # Verify updated row has new batch_id and new data
        merge_data = self.getMergeTableData()
        self.assertEqual(len(merge_data), 4)  # One row deleted

        # Check updated row
        updated_row = next((row for row in merge_data if row['id'] == '1'), None)
        self.assertIsNotNone(updated_row)
        self.assertEqual(updated_row['ds_surf_batch_id'], 3)
        self.assertEqual(updated_row['firstName'], 'Johnny')
        self.assertEqual(updated_row['employer'], 'Company X')

        # Verify deleted row is gone
        deleted_row = next((row for row in merge_data if row['id'] == '3'), None)
        self.assertIsNone(deleted_row)

        # Step 4: Re-insert the deleted row and run batch 4
        print("Step 4: Re-inserting row 3 and running batch 4")
        self.insertTestData([{"id": "3", "firstName": "Bob", "lastName": "Johnson", "dob": "1975-03-20", "employer": "Company C", "dod": None}])

        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)

        # Check batch status
        self.checkSpecificBatchStatus("Store1", 4, BatchStatus.COMMITTED)
        self.checkCurrentBatchIs("Store1", 4)

        # Verify re-inserted row is present with batch_id = 4
        merge_data = self.getMergeTableData()
        self.assertEqual(len(merge_data), 5)

        reinserted_row = next((row for row in merge_data if row['id'] == '3'), None)
        self.assertIsNotNone(reinserted_row)
        self.assertEqual(reinserted_row['ds_surf_batch_id'], 4)

        # Step 5: Run another batch with no changes
        print("Step 5: Running batch 5 with no changes")
        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)

        # Check batch status
        self.checkSpecificBatchStatus("Store1", 5, BatchStatus.COMMITTED)
        self.checkCurrentBatchIs("Store1", 5)

        # Verify no changes occurred - unchanged rows should keep their previous batch_ids
        merge_data_after = self.getMergeTableData()
        self.assertEqual(len(merge_data_after), 5)

        # All rows should still have their previous batch_ids (unchanged rows keep original batch_id)
        for row in merge_data_after:
            if row['id'] == '1':
                self.assertEqual(row['ds_surf_batch_id'], 3)  # Updated in batch 3
            elif row['id'] == '3':
                self.assertEqual(row['ds_surf_batch_id'], 4)  # Re-inserted in batch 4
            else:
                self.assertEqual(row['ds_surf_batch_id'], 2)  # Original batch 2, unchanged

        print("All batch lifecycle tests passed!")

    def cleanupBatchTables(self) -> None:
        """Clean up batch counter and metrics tables to ensure clean state"""
        # Create merge database connection if it doesn't exist yet
        if not hasattr(self, 'merge_engine'):
            self.merge_engine = create_engine('postgresql://postgres:postgres@localhost:5432/test_merge_db')

        with self.merge_engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS "test_dp_batch_counter" CASCADE'))
            conn.execute(text('DROP TABLE IF EXISTS "test_dp_batch_metrics" CASCADE'))


if __name__ == "__main__":
    unittest.main()
