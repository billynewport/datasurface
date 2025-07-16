"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from sqlalchemy import text
from datasurface.platforms.yellow.jobs import BatchState, BatchStatus, JobStatus
from datasurface.platforms.yellow.yellow_dp import YellowMilestoneStrategy
from datasurface.md.governance import WorkspacePlatformConfig, DataMilestoningStrategy
from src.tests.test_SnapshotMergeJobLiveOnly import BaseSnapshotMergeJobTest
from typing import cast


class TestSnapshotMergeJobForensic(BaseSnapshotMergeJobTest, unittest.TestCase):
    """Test the SnapshotMergeJobForensic with forensic merge scenario"""

    def preprocessEcosystemModel(self) -> None:
        # Set the dataplatform to forensic mode
        self.dp.milestoneStrategy = YellowMilestoneStrategy.BATCH_MILESTONED

        # Set the consumer to forensic mode
        req: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, self.eco.cache_getWorkspaceOrThrow("Consumer1").workspace.dsgs["TestDSG"].platformMD)
        req.retention.milestoningStrategy = DataMilestoningStrategy.FORENSIC

    def setUp(self) -> None:
        super().setUp()
        from datasurface.platforms.yellow.jobs import SnapshotMergeJobForensic
        assert self.eco is not None
        assert self.dp is not None
        assert self.store is not None
        self.job = SnapshotMergeJobForensic(
            self.eco,
            self.dp.getCredentialStore(),
            self.dp,
            self.store
        )
        self.overrideCredentialStore()
        self.setupDatabases()

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
        assert self.job is not None
        assert self.merge_engine is not None
        self.job.startBatch(self.merge_engine)  # type: ignore[attr-defined]
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.STARTED)

    def test_forensic_merge_scenario(self) -> None:
        """Test the forensic merge scenario as described in the example"""

        # Step 1: Batch 1 - Insert <id1, billy, newport>
        print("Step 1: Batch 1 - Insert <id1, billy, newport>")
        test_data = [
            {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None}
        ]
        self.insertTestData(test_data)

        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.COMMITTED)

        # Verify merge table has one live record
        live_records = self.getLiveRecords()
        self.assertEqual(len(live_records), 1)
        record = live_records[0]
        self.assertEqual(record['id'], 'id1')
        self.assertEqual(record['firstName'], 'billy')
        self.assertEqual(record['lastName'], 'newport')
        self.assertEqual(record['ds_surf_batch_in'], 1)
        self.assertEqual(record['ds_surf_batch_out'], 2147483647)  # MaxInt

        # Step 2: Batch 2 - Insert <id1, billy, newport>, <id2, laura, diaz>
        print("Step 2: Batch 2 - Insert <id1, billy, newport>, <id2, laura, diaz>")
        test_data = [
            {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        # Clear and re-insert
        with self.source_engine.begin() as conn:
            conn.execute(text('DELETE FROM people'))
        self.insertTestData(test_data)

        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 2, BatchStatus.COMMITTED)

        # Verify merge table has two live records
        live_records = self.getLiveRecords()
        self.assertEqual(len(live_records), 2)

        # Check id1 record (should still be from batch 1, unchanged)
        id1_record = next((r for r in live_records if r['id'] == 'id1'), None)
        self.assertIsNotNone(id1_record)
        self.assertEqual(id1_record['ds_surf_batch_in'], 1)
        self.assertEqual(id1_record['ds_surf_batch_out'], 2147483647)

        # Check id2 record (new from batch 2)
        id2_record = next((r for r in live_records if r['id'] == 'id2'), None)
        self.assertIsNotNone(id2_record)
        self.assertEqual(id2_record['ds_surf_batch_in'], 2)
        self.assertEqual(id2_record['ds_surf_batch_out'], 2147483647)

        # Step 3: Batch 3 - Insert <id1, william, newport>, <id2, laura, diaz>
        print("Step 3: Batch 3 - Insert <id1, william, newport>, <id2, laura, diaz>")
        test_data = [
            {"id": "id1", "firstName": "william", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        # Clear and re-insert
        with self.source_engine.begin() as conn:
            conn.execute(text('DELETE FROM people'))
        self.insertTestData(test_data)

        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 3, BatchStatus.COMMITTED)

        # Verify forensic history
        all_records = self.getMergeTableData()
        self.assertEqual(len(all_records), 3)  # 3 records total (2 live + 1 historical)

        # Check historical record (id1, billy, newport from batch 1)
        historical_records = [r for r in all_records if r['ds_surf_batch_out'] != 2147483647]
        self.assertEqual(len(historical_records), 1)
        historical_record = historical_records[0]
        self.assertEqual(historical_record['id'], 'id1')
        self.assertEqual(historical_record['firstName'], 'billy')
        self.assertEqual(historical_record['ds_surf_batch_in'], 1)
        self.assertEqual(historical_record['ds_surf_batch_out'], 2)

        # Check live records
        live_records = self.getLiveRecords()
        self.assertEqual(len(live_records), 2)

        # Check id1 live record (william, newport from batch 3)
        id1_live = next((r for r in live_records if r['id'] == 'id1'), None)
        self.assertIsNotNone(id1_live)
        self.assertEqual(id1_live['firstName'], 'william')
        self.assertEqual(id1_live['ds_surf_batch_in'], 3)
        self.assertEqual(id1_live['ds_surf_batch_out'], 2147483647)

        # Check id2 live record (unchanged from batch 2)
        id2_live = next((r for r in live_records if r['id'] == 'id2'), None)
        self.assertIsNotNone(id2_live)
        self.assertEqual(id2_live['ds_surf_batch_in'], 2)
        self.assertEqual(id2_live['ds_surf_batch_out'], 2147483647)

        # Step 4: Batch 4 - Insert only <id2, laura, diaz> (id1 deleted)
        print("Step 4: Batch 4 - Insert only <id2, laura, diaz> (id1 deleted)")
        test_data = [
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        # Clear and re-insert
        with self.source_engine.begin() as conn:
            conn.execute(text('DELETE FROM people'))
        self.insertTestData(test_data)

        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 4, BatchStatus.COMMITTED)

        # Verify forensic history
        all_records = self.getMergeTableData()
        self.assertEqual(len(all_records), 3)  # 3 records total (1 live + 2 historical)

        # Check that id1 william record is now closed
        id1_william_historical = next((r for r in all_records if r['id'] == 'id1' and r['firstName'] == 'william'), None)
        self.assertIsNotNone(id1_william_historical)
        self.assertEqual(id1_william_historical['ds_surf_batch_in'], 3)
        self.assertEqual(id1_william_historical['ds_surf_batch_out'], 3)  # Closed in batch 4

        # Check live records (only id2 should be live)
        live_records = self.getLiveRecords()
        self.assertEqual(len(live_records), 1)
        self.assertEqual(live_records[0]['id'], 'id2')

        # Step 5: Batch 5 - Insert <id1, billy, newport>, <id2, laura, diaz>
        print("Step 5: Batch 5 - Insert <id1, billy, newport>, <id2, laura, diaz>")
        test_data = [
            {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        # Clear and re-insert
        with self.source_engine.begin() as conn:
            conn.execute(text('DELETE FROM people'))
        self.insertTestData(test_data)

        status = self.runJob()
        self.assertEqual(status, JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 5, BatchStatus.COMMITTED)

        # Verify final forensic history
        all_records = self.getMergeTableData()
        self.assertEqual(len(all_records), 4)  # 4 records total (2 live + 2 historical)

        # Check live records
        live_records = self.getLiveRecords()
        self.assertEqual(len(live_records), 2)

        # Check id1 live record (billy, newport from batch 5)
        id1_live = next((r for r in live_records if r['id'] == 'id1'), None)
        self.assertIsNotNone(id1_live)
        self.assertEqual(id1_live['firstName'], 'billy')
        self.assertEqual(id1_live['ds_surf_batch_in'], 5)
        self.assertEqual(id1_live['ds_surf_batch_out'], 2147483647)

        # Check id2 live record (unchanged from batch 2)
        id2_live = next((r for r in live_records if r['id'] == 'id2'), None)
        self.assertIsNotNone(id2_live)
        self.assertEqual(id2_live['ds_surf_batch_in'], 2)
        self.assertEqual(id2_live['ds_surf_batch_out'], 2147483647)

        print("All forensic merge scenario tests passed!")


if __name__ == "__main__":
    unittest.main()
