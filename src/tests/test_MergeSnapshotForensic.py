"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.platforms.yellow.jobs import JobStatus
from datasurface.platforms.yellow.yellow_dp import BatchStatus, YellowDataPlatform
from datasurface.platforms.yellow.yellow_dp import YellowMilestoneStrategy
from datasurface.md.governance import WorkspacePlatformConfig, DataMilestoningStrategy
from tests.test_MergeSnapshotLiveOnly import BaseSnapshotMergeJobTest
from typing import cast
from datasurface.platforms.yellow.merge_forensic import SnapshotMergeJobForensic
from datasurface.md import Ecosystem
from typing import Optional


class TestSnapshotMergeJobForensic(BaseSnapshotMergeJobTest, unittest.TestCase):
    """Test the SnapshotMergeJobForensic with forensic merge scenario"""

    def __init__(self, methodName: str = "runTest") -> None:
        eco: Optional[Ecosystem] = BaseSnapshotMergeJobTest.loadEcosystem("src/tests/yellow_dp_tests")
        dp: YellowDataPlatform = cast(YellowDataPlatform, eco.getDataPlatformOrThrow("Test_DP"))
        dp.milestoneStrategy = YellowMilestoneStrategy.SCD2

        # Set the consumer to forensic mode
        req: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, eco.cache_getWorkspaceOrThrow("Consumer1").workspace.dsgs["TestDSG"].platformMD)
        req.retention.milestoningStrategy = DataMilestoningStrategy.FORENSIC

        assert eco is not None
        BaseSnapshotMergeJobTest.__init__(self, eco, dp.name, "Store1")
        unittest.TestCase.__init__(self, methodName)

    def baseTearDown(self) -> None:
        return super().baseTearDown()

    def setUp(self) -> None:
        self.common_setup_job(SnapshotMergeJobForensic, self)

    def common_verify_forensic_history(self, expected_total: int, expected_historical: int, tc: unittest.TestCase) -> None:
        """Common pattern to verify forensic history"""
        all_records = self.getMergeTableData()
        tc.assertEqual(len(all_records), expected_total)

        historical_records = [r for r in all_records if r['ds_surf_batch_out'] != 2147483647]
        tc.assertEqual(len(historical_records), expected_historical)

    def tearDown(self) -> None:
        self.baseTearDown()

    def test_BatchState(self) -> None:
        self.common_test_BatchState(self)

    def test_first_batch_started(self) -> None:
        self.common_test_first_batch_started(self)

    def test_forensic_merge_scenario(self) -> None:
        """Test the forensic merge scenario as described in the example"""

        # Step 1: Batch 1 - Insert <id1, billy, newport>
        print("Step 1: Batch 1 - Insert <id1, billy, newport>")
        test_data = [
            {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None}
        ]
        self.insertTestData(test_data)

        self.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 1, BatchStatus.COMMITTED, self)

        # Verify merge table has one live record
        live_records = self.getLiveRecords()
        self.assertEqual(len(live_records), 1)
        self.common_verify_record_exists(live_records, "id1", {
            'firstName': 'billy',
            'lastName': 'newport',
            'ds_surf_batch_in': 1,
            'ds_surf_batch_out': 2147483647  # MaxInt
        }, self)

        # Step 2: Batch 2 - Insert <id1, billy, newport>, <id2, laura, diaz>
        print("Step 2: Batch 2 - Insert <id1, billy, newport>, <id2, laura, diaz>")
        test_data = [
            {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        self.common_clear_and_insert_data(test_data, self)

        self.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 2, BatchStatus.COMMITTED, self)

        # Verify merge table has two live records
        live_records = self.getLiveRecords()
        self.assertEqual(len(live_records), 2)

        # Check id1 record (should still be from batch 1, unchanged)
        self.common_verify_record_exists(live_records, "id1", {
            'ds_surf_batch_in': 1,
            'ds_surf_batch_out': 2147483647
        }, self)

        # Check id2 record (new from batch 2)
        self.common_verify_record_exists(live_records, "id2", {
            'ds_surf_batch_in': 2,
            'ds_surf_batch_out': 2147483647
        }, self)

        # Step 3: Batch 3 - Insert <id1, william, newport>, <id2, laura, diaz>
        print("Step 3: Batch 3 - Insert <id1, william, newport>, <id2, laura, diaz>")
        test_data = [
            {"id": "id1", "firstName": "william", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        self.common_clear_and_insert_data(test_data, self)

        self.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 3, BatchStatus.COMMITTED, self)

        # Verify forensic history
        self.common_verify_forensic_history(3, 1, self)  # 3 records total (2 live + 1 historical)

        # Check historical record (id1, billy, newport from batch 1)
        all_records = self.getMergeTableData()
        historical_records = [r for r in all_records if r['ds_surf_batch_out'] != 2147483647]
        historical_record = historical_records[0]
        self.common_verify_record_exists([historical_record], "id1", {
            'firstName': 'billy',
            'ds_surf_batch_in': 1,
            'ds_surf_batch_out': 2
        }, self)

        # Check live records
        live_records = self.getLiveRecords()
        self.assertEqual(len(live_records), 2)

        # Check id1 live record (william, newport from batch 3)
        self.common_verify_record_exists(live_records, "id1", {
            'firstName': 'william',
            'ds_surf_batch_in': 3,
            'ds_surf_batch_out': 2147483647
        }, self)

        # Check id2 live record (unchanged from batch 2)
        self.common_verify_record_exists(live_records, "id2", {
            'ds_surf_batch_in': 2,
            'ds_surf_batch_out': 2147483647
        }, self)

        # Step 4: Batch 4 - Insert only <id2, laura, diaz> (id1 deleted)
        print("Step 4: Batch 4 - Insert only <id2, laura, diaz> (id1 deleted)")
        test_data = [
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        self.common_clear_and_insert_data(test_data, self)

        self.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 4, BatchStatus.COMMITTED, self)

        # Verify forensic history
        self.common_verify_forensic_history(3, 2, self)  # 3 records total (1 live + 2 historical)

        # Check that id1 william record is now closed
        all_records = self.getMergeTableData()
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
        self.common_clear_and_insert_data(test_data, self)

        self.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store1", 5, BatchStatus.COMMITTED, self)

        # Verify final forensic history
        self.common_verify_forensic_history(4, 2, self)  # 4 records total (2 live + 2 historical)

        # Check live records
        live_records = self.getLiveRecords()
        self.assertEqual(len(live_records), 2)

        # Check id1 live record (billy, newport from batch 5)
        self.common_verify_record_exists(live_records, "id1", {
            'firstName': 'billy',
            'ds_surf_batch_in': 5,
            'ds_surf_batch_out': 2147483647
        }, self)

        # Check id2 live record (unchanged from batch 2)
        self.common_verify_record_exists(live_records, "id2", {
            'ds_surf_batch_in': 2,
            'ds_surf_batch_out': 2147483647
        }, self)

        self.assertEqual(self.job.numReconcileDDLs, 1)
        print("All forensic merge scenario tests passed!")


if __name__ == "__main__":
    unittest.main()
