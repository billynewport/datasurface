"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest

from typing import Optional
from sqlalchemy import text
from datasurface.platforms.yellow.jobs import JobStatus
from datasurface.platforms.yellow.yellow_dp import BatchStatus
from datasurface.md import Ecosystem
from datasurface.md.governance import DataMilestoningStrategy, WorkspacePlatformConfig
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform, YellowMilestoneStrategy
from typing import cast
from datasurface.platforms.yellow.merge_watermark import MergeWatermarkLive, MergeWatermarkForensic
from tests.test_MergeSnapshotLiveOnly import BaseMergeJobTest
from datasurface.md.lint import ValidationTree


class BaseWatermarkMergeJobTest(BaseMergeJobTest):

    def __init__(self, eco: Ecosystem, dpName: str, storeName: str = "Store1") -> None:
        super().__init__(eco, dpName, storeName)

    def common_clear_and_insert_data(self, test_data: list, tc: unittest.TestCase) -> None:
        """Common pattern to clear source and insert new test data"""
        with self.source_engine.begin() as conn:
            for dataset in self.store.datasets.values():
                conn.execute(text(f'DELETE FROM {dataset.name}'))
        self.insertTestData(test_data)

    def insertTestData(self, data: list[dict]) -> None:
        with self.source_engine.begin() as conn:
            for row in data:
                conn.execute(text("""
                    INSERT INTO people ("id", "firstName", "lastName", "dob", "employer", "dod", "last_update_time")
                    VALUES (:id, :firstName, :lastName, :dob, :employer, :dod, :last_update_time)
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
            # Check if this is a forensic platform (has batch milestoning)
            if self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
                # Forensic table - no ds_surf_batch_id column
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod", "last_update_time",
                           ds_surf_all_hash, ds_surf_key_hash,
                           ds_surf_batch_in, ds_surf_batch_out
                    FROM {self.ydu.getPhysMergeTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id", ds_surf_batch_in
                """))
            elif self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
                # Live-only table - has ds_surf_batch_id column
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod", "last_update_time",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
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
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod", "last_update_time",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM {self.ydu.getPhysMergeTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id"
                """))
            elif self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
                # Forensic schema: has batch_in/batch_out columns, filter for live records
                # Note: ds_surf_batch_id is not included in forensic tables
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod", "last_update_time",
                           ds_surf_all_hash, ds_surf_key_hash,
                           ds_surf_batch_in, ds_surf_batch_out
                    FROM {self.ydu.getPhysMergeTableNameForDataset(self.store.datasets["people"])}
                    WHERE ds_surf_batch_out = 2147483647
                    ORDER BY "id"
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
        if self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
            batch_merge_column = 'ds_surf_batch_id'
        else:
            batch_merge_column = 'ds_surf_batch_in'

        # Verify merge table is empty
        live_data = self.getMergeTableData()
        tc.assertEqual(len(live_data), 0)

        # Step 2: Have 5 rows, 4 older rows, 1 newer row. This should ingest the 4 older rows and not the newer row.
        print("Step 2: Inserting 4 older rows and running batch 2")
        test_data = [
            {"id": "1", "firstName": "John", "lastName": "Doe", "dob": "1980-01-01",
             "employer": "Company A", "dod": None, "last_update_time": "2024-12-31 23:58:00"},
            {"id": "2", "firstName": "Jane", "lastName": "Smith", "dob": "1985-02-15",
             "employer": "Company B", "dod": None, "last_update_time": "2024-12-31 23:59:00"},
            {"id": "3", "firstName": "Bob", "lastName": "Johnson", "dob": "1975-03-20",
             "employer": "Company C", "dod": None, "last_update_time": "2025-01-01 00:00:00"},
            {"id": "4", "firstName": "Alice", "lastName": "Brown", "dob": "1990-04-10",
             "employer": "Company D", "dod": None, "last_update_time": "2025-01-01 00:01:00"},
            {"id": "5", "firstName": "Charlie", "lastName": "Wilson", "dob": "1982-05-25",
             "employer": "Company E", "dod": None, "last_update_time": "2025-01-01 00:02:00"}
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

        # Verify 4 rows are in merge table with batch_id = 2 (records with watermark < "2025-01-01 00:02:00")
        # Records 1,2,3,4 should be ingested, but NOT record 5 (which has watermark = "2025-01-01 00:02:00")
        live_data = self.getLiveRecords()
        tc.assertEqual(len(live_data), 4)
        for row in live_data:
            tc.assertEqual(row[batch_merge_column], 2)

        # Verify specific records are present
        ingested_ids = {row['id'] for row in live_data}
        tc.assertEqual(ingested_ids, {'1', '2', '3', '4'})

        # Step 3: Update a #1 and add the #5, then run batch 3
        # #5 is ingested now because the update to #1 has watermark "2025-01-01 00:03:00" which is >= "2025-01-01 00:02:00"
        print("Step 3: Updating row 1 and running batch 3")
        self.updateTestData("1", {"employer": "Company X", "firstName": "Johnny", "last_update_time": "2025-01-01 00:03:00"})

        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 3, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 3, tc)

        # We now have 5 lives but don't have the updated record 1 yet.
        live_data = self.getLiveRecords()
        tc.assertEqual(len(live_data), 5)

        # Check that record 5 was ingested in batch 3
        self.common_verify_record_exists(live_data, "5", {batch_merge_column: 3}, tc)

        # Record 1 should still have its original data from batch 2 (not updated yet)
        self.common_verify_record_exists(live_data, "1", {
            batch_merge_column: 2,
            'firstName': 'John',  # Original name, not updated
            'employer': 'Company A'  # Original employer
        }, tc)

        # Step 4: Run batch 4 to ingest the updated record 1
        # Update record 2 which is ignored because its the high watermark
        self.updateTestData("2", {
                                    "id": "2", "firstName": "Jane", "lastName": "Doe", "dob": "1985-02-15",
                                    "employer": "Company B", "dod": None, "last_update_time": "2025-01-01 00:04:00"})
        print("Step 4: Running batch 4 to ingest updated record 1")
        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 4, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 4, tc)
        live_data = self.getLiveRecords()
        # Check that record 1 was updated in batch 4
        self.common_verify_record_exists(live_data, "1", {batch_merge_column: 4}, tc)

        # Verify updated record 1 is now ingested with batch_id = 4
        # Batch 4 should ingest records with watermark >= "2025-01-01 00:02:00" AND < "2025-01-01 00:03:00"
        # Wait, this is wrong. The updated record 1 has watermark "2025-01-01 00:03:00" which equals the high watermark
        # So it won't be ingested until there's a higher watermark. Let me add a new record first.
        live_data = self.getLiveRecords()
        tc.assertEqual(len(live_data), 5)  # Still 5 records, updated record 1 not ingested yet

        # Step 5: Add a new record with higher watermark and run batch 5
        print("Step 5: Adding a new record with higher watermark and running batch 5")
        self.insertTestData([{"id": "6", "firstName": "Bobby", "lastName": "Johnson", "dob": "1975-03-20",
                              "employer": "Company F", "dod": None, "last_update_time": "2025-01-01 00:05:00"}])

        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 5, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 5, tc)

        # Now batch 5 should ingest records with watermark >= "2025-01-01 00:03:00" AND < "2025-01-01 00:04:00"
        # This includes the updated record 1 with watermark "2025-01-01 00:03:00"
        live_data = self.getLiveRecords()
        tc.assertEqual(len(live_data), 5)  # Now we have 5 live records total (5 previous)

        # Check that updated record 2 is now present with new data
        self.common_verify_record_exists(live_data, "2", {
            batch_merge_column: 5,
            'lastName': 'Doe',  # Updated name
            'employer': 'Company B'  # Updated employer
        }, tc)

        # Record 6 should NOT be ingested yet because it has watermark "2025-01-01 00:04:00" which equals the high watermark
        ingested_ids = {row['id'] for row in live_data}
        tc.assertNotIn('6', ingested_ids)

        # Step 6: Add another record to trigger ingestion of record 6, then run batch 6, id 7 is not ingested
        print("Step 6: Adding another record to trigger ingestion of record 6")
        self.insertTestData([{"id": "7", "firstName": "David", "lastName": "Wilson", "dob": "1988-06-15",
                              "employer": "Company G", "dod": None, "last_update_time": "2025-01-01 00:06:00"}])

        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 6, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 6, tc)

        # Now batch 6 should ingest records with watermark >= "2025-01-01 00:04:00" AND < "2025-01-01 00:05:00"
        # This includes record 6 with watermark "2025-01-01 00:04:00"
        live_data = self.getLiveRecords()
        tc.assertEqual(len(live_data), 6)  # Now we have 6 records total

        # Check that record 6 is now present
        self.common_verify_record_exists(live_data, "6", {batch_merge_column: 6}, tc)

        # Step 7: Run another batch with no changes
        print("Step 7: Running batch 7 with no changes")
        tc.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus(self.store.name, 7, BatchStatus.COMMITTED, tc)
        self.checkCurrentBatchIs(self.store.name, 7, tc)

        # Verify no changes occurred - all rows should keep their previous batch_ids
        live_data_after = self.getLiveRecords()
        tc.assertEqual(len(live_data_after), 6)

        if self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
            # Look at merge data
            merge_data = self.getMergeTableData()
            tc.assertEqual(len(merge_data), 8)
            # Check there for id 1 and 2, there are 2 rows for each, all others should be 1 row
            countMap: dict[str, int] = {}
            for row in merge_data:
                countMap[row["id"]] = countMap.get(row["id"], 0) + 1

            tc.assertEqual(countMap["1"], 2)
            tc.assertEqual(countMap["2"], 2)
            tc.assertEqual(countMap["3"], 1)
            tc.assertEqual(countMap["4"], 1)
            tc.assertEqual(countMap["5"], 1)
            tc.assertEqual(countMap["6"], 1)

        print("All batch lifecycle tests passed!")


class TestWatermarkMergeJob(BaseWatermarkMergeJobTest, unittest.TestCase):
    """Test the SnapshotMergeJob with a simple ecosystem (live-only)"""

    def __init__(self, methodName: str = "runTest") -> None:
        eco: Optional[Ecosystem] = BaseWatermarkMergeJobTest.loadEcosystem("src/tests/yellow_dp_tests")
        assert eco is not None
        dp: YellowDataPlatform = cast(YellowDataPlatform, eco.getDataPlatformOrThrow("Test_DP"))
        dp.milestoneStrategy = YellowMilestoneStrategy.SCD1

        # Set the consumer to live-only mode
        req: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, eco.cache_getWorkspaceOrThrow("Consumer1").workspace.dsgs["TestDSG"].platformMD)
        req.retention.milestoningStrategy = DataMilestoningStrategy.LIVE_ONLY
        tree: ValidationTree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())
        print(tree.getErrorsAsStructuredData())
        BaseWatermarkMergeJobTest.__init__(self, eco, dp.name, "Store2")
        unittest.TestCase.__init__(self, methodName)

    def setUp(self) -> None:
        self.common_setup_job(MergeWatermarkLive, self)

    def tearDown(self) -> None:
        self.baseTearDown()

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
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod", "last_update_time",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM {self.ydu.getPhysStagingTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id"
                """))
            except Exception:
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod", "last_update_time",
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
             "employer": "Company A", "dod": None, "last_update_time": "2025-01-01 00:00:00"},
            {"id": "2", "firstName": "Jane", "lastName": "Smith", "dob": "1985-02-15",
             "employer": "Company B", "dod": None, "last_update_time": "2025-01-01 00:00:00"}
        ]
        self.insertTestData(test_data)

        # Run the job until completion (should commit the batch)
        self.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store2", 1, BatchStatus.COMMITTED, self)

        # Step 2: Try to reset the committed batch - this should fail
        assert self.eco is not None
        assert self.dp is not None
        result = self.dp.resetBatchState(self.eco, "Store2")

        # Check that the reset failed with the expected error message
        self.assertEqual(result, "ERROR: Batch is COMMITTED and cannot be reset")

        # Verify batch status is still COMMITTED (unchanged)
        self.checkSpecificBatchStatus("Store2", 1, BatchStatus.COMMITTED, self)

    def test_reset_nonexistent_datastore_fails(self) -> None:
        """Test that trying to reset a non-existent datastore fails"""
        assert self.eco is not None
        assert self.dp is not None

        # Try to reset a datastore that doesn't exist
        result = self.dp.resetBatchState(self.eco, "NonExistentStore")

        # Check that the reset failed with the expected error message
        self.assertEqual(result, "ERROR: Could not find datastore in ecosystem")


class TestWatermarkMergeJobForensic(BaseWatermarkMergeJobTest, unittest.TestCase):
    """Test the SnapshotMergeJob with a simple ecosystem (forensic)"""

    def __init__(self, methodName: str = "runTest") -> None:
        eco: Optional[Ecosystem] = BaseWatermarkMergeJobTest.loadEcosystem("src/tests/yellow_dp_tests")
        assert eco is not None
        dp: YellowDataPlatform = cast(YellowDataPlatform, eco.getDataPlatformOrThrow("Test_DP"))
        dp.milestoneStrategy = YellowMilestoneStrategy.SCD2

        # Set the consumer to live-only mode
        req: WorkspacePlatformConfig = cast(WorkspacePlatformConfig, eco.cache_getWorkspaceOrThrow("Consumer1").workspace.dsgs["TestDSG"].platformMD)
        req.retention.milestoningStrategy = DataMilestoningStrategy.FORENSIC
        tree: ValidationTree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())
        print(tree.getErrorsAsStructuredData())
        BaseWatermarkMergeJobTest.__init__(self, eco, dp.name, "Store2")
        unittest.TestCase.__init__(self, methodName)

    def setUp(self) -> None:
        self.common_setup_job(MergeWatermarkForensic, self)

    def tearDown(self) -> None:
        self.baseTearDown()

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
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod", "last_update_time",
                           ds_surf_batch_id, ds_surf_all_hash, ds_surf_key_hash
                    FROM {self.ydu.getPhysStagingTableNameForDataset(self.store.datasets["people"])}
                    ORDER BY "id"
                """))
            except Exception:
                result = conn.execute(text(f"""
                    SELECT "id", "firstName", "lastName", "dob", "employer", "dod", "last_update_time",
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
             "employer": "Company A", "dod": None, "last_update_time": "2025-01-01 00:00:00"},
            {"id": "2", "firstName": "Jane", "lastName": "Smith", "dob": "1985-02-15",
             "employer": "Company B", "dod": None, "last_update_time": "2025-01-01 00:00:00"}
        ]
        self.insertTestData(test_data)

        # Run the job until completion (should commit the batch)
        self.assertEqual(self.runJob(), JobStatus.DONE)
        self.checkSpecificBatchStatus("Store2", 1, BatchStatus.COMMITTED, self)

        # Step 2: Try to reset the committed batch - this should fail
        assert self.eco is not None
        assert self.dp is not None
        result = self.dp.resetBatchState(self.eco, "Store2")

        # Check that the reset failed with the expected error message
        self.assertEqual(result, "ERROR: Batch is COMMITTED and cannot be reset")

        # Verify batch status is still COMMITTED (unchanged)
        self.checkSpecificBatchStatus("Store2", 1, BatchStatus.COMMITTED, self)

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
