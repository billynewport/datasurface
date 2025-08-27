"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
import os
import pytest
from datasurface.platforms.yellow.jobs import JobStatus
from datasurface.platforms.yellow.merge_remote_forensic import SnapshotMergeJobRemoteForensic
from datasurface.platforms.yellow.merge_forensic import SnapshotMergeJobForensic
from datasurface.platforms.yellow.yellow_dp import BatchStatus
from tests.test_MergeSnapshotLiveOnly import BaseSnapshotMergeJobTest
from typing import cast
from datasurface.platforms.yellow.merge_remote_live import SnapshotMergeJobRemoteLive
from datasurface.md import Ecosystem
from datasurface.md import ValidationTree
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform, YellowMilestoneStrategy
from typing import Any, Optional, Type
from datetime import datetime
from datasurface.platforms.yellow.jobs import Job
from datetime import date

# Check if SQL Server tests should be enabled
ENABLE_SQLSERVER_TESTS = os.getenv("ENABLE_SQLSERVER_TESTS", "false").lower() == "true"
skip_sqlserver = pytest.mark.skipif(
    not ENABLE_SQLSERVER_TESTS,
    reason="SQL Server tests disabled. Set ENABLE_SQLSERVER_TESTS=true to enable."
)
# Check if Oracle tests should be enabled
ENABLE_ORACLE_TESTS = os.getenv("ENABLE_ORACLE_TESTS", "false").lower() == "true"
skip_oracle = pytest.mark.skipif(
    not ENABLE_ORACLE_TESTS,
    reason="Oracle tests disabled. Set ENABLE_ORACLE_TESTS=true to enable."
)
# Check if DB2 tests should be enabled
ENABLE_DB2_TESTS = os.getenv("ENABLE_DB2_TESTS", "false").lower() == "true"
skip_db2 = pytest.mark.skipif(
    not ENABLE_DB2_TESTS,
    reason="DB2 tests disabled. Set ENABLE_DB2_TESTS=true to enable."
)
# Check if Snowflake tests should be enabled
ENABLE_SNOWFLAKE_TESTS = os.getenv("ENABLE_SNOWFLAKE_TESTS", "false").lower() == "true"
skip_snowflake = pytest.mark.skipif(
    not ENABLE_SNOWFLAKE_TESTS,
    reason="Snowflake tests disabled. Set ENABLE_SNOWFLAKE_TESTS=true to enable."
)


class TestMergeRemoteLive(unittest.TestCase):

    """In the ecosystem which is configured, Store1 is a postgres source and Store2 is a SQLServer source. Store3 is an Oracle source."""

    def checkTestRecordsMatchExpected(self, test_data: list[dict[str, Any]], live_records: list[Any]) -> None:
        # Adapter test_data to a dict on key so we can find the record quickly.
        test_data_dict: dict[str, Any] = {record["id"]: record for record in test_data}
        for record in live_records:
            id: str = record["id"]
            if id not in test_data_dict:
                self.fail(f"Record {record} not found in test data")
            test_record: dict[str, Any] = test_data_dict[id]
            for key, value in test_record.items():
                if key in ['dob', 'dod'] and value is not None:
                    # Handle both string dates and date objects in test data
                    if isinstance(value, str):
                        expected_date = datetime.strptime(value, '%Y-%m-%d').date()
                    elif isinstance(value, datetime):
                        expected_date = value.date()
                    else:
                        expected_date = value  # Already a date object

                    # Handle database record value - Oracle returns datetime objects for DATE columns
                    actual_value = record[key]
                    if isinstance(actual_value, datetime):
                        actual_date = actual_value.date()
                    else:
                        actual_date = actual_value  # Already a date object

                    self.assertEqual(actual_date, expected_date, f"Record {record} does not match test data {test_record}")
                else:
                    self.assertEqual(record[key], value, f"Record {record} does not match test data {test_record}")

    def checkForensicTablesMatch(self, primary_data: list[Any], remote_data: list[Any], batch_id: int) -> None:
        """Check that the remote forensic table exactly matches the primary forensic table.

        This performs a detailed record-by-record comparison to ensure the remote table
        is an exact mirror of the primary table, including all forensic metadata.
        """
        # Sort both datasets by a consistent key for comparison
        # Use a composite key: (id, ds_surf_batch_in) to handle multiple versions of the same record
        def sort_key(record: Any) -> tuple[str, int]:
            return (record["id"], record["ds_surf_batch_in"])

        primary_sorted = sorted(primary_data, key=sort_key)
        remote_sorted = sorted(remote_data, key=sort_key)

        # Compare record by record
        for i, (primary_record, remote_record) in enumerate(zip(primary_sorted, remote_sorted)):
            # Compare all fields - ds_surf_batch_id is no longer present in forensic merge tables
            for key in primary_record.keys():
                primary_value = primary_record[key]
                remote_value = remote_record[key]
                if key in ['dob', 'dod'] and remote_value is not None:
                    # Make sure any datetime is converted to a date object for comparison
                    if isinstance(remote_value, datetime):
                        remote_value = remote_value.date()
                    if isinstance(primary_value, datetime):
                        primary_value = primary_value.date()

                self.assertEqual(
                    remote_value,
                    primary_value,
                    f"Batch {batch_id}, Record {i}: Field '{key}' mismatch. "
                    f"Primary: {primary_value}, Remote: {remote_value}. "
                    f"Primary record: {primary_record}, Remote record: {remote_record}"
                )

    def setup_stream_test(self, platformName: str, storeName: str, job_class: Type[Job]) -> BaseSnapshotMergeJobTest:
        eco: Optional[Ecosystem]
        tree: Optional[ValidationTree]
        eco, tree = loadEcosystemFromEcoModule("src/tests/pip_test_model")
        assert eco is not None
        assert tree is not None
        assert not tree.hasErrors()
        dp: YellowDataPlatform = cast(YellowDataPlatform, eco.getDataPlatformOrThrow(platformName))
        assert dp is not None
        # Choose the correct store based on platform type
        utils: BaseSnapshotMergeJobTest = BaseSnapshotMergeJobTest(eco, platformName, storeName)
        assert utils.store is not None
        utils.store.cmd = dp.getEffectiveCMDForDatastore(eco, utils.store)
        utils.common_setup_job(job_class, self)
        return utils

    def setup_live_and_merge_batch_runs(self, storeName: str) -> tuple[BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest]:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLive", storeName, SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensic", storeName, SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensic", storeName, SnapshotMergeJobRemoteForensic)
        return live_utils, merge_utils, remote_merge_utils

    def setup_live_and_merge_batch_runsSQLServer(self, storeName: str) -> tuple[BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest]:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveSQLServer", storeName, SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSQLServer", storeName, SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicSQLServer", storeName, SnapshotMergeJobRemoteForensic)
        return live_utils, merge_utils, remote_merge_utils

    def setup_live_and_merge_batch_runsOracle(self, storeName: str) -> tuple[BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest]:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveOracle", storeName, SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicOracle", storeName, SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicOracle", storeName, SnapshotMergeJobRemoteForensic)
        return live_utils, merge_utils, remote_merge_utils

    def setup_live_and_merge_batch_runsDB2(self, storeName: str) -> tuple[BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest]:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveDB2", storeName, SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicDB2", storeName, SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicDB2", storeName, SnapshotMergeJobRemoteForensic)
        return live_utils, merge_utils, remote_merge_utils

    def setup_live_and_merge_batch_runsSnowflake(self, storeName: str) -> tuple[BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest, BaseSnapshotMergeJobTest]:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveSnowflake", storeName, SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSnowflake", storeName, SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicSnowflake", storeName, SnapshotMergeJobRemoteForensic)
        return live_utils, merge_utils, remote_merge_utils

    def getBatchTestData(self) -> list[list[dict[str, Any]]]:
        batch_test_data: list[list[dict[str, Any]]] = [
            [  # Batch 1
                {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": date.fromisoformat("1980-01-01"), "employer": "Company A", "dod": None}
            ],
            [  # Batch 2
                {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": date.fromisoformat("1980-01-01"), "employer": "Company A", "dod": None},
                {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": date.fromisoformat("1985-02-15"), "employer": "Company B", "dod": None}
            ],
            [  # Batch 3
                {"id": "id1", "firstName": "william", "lastName": "newport", "dob": date.fromisoformat("1980-01-01"), "employer": "Company A", "dod": None},
                {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": date.fromisoformat("1985-02-15"), "employer": "Company B", "dod": None}
            ],
            [  # Batch 4
                {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": date.fromisoformat("1985-02-15"), "employer": "Company B", "dod": None}
            ],
            [  # Batch 5
                {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": date.fromisoformat("1980-01-01"), "employer": "Company A", "dod": None},
                {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": date.fromisoformat("1985-02-15"), "employer": "Company B", "dod": None}
            ]
        ]
        return batch_test_data

    def compare_forensic_merge_tables(self, test_utils: BaseSnapshotMergeJobTest, pip_utils: BaseSnapshotMergeJobTest,
                                      batch_id: int, batch_data: list[dict[str, Any]]) -> None:
        if test_utils.dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
            # Remote forensic merge: compare remote merge table with primary merge table
            remote_merge_data: list[Any] = test_utils.getMergeTableData()
            primary_merge_data: list[Any] = pip_utils.getMergeTableData()
            msg = (
                f"Remote merge table mismatch. Expected {len(primary_merge_data)} records but got "
                f"{len(remote_merge_data)} for batch {batch_id}"
            )
            self.assertEqual(len(remote_merge_data), len(primary_merge_data), msg)
            # For forensic merge, compare remote table against primary table, not input data
            self.checkForensicTablesMatch(primary_merge_data, remote_merge_data, batch_id)
        elif test_utils.dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
            # Regular forensic merge: compare live records against input data
            live_records: list[Any] = test_utils.getLiveRecords()
            msg = (
                f"Live records count mismatch. Expected {len(batch_data)} but got "
                f"{len(live_records)} for batch {batch_id}"
            )
            self.assertEqual(len(live_records), len(batch_data), msg)
            self.checkTestRecordsMatchExpected(batch_data, live_records)
        else:
            raise Exception(f"Unknown milestone strategy: {test_utils.dp.milestoneStrategy}")

    # This ingests a primary stream using pip_utils and then ingests batches from there to the test_utils.
    def generic_test_5_batches_one_by_one(self, test_utils: BaseSnapshotMergeJobTest, pip_utils: BaseSnapshotMergeJobTest) -> None:
        pip_utils.common_clear_and_insert_data([], self)

        # First, lets do a 5 batch ingestion/test on YellowForensic.
        batch_test_data: list[list[dict[str, Any]]] = self.getBatchTestData()

        batch_id: int = 1
        for batch_data in batch_test_data:
            print(f"Inserting batch {batch_id} data: {batch_data}")
            pip_utils.common_clear_and_insert_data(batch_data, self)

            self.assertEqual(pip_utils.runJob(), JobStatus.DONE)
            pip_utils.checkSpecificBatchStatus(pip_utils.storeName, batch_id, BatchStatus.COMMITTED, self)
            pip_utils.checkCurrentBatchIs(pip_utils.storeName, batch_id, self)
            # print merge table data for debugging
            merge_table_data: list[Any] = pip_utils.getMergeTableData()
            print(f"Merge table data for batch {batch_id}: {merge_table_data}")
            # Try to ingest from the merge batch idx to the live platform.
            self.assertEqual(test_utils.runJob(), JobStatus.DONE)
            test_utils.checkSpecificBatchStatus(test_utils.storeName, batch_id, BatchStatus.COMMITTED, self)

            # For remote forensic merge, compare the remote table against the primary table
            assert test_utils.dp is not None
            assert isinstance(test_utils.dp, YellowDataPlatform)
            self.compare_forensic_merge_tables(test_utils, pip_utils, batch_id, batch_data)
            batch_id += 1

        self.assertEqual(test_utils.job.numReconcileDDLs, 1)
        self.assertEqual(pip_utils.job.numReconcileDDLs, 1)

        test_utils.baseTearDown()
        pip_utils.baseTearDown()

    def test_5_batches_remote_forensic_one_by_one(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensic", "Store1", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensic", "Store1", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_one_by_one(remote_merge_utils, merge_utils)

    @skip_sqlserver
    def test_5_batches_remote_forensic_one_by_one_SQLServer(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSQLServer", "Store2", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicSQLServer", "Store2", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_one_by_one(remote_merge_utils, merge_utils)

    @skip_oracle
    def test_5_batches_remote_forensic_one_by_one_Oracle(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicOracle", "Store3", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicOracle", "Store3", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_one_by_one(remote_merge_utils, merge_utils)

    @skip_db2
    def test_5_batches_remote_forensic_one_by_one_DB2(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicDB2", "Store4", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicDB2", "Store4", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_one_by_one(remote_merge_utils, merge_utils)

    @skip_snowflake
    def test_5_batches_remote_forensic_one_by_one_Snowflake(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSnowflake", "Store5", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicSnowflake", "Store5", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_one_by_one(remote_merge_utils, merge_utils)

    def test_5_batches_remote_live_one_by_one(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLive", "Store1", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensic", "Store1", SnapshotMergeJobForensic)

        self.generic_test_5_batches_one_by_one(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    @skip_sqlserver
    def test_5_batches_remote_live_one_by_one_SQLServer(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveSQLServer", "Store2", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSQLServer", "Store2", SnapshotMergeJobForensic)

        self.generic_test_5_batches_one_by_one(live_utils, merge_utils)

        self.assertEqual(live_utils.job.numReconcileDDLs, 1)
        self.assertEqual(merge_utils.job.numReconcileDDLs, 1)
        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    @skip_oracle
    def test_5_batches_remote_live_one_by_one_Oracle(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveOracle", "Store3", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicOracle", "Store3", SnapshotMergeJobForensic)

        self.generic_test_5_batches_one_by_one(live_utils, merge_utils)

        self.assertEqual(live_utils.job.numReconcileDDLs, 1)
        self.assertEqual(merge_utils.job.numReconcileDDLs, 1)
        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    @skip_db2
    def test_5_batches_remote_live_one_by_one_DB2(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveDB2", "Store4", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicDB2", "Store4", SnapshotMergeJobForensic)

        self.generic_test_5_batches_one_by_one(live_utils, merge_utils)

        self.assertEqual(live_utils.job.numReconcileDDLs, 1)
        self.assertEqual(merge_utils.job.numReconcileDDLs, 1)
        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    @skip_snowflake
    def test_5_batches_remote_live_one_by_one_Snowflake(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveSnowflake", "Store5", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSnowflake", "Store5", SnapshotMergeJobForensic)

        self.generic_test_5_batches_one_by_one(live_utils, merge_utils)

        self.assertEqual(live_utils.job.numReconcileDDLs, 1)
        self.assertEqual(merge_utils.job.numReconcileDDLs, 1)
        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    # Seed at batch 3, then incrementally ingest the rest.
    def test_5_batches_remote_live_b3_then_one_by_one(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLive", "Store1", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensic", "Store1", SnapshotMergeJobForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    @skip_sqlserver
    def test_5_batches_remote_live_b3_then_one_by_one_SQLServer(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveSQLServer", "Store2", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSQLServer", "Store2", SnapshotMergeJobForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    @skip_oracle
    def test_5_batches_remote_live_b3_then_one_by_one_Oracle(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveOracle", "Store3", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicOracle", "Store3", SnapshotMergeJobForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    @skip_db2
    def test_5_batches_remote_live_b3_then_one_by_one_DB2(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveDB2", "Store4", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicDB2", "Store4", SnapshotMergeJobForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    @skip_snowflake
    def test_5_batches_remote_live_b3_then_one_by_one_Snowflake(self) -> None:
        live_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowLiveSnowflake", "Store5", SnapshotMergeJobRemoteLive)
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSnowflake", "Store5", SnapshotMergeJobForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    def test_5_batches_remote_forensic_b3_then_one_by_one(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensic", "Store1", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensic", "Store1", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(remote_merge_utils, merge_utils)

        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_sqlserver
    def test_5_batches_remote_forensic_b3_then_one_by_one_SQLServer(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSQLServer", "Store2", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicSQLServer", "Store2", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(remote_merge_utils, merge_utils)

        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_oracle
    def test_5_batches_remote_forensic_b3_then_one_by_one_Oracle(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicOracle", "Store3", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicOracle", "Store3", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(remote_merge_utils, merge_utils)

        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_db2
    def test_5_batches_remote_forensic_b3_then_one_by_one_DB2(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicDB2", "Store4", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicDB2", "Store4", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(remote_merge_utils, merge_utils)

        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_snowflake
    def test_5_batches_remote_forensic_b3_then_one_by_one_Snowflake(self) -> None:
        merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowForensicSnowflake", "Store5", SnapshotMergeJobForensic)
        remote_merge_utils: BaseSnapshotMergeJobTest = self.setup_stream_test("YellowRemoteForensicSnowflake", "Store5", SnapshotMergeJobRemoteForensic)

        self.generic_test_5_batches_remote_live_b3_then_one_by_one(remote_merge_utils, merge_utils)

        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    def generic_test_5_batches_remote_live_b3_then_one_by_one(self, test_utils: BaseSnapshotMergeJobTest, pip_utils: BaseSnapshotMergeJobTest) -> None:
        pip_utils.common_clear_and_insert_data([], self)

        # First, lets do a 5 batch ingestion/test on YellowForensic.
        batch_test_data: list[list[dict[str, Any]]] = self.getBatchTestData()

        batch_id: int = 1
        # This is seed the live table with batches 1-3, then do 4 and then 5
        for batch_data in batch_test_data:
            print(f"Inserting batch {batch_id} data: {batch_data}")
            pip_utils.common_clear_and_insert_data(batch_data, self)

            self.assertEqual(pip_utils.runJob(), JobStatus.DONE)
            pip_utils.checkSpecificBatchStatus(pip_utils.storeName, batch_id, BatchStatus.COMMITTED, self)
            pip_utils.checkCurrentBatchIs(pip_utils.storeName, batch_id, self)
            # print merge table data for debugging
            if batch_id >= 3:
                merge_table_data: list[Any] = pip_utils.getMergeTableData()
                print(f"Merge table data for batch {batch_id}: {merge_table_data}")
                # Try to ingest from the merge batch idx to the live platform.
                self.assertEqual(test_utils.runJob(), JobStatus.DONE)
                # For remote live merge, determine the correct local batch ID
                if test_utils.dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
                    # Remote live merge creates its own local batch sequence starting from 1
                    # The first time it runs (at batch 3), it creates local batch 1
                    local_batch_id: int = 1 if batch_id == 3 else (batch_id - 2)  # Adjust for starting at batch 3
                else:
                    # Remote forensic merge uses the primary batch ID as local batch ID
                    local_batch_id = batch_id
                test_utils.checkSpecificBatchStatus(test_utils.storeName, local_batch_id, BatchStatus.COMMITTED, self)
                self.compare_forensic_merge_tables(test_utils, pip_utils, local_batch_id, batch_data)
            batch_id += 1

        test_utils.baseTearDown()
        pip_utils.baseTearDown()

    def generic_test_5_batches_remote_live_b3_then_two(self, test_utils: BaseSnapshotMergeJobTest, pip_utils: BaseSnapshotMergeJobTest) -> None:
        pip_utils.common_clear_and_insert_data([], self)

        # First, lets do a 5 batch ingestion/test on YellowForensic.
        batch_test_data: list[list[dict[str, Any]]] = self.getBatchTestData()
        batch_id: int = 1
        # This is seed the table with batches 1-3, then do 4 and then 5
        for batch_data in batch_test_data:
            print(f"Inserting batch {batch_id} data: {batch_data}")
            pip_utils.common_clear_and_insert_data(batch_data, self)

            self.assertEqual(pip_utils.runJob(), JobStatus.DONE)
            pip_utils.checkSpecificBatchStatus(pip_utils.storeName, batch_id, BatchStatus.COMMITTED, self)
            pip_utils.checkCurrentBatchIs(pip_utils.storeName, batch_id, self)
            # print merge table data for debugging
            if batch_id == 3 or batch_id == 5:
                merge_table_data: list[Any] = pip_utils.getMergeTableData()
                print(f"Merge table data for batch {batch_id}: {merge_table_data}")
                # Try to ingest from the merge batch idx to the remote platform.
                self.assertEqual(test_utils.runJob(), JobStatus.DONE)
                # For remote live merge, determine the correct local batch ID
                if test_utils.dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
                    # Remote live merge creates its own local batch sequence starting from 1
                    # The local batch ID corresponds to the number of remote merge jobs run
                    local_batch_id: int = 1 if batch_id == 3 else 2
                    test_utils.checkSpecificBatchStatus(test_utils.storeName, local_batch_id, BatchStatus.COMMITTED, self)
                    live_records: list[Any] = test_utils.getLiveRecords()
                    msg = (
                        f"Live records count mismatch. Expected {len(batch_data)} but got "
                        f"{len(live_records)} for batch {local_batch_id}"
                    )
                    self.assertEqual(len(live_records), len(batch_data), msg)
                    self.checkTestRecordsMatchExpected(batch_data, live_records)
                else:
                    # For remote forensic merge, the batch ID is the same as the primary batch ID (remote batch ID)
                    local_batch_id: int = batch_id  # Remote forensic uses primary batch ID as local batch ID
                    test_utils.checkSpecificBatchStatus(test_utils.storeName, local_batch_id, BatchStatus.COMMITTED, self)
                    self.compare_forensic_merge_tables(test_utils, pip_utils, local_batch_id, batch_data)
            batch_id += 1

    # Seed at batch 3, then ingest batch 4 and 5 as one batch.
    def test_5_batches_remote_live_b3_then_two(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runs("Store1")

        self.generic_test_5_batches_remote_live_b3_then_two(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    def test_5_batches_remote_forensic_b3_then_two(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runs("Store1")

        self.generic_test_5_batches_remote_live_b3_then_two(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_sqlserver
    def test_5_batches_remote_live_b3_then_two_SQLServer(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsSQLServer("Store2")

        self.generic_test_5_batches_remote_live_b3_then_two(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_sqlserver
    def test_5_batches_remote_forensic_b3_then_two_SQLServer(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsSQLServer("Store2")

        self.generic_test_5_batches_remote_live_b3_then_two(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_oracle
    def test_5_batches_remote_live_b3_then_two_Oracle(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsOracle("Store3")

        self.generic_test_5_batches_remote_live_b3_then_two(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_oracle
    def test_5_batches_remote_forensic_b3_then_two_Oracle(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsOracle("Store3")

        self.generic_test_5_batches_remote_live_b3_then_two(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_db2
    def test_5_batches_remote_live_b3_then_two_DB2(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsDB2("Store4")

        self.generic_test_5_batches_remote_live_b3_then_two(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_db2
    def test_5_batches_remote_forensic_b3_then_two_DB2(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsDB2("Store4")

        self.generic_test_5_batches_remote_live_b3_then_two(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_snowflake
    def test_5_batches_remote_live_b3_then_two_Snowflake(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsSnowflake("Store5")

        self.generic_test_5_batches_remote_live_b3_then_two(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_snowflake
    def test_5_batches_remote_forensic_b3_then_two_Snowflake(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsSnowflake("Store5")

        self.generic_test_5_batches_remote_live_b3_then_two(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    def generic_test_5_batches_remote_live_b5(self, test_utils: BaseSnapshotMergeJobTest, pip_utils: BaseSnapshotMergeJobTest) -> None:
        pip_utils.common_clear_and_insert_data([], self)

        # First, lets do a 5 batch ingestion/test on YellowForensic.
        batch_test_data: list[list[dict[str, Any]]] = self.getBatchTestData()
        batch_id: int = 1
        # This is seed the live table with batches 1-3, then do 4 and then 5
        for batch_data in batch_test_data:
            print(f"Inserting batch {batch_id} data: {batch_data}")
            pip_utils.common_clear_and_insert_data(batch_data, self)

            self.assertEqual(pip_utils.runJob(), JobStatus.DONE)
            pip_utils.checkSpecificBatchStatus(pip_utils.storeName, batch_id, BatchStatus.COMMITTED, self)
            pip_utils.checkCurrentBatchIs(pip_utils.storeName, batch_id, self)
            # print merge table data for debugging
            batch_id += 1

        batch_id = 5
        batch_data = batch_test_data[batch_id - 1]
        merge_table_data: list[Any] = pip_utils.getMergeTableData()
        print(f"Merge table data for batch {batch_id}: {merge_table_data}")
        # Try to ingest from the merge batch idx to the remote platform.
        self.assertEqual(test_utils.runJob(), JobStatus.DONE)

        # Handle different comparison logic for live vs forensic
        if test_utils.dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
            # Remote live merge creates its own local batch sequence starting from 1
            local_batch_id: int = 1
            test_utils.checkSpecificBatchStatus(test_utils.storeName, local_batch_id, BatchStatus.COMMITTED, self)
            live_records: list[Any] = test_utils.getLiveRecords()
            msg = (
                f"Live records count mismatch. Expected {len(batch_data)} but got "
                f"{len(live_records)} for batch {local_batch_id}"
            )
            self.assertEqual(len(live_records), len(batch_data), msg)
            self.checkTestRecordsMatchExpected(batch_data, live_records)
        else:
            # Remote forensic merge uses primary batch ID as local batch ID
            local_batch_id: int = batch_id
            test_utils.checkSpecificBatchStatus(test_utils.storeName, local_batch_id, BatchStatus.COMMITTED, self)
            self.compare_forensic_merge_tables(test_utils, pip_utils, local_batch_id, batch_data)

    # Seed at batch 5
    def test_5_batches_remote_live_b5(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runs("Store1")

        self.generic_test_5_batches_remote_live_b5(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    def test_5_batches_remote_forensic_b5(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runs("Store1")

        self.generic_test_5_batches_remote_live_b5(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_sqlserver
    def test_5_batches_remote_live_b5_SQLServer(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsSQLServer("Store2")

        self.generic_test_5_batches_remote_live_b5(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_sqlserver
    def test_5_batches_remote_forensic_b5_SQLServer(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsSQLServer("Store2")

        self.generic_test_5_batches_remote_live_b5(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_oracle
    def test_5_batches_remote_live_b5_Oracle(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsOracle("Store3")

        self.generic_test_5_batches_remote_live_b5(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_oracle
    def test_5_batches_remote_forensic_b5_Oracle(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsOracle("Store3")

        self.generic_test_5_batches_remote_live_b5(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_db2
    def test_5_batches_remote_live_b5_DB2(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsDB2("Store4")

        self.generic_test_5_batches_remote_live_b5(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_db2
    def test_5_batches_remote_forensic_b5_DB2(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsDB2("Store4")

        self.generic_test_5_batches_remote_live_b5(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_snowflake
    def test_5_batches_remote_live_b5_Snowflake(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsSnowflake("Store5")

        self.generic_test_5_batches_remote_live_b5(live_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    @skip_snowflake
    def test_5_batches_remote_forensic_b5_Snowflake(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runsSnowflake("Store5")

        self.generic_test_5_batches_remote_live_b5(remote_merge_utils, merge_utils)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    def test_noop_incremental_batch(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runs("Store1")

        # Batch 1: seed a single record
        seed_data: list[dict[str, Any]] = [
            {"id": "id1", "firstName": "a", "lastName": "b", "dob": date.fromisoformat("2000-01-01"), "employer": "X", "dod": None}
        ]
        merge_utils.common_clear_and_insert_data(seed_data, self)
        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        self.assertEqual(live_utils.runJob(), JobStatus.DONE)
        self.assertEqual(len(live_utils.getLiveRecords()), 1)

        # Batch 2: no changes in forensic; live should be no-op and still have 1 record
        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        self.assertEqual(live_utils.runJob(), JobStatus.DONE)
        self.assertEqual(len(live_utils.getLiveRecords()), 1)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
        remote_merge_utils.baseTearDown()

    def test_reinsert_after_delete(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runs("Store1")

        # Batch 1: insert id1
        merge_utils.common_clear_and_insert_data([
            {"id": "id1", "firstName": "a", "lastName": "b", "dob": date.fromisoformat("2000-01-01"), "employer": "X", "dod": None}
        ], self)
        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        self.assertEqual(live_utils.runJob(), JobStatus.DONE)
        self.assertEqual(len(live_utils.getLiveRecords()), 1)

        # Batch 2: delete id1 (empty set)
        merge_utils.common_clear_and_insert_data([], self)
        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        self.assertEqual(live_utils.runJob(), JobStatus.DONE)
        self.assertEqual(len(live_utils.getLiveRecords()), 0)

        # Batch 3: re-insert id1 with new employer
        merge_utils.common_clear_and_insert_data([
            {"id": "id1", "firstName": "aa", "lastName": "bb", "dob": date.fromisoformat("2000-01-01"), "employer": "Y", "dod": None}
        ], self)
        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        self.assertEqual(live_utils.runJob(), JobStatus.DONE)
        recs: list[Any] = live_utils.getLiveRecords()
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["employer"], "Y")

        live_utils.baseTearDown()
        merge_utils.baseTearDown()

    def test_idempotent_same_batch_rerun(self) -> None:
        live_utils, merge_utils, remote_merge_utils = self.setup_live_and_merge_batch_runs("Store1")

        # Batch 1: insert id1
        merge_utils.common_clear_and_insert_data([
            {"id": "id1", "firstName": "a", "lastName": "b", "dob": date.fromisoformat("2000-01-01"), "employer": "X", "dod": None}
        ], self)
        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)

        # Live batch run twice for the same remote batch
        self.assertEqual(live_utils.runJob(), JobStatus.DONE)
        first: list[Any] = live_utils.getLiveRecords()
        self.assertEqual(live_utils.runJob(), JobStatus.DONE)
        second: list[Any] = live_utils.getLiveRecords()
        self.assertEqual(first, second)
        self.assertEqual(len(second), 1)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
