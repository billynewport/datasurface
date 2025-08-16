"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.md.governance import CaptureMetaData
from datasurface.platforms.yellow.jobs import JobStatus
from datasurface.platforms.yellow.merge_forensic import SnapshotMergeJobForensic
from datasurface.platforms.yellow.yellow_dp import BatchStatus
from tests.test_MergeSnapshotLiveOnly import BaseSnapshotMergeJobTest
from typing import cast
from datasurface.platforms.yellow.merge_remote_live import SnapshotMergeJobRemoteLive
from datasurface.md import Ecosystem
from datasurface.md import ValidationTree
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
from typing import Any, Optional
from datetime import datetime


class TestMergeRemoteLive(unittest.TestCase):

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
                    # Convert string date to date object for comparison
                    expected_date = datetime.strptime(value, '%Y-%m-%d').date()
                    self.assertEqual(record[key], expected_date, f"Record {record} does not match test data {test_record}")
                else:
                    self.assertEqual(record[key], value, f"Record {record} does not match test data {test_record}")

    def test_5_batches_remote_live(self) -> None:
        ecoLive: Optional[Ecosystem]
        treeLive: Optional[ValidationTree]
        ecoLive, treeLive = loadEcosystemFromEcoModule("src/tests/pip_test_model")
        assert ecoLive is not None
        assert treeLive is not None
        assert not treeLive.hasErrors()

        ecoForensic: Optional[Ecosystem]
        treeForensic: Optional[ValidationTree]
        ecoForensic, treeForensic = loadEcosystemFromEcoModule("src/tests/pip_test_model")
        assert ecoForensic is not None
        assert treeForensic is not None
        assert not treeForensic.hasErrors()

        # One Ecosystem with 2 dataplatforms defined. One Datastore called Store1. YellowForensic is the PIP for Store1. Primary
        # ingestion is a forensic ingestion on YellowForensic.
        # YellowLive uses Store1 but must ingest it from the primary ingestion platform which is YellowForensic. Thus, YellowLive
        # must pull records/changes from the merge table for the datasets of Store1 in YellowForensic.\

        live_dp: YellowDataPlatform = cast(YellowDataPlatform, ecoLive.getDataPlatformOrThrow("YellowLive"))
        merge_dp: YellowDataPlatform = cast(YellowDataPlatform, ecoForensic.getDataPlatformOrThrow("YellowForensic"))
        self.assertIsNotNone(live_dp)
        self.assertIsNotNone(merge_dp)

        # Prep the live platform utils. We will use this to ingest data from the forensic platform to test the remote live ingestion.
        live_utils: BaseSnapshotMergeJobTest = BaseSnapshotMergeJobTest(ecoLive, "YellowLive")
        assert live_utils.store is not None
        old_live_cmd: Optional[CaptureMetaData] = live_utils.store.cmd
        # Live is the remote ingestion platform so the cmd should have changed to SQLMergeIngestion pointing at the primary ingestion database/tables
        live_utils.store.cmd = live_dp.getEffectiveCMDForDatastore(ecoLive, live_utils.store)
        assert live_utils.store.cmd != old_live_cmd
        live_utils.common_setup_job(SnapshotMergeJobRemoteLive, self)

        # Prep the forensic platform utils. We will use this to ingest data from the source table to the forensic platform. The live platform will then ingest
        # the data from this platform rather than the source database/table.
        merge_utils: BaseSnapshotMergeJobTest = BaseSnapshotMergeJobTest(ecoForensic, "YellowForensic")
        assert merge_utils.store is not None

        # Forensic is the primary ingestion platform so the cmd should not have changed, we still ingest from the source database/table
        old_merge_cmd: Optional[CaptureMetaData] = merge_utils.store.cmd
        merge_utils.store.cmd = merge_dp.getEffectiveCMDForDatastore(ecoForensic, merge_utils.store)
        assert merge_utils.store.cmd == old_merge_cmd

        merge_utils.common_setup_job(SnapshotMergeJobForensic, self)
        merge_utils.common_clear_and_insert_data([], self)

        # First, lets do a 5 batch ingestion/test on YellowForensic.
        batch_test_data: list[list[dict[str, Any]]] = [
            [  # Batch 1
                {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None}
            ],
            [  # Batch 2
                {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
                {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
            ],
            [  # Batch 3
                {"id": "id1", "firstName": "william", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
                {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
            ],
            [  # Batch 4
                {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
            ],
            [  # Batch 5
                {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
                {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
            ]
        ]
        batch_id: int = 1
        for batch_data in batch_test_data:
            print(f"Inserting batch {batch_id} data: {batch_data}")
            merge_utils.common_clear_and_insert_data(batch_data, self)

            self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
            merge_utils.checkSpecificBatchStatus("Store1", batch_id, BatchStatus.COMMITTED, self)
            merge_utils.checkCurrentBatchIs("Store1", batch_id, self)
            # print merge table data for debugging
            merge_table_data: list[Any] = merge_utils.getMergeTableData()
            print(f"Merge table data for batch {batch_id}: {merge_table_data}")
            # Try to ingest from the merge batch idx to the live platform.
            self.assertEqual(live_utils.runJob(), JobStatus.DONE)
            live_utils.checkSpecificBatchStatus("Store1", batch_id, BatchStatus.COMMITTED, self)
            live_records: list[Any] = live_utils.getLiveRecords()
            self.assertEqual(len(live_records), len(batch_data), f"Live records count mismatch. Expected {len(batch_data)} but got {len(live_records)} for batch {batch_id}")
            self.checkTestRecordsMatchExpected(batch_data, live_records)
            batch_id += 1

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
