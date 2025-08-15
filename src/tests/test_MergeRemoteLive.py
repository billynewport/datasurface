"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
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
from typing import Optional


class TestMergeRemoteLive(unittest.TestCase):
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

        live_utils: BaseSnapshotMergeJobTest = BaseSnapshotMergeJobTest(ecoLive, "YellowLive")
        assert live_utils.store is not None
        live_utils.store.cmd = live_dp.getEffectiveCMDForDatastore(ecoLive, live_utils.store)
        live_utils.common_setup_job(SnapshotMergeJobRemoteLive, self)
        merge_utils: BaseSnapshotMergeJobTest = BaseSnapshotMergeJobTest(ecoForensic, "YellowForensic")
        assert merge_utils.store is not None
        merge_utils.store.cmd = merge_dp.getEffectiveCMDForDatastore(ecoForensic, merge_utils.store)
        merge_utils.common_setup_job(SnapshotMergeJobForensic, self)
        merge_utils.common_clear_and_insert_data([], self)

        # First, lets do a 5 batch ingestion/test on YellowForensic.
        # Step 1: Batch 1 - Insert <id1, billy, newport>
        print("Step 1: Batch 1 - Insert <id1, billy, newport>")
        test_data = [
            {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None}
        ]
        merge_utils.insertTestData(test_data)

        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        merge_utils.checkSpecificBatchStatus("Store1", 1, BatchStatus.COMMITTED, self)

        print("Step 2: Batch 2 - Insert <id1, billy, newport>, <id2, laura, diaz>")
        test_data = [
            {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        merge_utils.common_clear_and_insert_data(test_data, self)

        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        merge_utils.checkSpecificBatchStatus("Store1", 2, BatchStatus.COMMITTED, self)

        # Batch 3
        # Step 3: Batch 3 - Insert <id1, william, newport>, <id2, laura, diaz>
        print("Step 3: Batch 3 - Insert <id1, william, newport>, <id2, laura, diaz>")
        test_data = [
            {"id": "id1", "firstName": "william", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        merge_utils.common_clear_and_insert_data(test_data, self)

        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        merge_utils.checkSpecificBatchStatus("Store1", 3, BatchStatus.COMMITTED, self)

        # Batch 4
        # Step 4: Batch 4 - Insert only <id2, laura, diaz> (id1 deleted)
        print("Step 4: Batch 4 - Insert only <id2, laura, diaz> (id1 deleted)")
        test_data = [
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        merge_utils.common_clear_and_insert_data(test_data, self)

        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        merge_utils.checkSpecificBatchStatus("Store1", 4, BatchStatus.COMMITTED, self)

        # Batch 5
        # Step 5: Batch 5 - Insert <id1, billy, newport>, <id2, laura, diaz>
        print("Step 5: Batch 5 - Insert <id1, billy, newport>, <id2, laura, diaz>")
        test_data = [
            {"id": "id1", "firstName": "billy", "lastName": "newport", "dob": "1980-01-01", "employer": "Company A", "dod": None},
            {"id": "id2", "firstName": "laura", "lastName": "diaz", "dob": "1985-02-15", "employer": "Company B", "dod": None}
        ]
        merge_utils.common_clear_and_insert_data(test_data, self)

        self.assertEqual(merge_utils.runJob(), JobStatus.DONE)
        merge_utils.checkSpecificBatchStatus("Store1", 5, BatchStatus.COMMITTED, self)

        live_utils.baseTearDown()
        merge_utils.baseTearDown()
