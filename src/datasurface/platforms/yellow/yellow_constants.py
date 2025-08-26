"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


class YellowSchemaConstants:
    BATCH_ID_COLUMN_NAME: str = "ds_surf_batch_id"
    ALL_HASH_COLUMN_NAME: str = "ds_surf_all_hash"
    KEY_HASH_COLUMN_NAME: str = "ds_surf_key_hash"

    BATCH_IN_COLUMN_NAME: str = "ds_surf_batch_in"
    BATCH_OUT_COLUMN_NAME: str = "ds_surf_batch_out"
    IUD_COLUMN_NAME: str = "ds_surf_iud"

    # The batch out value for a live record
    LIVE_RECORD_ID: int = 0x7FFFFFFF  # MaxInt32

    SCHEMA_TYPE_MERGE: str = "MERGE"
    SCHEMA_TYPE_STAGING: str = "STAGING"
