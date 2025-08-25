"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    Datastore, Ecosystem
)
from typing import cast, Optional
from datasurface.md.governance import SQLMergeIngestion, SQLSnapshotIngestion, DataTransformerOutput
from datasurface.platforms.yellow.yellow_dp import (
    YellowDataPlatform, YellowMilestoneStrategy
)
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.platforms.yellow.merge_live import SnapshotMergeJobLiveOnly
from datasurface.platforms.yellow.merge_forensic import SnapshotMergeJobForensic
from datasurface.platforms.yellow.merge_remote_forensic import SnapshotMergeJobRemoteForensic
from datasurface.platforms.yellow.merge_remote_live import SnapshotMergeJobRemoteLive
from datasurface.platforms.yellow.merge import Job

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


def calculateCorrectJob(eco: Ecosystem, dp: YellowDataPlatform, store: Datastore, datasetName: Optional[str] = None) -> Optional[Job]:
    job: Optional[Job] = None
    # Check if this is a remote merge ingestion (from another platform's merge table)
    if dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
        if isinstance(store.cmd, SQLMergeIngestion):
            job = SnapshotMergeJobRemoteLive(eco, dp.getCredentialStore(), cast(YellowDataPlatform, dp), store, datasetName)
        elif isinstance(store.cmd, SQLSnapshotIngestion) or isinstance(store.cmd, DataTransformerOutput):
            job = SnapshotMergeJobLiveOnly(eco, dp.getCredentialStore(), cast(YellowDataPlatform, dp), store, datasetName)
        else:
            logger.error("Unknown ingestion type", ingestion_type=type(store.cmd))
            return None
    elif dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
        if isinstance(store.cmd, SQLMergeIngestion):
            # Forensic-to-forensic ingestion: remote merge from another forensic platform
            job = SnapshotMergeJobRemoteForensic(eco, dp.getCredentialStore(), cast(YellowDataPlatform, dp), store, datasetName)
        elif isinstance(store.cmd, SQLSnapshotIngestion) or isinstance(store.cmd, DataTransformerOutput):
            job = SnapshotMergeJobForensic(eco, dp.getCredentialStore(), cast(YellowDataPlatform, dp), store, datasetName)
        else:
            logger.error("Unknown ingestion type", ingestion_type=type(store.cmd))
            return None
    else:
        logger.error("Unknown milestone strategy", milestone_strategy=dp.milestoneStrategy)
        return None
    return job
