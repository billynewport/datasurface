"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, DataPlatformExecutor, Documentation, Ecosystem, ValidationTree, CloudVendor, DataContainer, \
    PlatformPipelineGraph, DataPlatformGraphHandler, PostgresDatabase, MySQLDatabase, OracleDatabase, SQLServerDatabase, EcosystemPipelineGraph
from typing import Any, Optional
from datasurface.md import LocationKey, Credential, KafkaServer, Datastore, KafkaIngestion, SQLSnapshotIngestion, ProblemSeverity, UnsupportedIngestionType, \
    DatastoreCacheEntry, IngestionConsistencyType, DatasetConsistencyNotSupported, \
    DataTransformerNode, DataTransformer, HostPortPair, HostPortPairList, Workspace, SQLIngestion, IngestionNode
from datasurface.md.governance import DatasetGroup, DataTransformerOutput, IngestionMetadata, PlatformDataTransformerHint, \
    PlatformIngestionHint, PlatformRuntimeHint, SQLWatermarkSnapshotDeltaIngestion
from datasurface.md.lint import ObjectWrongType, ObjectMissing, UnknownObjectReference, UnexpectedExceptionProblem, \
    ObjectNotSupportedByDataPlatform, AttributeValueNotSupported, AttributeNotSet, ValidationProblem
from datasurface.md.exceptions import ObjectDoesntExistException
from jinja2 import Environment, PackageLoader, select_autoescape, Template
from datasurface.md.credential import CredentialStore, CredentialType, CredentialTypeNotSupportedProblem, CredentialNotAvailableException, \
    CredentialNotAvailableProblem
from datasurface.md import SchemaProjector, DataContainerNamingMapper, Dataset, DataPlatformChooser, WorkspacePlatformConfig, \
    DataMilestoningStrategy, SQLDatabase, HostPortSQLDatabase
from datasurface.md import DataPlatformManagedDataContainer, PlatformServicesProvider
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyList
from datasurface.md.types import Integer, VarChar
from datasurface.md import StorageRequirement, DataPlatformKey, PrimaryIngestionPlatform
import os
import re
from datasurface.md.repo import GitHubRepository
from typing import cast
import copy
from enum import Enum
import json
import sqlalchemy
from typing import List
from sqlalchemy import Table, Column, TIMESTAMP, MetaData, Engine, Connection
from sqlalchemy import text
from datasurface.platforms.yellow.db_utils import createEngine, getDriverNameAndQueryForDataContainer, createInspector
from datasurface.platforms.yellow.data_ops_factory import DatabaseOperationsFactory
from datasurface.md.sqlalchemyutils import createOrUpdateTable, datasetToSQLAlchemyTable
from datasurface.md import CronTrigger, ExternallyTriggered, StepTrigger, SQLMergeIngestion, CaptureMetaData
from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
import hashlib
from sqlalchemy.engine.reflection import Inspector
from datasurface.md.codeartifact import PythonRepoCodeArtifact
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger, set_context,
    log_operation_timing
)
from datasurface.platforms.yellow.assembly import K8sResourceLimits, K8sUtils, Component, Assembly
from datasurface.platforms.yellow.assembly import K8sAssemblyFactory
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants
from datasurface.platforms.yellow.database_operations import DatabaseOperations

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


"""
This job runs to ingest/merge new data into a dataset. The data is ingested from a source in to a staging table named
after the dataset. The data is ingested in batches and every ingested record is extended with the batch id that ingested
it as well as key and all column md5 hashs. The hashes improve performance when comparing records for equality or
looking up specific records as it allows a single column to be used rather than the entire record or complete primary
key.

The process is tracked using a metadata table, the batch_status table and a table for storing batch counters. The job ingested the
next batch of data from an ingestion stream and then merges it in to the merge table. Thus, an ingestion stream is a serial
collection of batches. Each batch ingests a chunk of data from the source and then merges it with the corresponding merge table
for the dataset. The process state is tracked in a batch_metrics record keyed to the ingestion stream and the batch id. This record
tracks that the batch has been started, is ingesting, is ready for merge or is complete. Some basic batch metrics are also
stored in the batch_metrics table.

The ingestion streams are named/keyed depending on whather the ingestion stream is for every dataset in a store or for just
one dataset in a datastore. The key is either just {storeName} or '{storeName}#{datasetName}.

The state of the ingestion has the following keys:

- datasets_to_go: A list of outstanding datasets to ingest
- current: tuple of dataset_name and offset. The offset is where ingestion of the dataset should restart.

When the last dataset is ingested, the state is set to MERGING and all datasets are merged in a single tx.

This job is designed to be run by Airflow as a KubernetesPodOperator. It returns:
- Exit code 0: "KEEP_WORKING" - The batch is still in progress, reschedule the job
- Exit code 1: "DONE" - The batch is committed or failed, stop rescheduling
"""


class JobStatus(Enum):
    """This is the status of a job"""
    KEEP_WORKING = 1  # The job is still in progress, put on queue and continue ASAP
    DONE = 0  # The job is complete, wait for trigger to run batch again
    ERROR = -1  # The job failed, stop the job and don't run again


# The maximum length of a stream key. This is the key used to identify a stream of data.
STREAM_KEY_MAX_LENGTH: int = 255


class JobUtilities(ABC):
    """This class provides utilities for working with jobs in the Yellow platform."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: 'YellowDataPlatform') -> None:
        self.eco: Ecosystem = eco
        self.dp: YellowDataPlatform = dp
        self.credStore: CredentialStore = credStore
        self.schemaProjector: Optional[YellowSchemaProjector] = cast(YellowSchemaProjector, self.dp.createSchemaProjector(eco))

        # Set logging context for this job
        set_context(platform=dp.name)

    def getPhysBatchCounterTableName(self) -> str:
        """This returns the name of the batch counter table"""
        return self.dp.psp.namingMapper.fmtTVI(self.dp.getTableForPlatform("batch_counter"))

    def getPhysBatchMetricsTableName(self) -> str:
        """This returns the name of the batch metrics table"""
        return self.dp.psp.namingMapper.fmtTVI(self.dp.getTableForPlatform("batch_metrics"))

    def getBatchCounterTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch counter table"""
        from sqlalchemy.sql import quoted_name
        t: Table = Table(self.getPhysBatchCounterTableName(), MetaData(),
                         Column(quoted_name("key", quote=True), sqlalchemy.String(length=STREAM_KEY_MAX_LENGTH), primary_key=True),
                         Column(quoted_name("currentBatch", quote=True), sqlalchemy.Integer()))
        return t

    def getBatchMetricsTable(self, mergeConnection: Optional[Connection] = None) -> Table:
        """This constructs the sqlalchemy table for the batch metrics table. The key is either the data store name or the
        data store name and the dataset name."""
        from sqlalchemy.sql import quoted_name
        # Use database-specific datetime type - default to TIMESTAMP for compatibility
        if mergeConnection is not None and hasattr(mergeConnection, 'dialect') and 'mssql' in str(mergeConnection.dialect.name):
            from sqlalchemy.types import DATETIME
            datetime_type = DATETIME()
        else:
            datetime_type = sqlalchemy.TIMESTAMP()
        t: Table = Table(self.getPhysBatchMetricsTableName(), MetaData(),
                         Column(quoted_name("key", quote=True), sqlalchemy.String(length=STREAM_KEY_MAX_LENGTH), primary_key=True),
                         Column(quoted_name("batch_id", quote=True), sqlalchemy.Integer(), primary_key=True),
                         Column(quoted_name("batch_start_time", quote=True), datetime_type),
                         Column(quoted_name("batch_end_time", quote=True), datetime_type, nullable=True),
                         Column(quoted_name("batch_status", quote=True), sqlalchemy.String(length=32)),
                         Column(quoted_name("records_inserted", quote=True), sqlalchemy.Integer(), nullable=True),
                         Column(quoted_name("records_updated", quote=True), sqlalchemy.Integer(), nullable=True),
                         Column(quoted_name("records_deleted", quote=True), sqlalchemy.Integer(), nullable=True),
                         Column(quoted_name("total_records", quote=True), sqlalchemy.Integer(), nullable=True),
                         Column(quoted_name("state", quote=True), sqlalchemy.String(length=2048), nullable=True))
        return t

    def getSchemaHash(self, dataset: Dataset) -> str:
        """Generate a hash of the dataset schema"""
        schema: DDLTable = cast(DDLTable, dataset.originalSchema)
        schema_data = {
            'columns': {name: {'type': str(col.type), 'nullable': str(col.nullable)}
                        for name, col in schema.columns.items()},
            'primaryKey': schema.primaryKeyColumns.colNames if schema.primaryKeyColumns else []
        }
        schema_str = json.dumps(schema_data, sort_keys=True)
        return hashlib.md5(schema_str.encode()).hexdigest()

    def validateSchemaUnchanged(self, dataset: Dataset, stored_hash: str) -> bool:
        """Validate that the schema hasn't changed since batch start"""
        current_hash = self.getSchemaHash(dataset)
        return current_hash == stored_hash

    def getRawBaseTableNameForDataset(self, store: Datastore, dataset: Dataset, allowMapping: bool) -> str:
        """This returns the base table name for a dataset"""
        if isinstance(store.cmd, SQLIngestion):
            if allowMapping:
                if dataset.name in store.cmd.tableForDataset:
                    return f"{store.name}_{store.cmd.tableForDataset[dataset.name]}"
                else:
                    return f"{store.name}_{dataset.name}"
            else:
                return f"{store.name}_{dataset.name}"
        elif isinstance(store.cmd, DataTransformerOutput):
            return f"{store.name}_{dataset.name}"  # ✅ Simple base name without circular call
        else:
            raise Exception(f"Unknown store command type: {type(store.cmd)}")

    def getRawMergeTableNameForDataset(self, store: Datastore, dataset: Dataset) -> str:
        """This returns the merge table name for a dataset"""
        tableName: str = self.getRawBaseTableNameForDataset(store, dataset, False)
        return self.dp.getTableForPlatform(tableName + "_merge")

    def getPhysDataTransformerOutputTableNameForDatasetForIngestionOnly(self, store: Datastore, dataset: Dataset) -> str:
        """This returns the DataTransformer output table name for a dataset (dt_ prefix) for ingestion only. Merged tables are stored in the
        normal merge table notation. The dt prefix is ONLY used for the output tables for a DataTransformer when doing the ingestion
        of these output tables. Once the data is ingested for output datastores, the data is merged in to the normal merge tables."""
        tableName: str = self.getRawBaseTableNameForDataset(store, dataset, False)
        return self.dp.psp.namingMapper.fmtTVI(self.dp.getTableForPlatform(f"dt_{tableName}"))


class K8sDataTransformerHint(PlatformDataTransformerHint):
    def __init__(self, workspaceName: str, resourceLimits: K8sResourceLimits):
        super().__init__(workspaceName)
        self.resourceLimits: K8sResourceLimits = resourceLimits

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"resourceLimits": self.resourceLimits.to_json()})
        return rc

    def to_k8s_json(self) -> dict[str, Any]:
        """Convert YellowDataTransformerHint to Kubernetes JSON format."""
        return {
            "requested_memory": Component.storageToKubernetesFormat(self.resourceLimits.requested_memory),
            "limits_memory": Component.storageToKubernetesFormat(self.resourceLimits.limits_memory),
            "requested_cpu": Component.cpuToKubernetesFormat(self.resourceLimits.requested_cpu),
            "limits_cpu": Component.cpuToKubernetesFormat(self.resourceLimits.limits_cpu)
        }

    def __eq__(self, other: object) -> bool:
        if isinstance(other, K8sDataTransformerHint):
            return super().__eq__(other) and self.resourceLimits == other.resourceLimits
        return False

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        super().lint(eco, tree)
        self.resourceLimits.lint(tree.addSubTree(self.resourceLimits))


class K8sIngestionHint(PlatformIngestionHint):
    def __init__(self, storeName: str, resourceLimits: K8sResourceLimits, datasetName: Optional[str] = None):
        super().__init__(storeName, datasetName)
        self.resourceLimits: K8sResourceLimits = resourceLimits

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"resourceLimits": self.resourceLimits.to_json()})
        return rc

    def to_k8s_json(self) -> dict[str, Any]:
        """Convert YellowIngestionHint to Kubernetes JSON format."""
        return {
            "requested_memory": Component.storageToKubernetesFormat(self.resourceLimits.requested_memory),
            "limits_memory": Component.storageToKubernetesFormat(self.resourceLimits.limits_memory),
            "requested_cpu": Component.cpuToKubernetesFormat(self.resourceLimits.requested_cpu),
            "limits_cpu": Component.cpuToKubernetesFormat(self.resourceLimits.limits_cpu)
        }

    def __eq__(self, other: object) -> bool:
        if isinstance(other, K8sIngestionHint):
            return super().__eq__(other) and self.resourceLimits == other.resourceLimits
        return False

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        super().lint(eco, tree)
        self.resourceLimits.lint(tree.addSubTree(self.resourceLimits))


class YellowDatasetUtilities(JobUtilities):
    """This class provides utilities for working with datasets in the Yellow platform."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: 'YellowDataPlatform', store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp)
        self.store: Datastore = store
        self.datasetName: Optional[str] = datasetName
        self.dataset: Optional[Dataset] = self.store.datasets[datasetName] if datasetName is not None else None

        # Initialize database operations based on merge store type
        assert self.schemaProjector is not None, "Schema projector must be initialized"
        self.db_ops = DatabaseOperationsFactory.create_database_operations(
            dp.psp.mergeStore, self.schemaProjector
        )

    def checkForSchemaChanges(self, state: 'BatchState') -> None:
        """Check if any dataset schemas have changed since batch start"""
        for dataset_name in state.all_datasets:
            dataset = self.store.datasets[dataset_name]
            stored_hash = state.schema_versions.get(dataset_name)
            if stored_hash and not self.validateSchemaUnchanged(dataset, stored_hash):
                raise Exception(f"Schema changed for dataset {dataset_name} during batch processing")

    def getIngestionStreamKey(self) -> str:
        """This returns the key for the batch"""
        key: str = f"{self.store.name}#{self.dataset.name}" if self.dataset is not None else self.store.name
        # If its too long then truncate it with mangling
        return DataContainerNamingMapper.truncateIdentifier(key, STREAM_KEY_MAX_LENGTH)

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str, mergeConnection: Optional[Connection] = None) -> Table:
        """This returns the staging schema for a dataset"""
        stagingDS: Dataset = self.schemaProjector.computeSchema(dataset, YellowSchemaConstants.SCHEMA_TYPE_STAGING, self.db_ops)
        t: Table = datasetToSQLAlchemyTable(stagingDS, tableName, sqlalchemy.MetaData(), mergeConnection)
        return t

    def getMergeSchemaForDataset(self, dataset: Dataset, tableName: str, mergeConnection: Optional[Connection] = None) -> Table:
        """This returns the merge schema for a dataset"""
        mergeDS: Dataset = self.schemaProjector.computeSchema(dataset, YellowSchemaConstants.SCHEMA_TYPE_MERGE, self.db_ops)
        t: Table = datasetToSQLAlchemyTable(mergeDS, tableName, sqlalchemy.MetaData(), mergeConnection)
        return t

    def getBaseTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the base table name for a dataset"""
        return self.getRawBaseTableNameForDataset(self.store, dataset, False)

    def getPhysStagingTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the staging table name for a dataset"""
        tableName: str = self.getBaseTableNameForDataset(dataset)
        return self.dp.psp.namingMapper.fmtTVI(self.dp.getTableForPlatform(tableName + "_staging"))

    def getPhysMergeTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the merge table name for a dataset"""
        return self.dp.psp.namingMapper.fmtTVI(self.getRawMergeTableNameForDataset(self.store, dataset))

    def createStagingTableIndexes(self, mergeConnection: Connection, tableName: str) -> None:
        """Create performance indexes for staging tables"""
        assert self.schemaProjector is not None

        batchIdIndexName: str = self.dp.psp.namingMapper.fmtTVI(f"idx_{tableName}_batch_id")
        keyHashIndexName: str = self.dp.psp.namingMapper.fmtTVI(f"idx_{tableName}_key_hash")
        batchKeyIndexName: str = self.dp.psp.namingMapper.fmtTVI(f"idx_{tableName}_batch_key")
        indexes = [
            # Primary: batch filtering (used in every query)
            (batchIdIndexName, [YellowSchemaConstants.BATCH_ID_COLUMN_NAME]),

            # Secondary: join performance
            (keyHashIndexName, [YellowSchemaConstants.KEY_HASH_COLUMN_NAME]),

            # Composite: optimal for most queries that filter by batch AND join by key
            (batchKeyIndexName, [YellowSchemaConstants.BATCH_ID_COLUMN_NAME, YellowSchemaConstants.KEY_HASH_COLUMN_NAME])
        ]

        for index_name, columns in indexes:
            # Check if index already exists using database operations
            index_exists = self.db_ops.check_index_exists(mergeConnection, tableName, index_name)

            if not index_exists:
                try:
                    with log_operation_timing(logger, "create_staging_index", index_name=index_name):
                        index_sql = self.db_ops.create_index_sql(index_name, tableName, columns)
                        mergeConnection.execute(sqlalchemy.text(index_sql))
                    logger.info("Created staging index successfully", index_name=index_name, table_name=tableName)
                except Exception as e:
                    logger.error("Failed to create staging index",
                                 index_name=index_name,
                                 table_name=tableName,
                                 error=str(e))

    def createMergeTableIndexes(self, mergeConnection: Connection, tableName: str) -> None:
        """Create performance indexes for merge tables"""
        assert self.schemaProjector is not None

        keyHashIndexName: str = self.dp.psp.namingMapper.fmtTVI(f"idx_{tableName}_key_hash")
        indexes = [
            # Critical: join performance (exists in all modes)
            (keyHashIndexName, [YellowSchemaConstants.KEY_HASH_COLUMN_NAME])
        ]

        # Add forensic-specific indexes for batch milestoned tables
        if self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
            batchOutIndexName: str = self.dp.psp.namingMapper.fmtTVI(f"idx_{tableName}_batch_out")
            liveRecordsIndexName: str = self.dp.psp.namingMapper.fmtTVI(f"idx_{tableName}_live_records")
            batchInIndexName: str = self.dp.psp.namingMapper.fmtTVI(f"idx_{tableName}_batch_in")
            batchRangeIndexName: str = self.dp.psp.namingMapper.fmtTVI(f"idx_{tableName}_batch_range")
            indexes.extend([
                # Critical: live record filtering (batch_out = 2147483647)
                (batchOutIndexName, [YellowSchemaConstants.BATCH_OUT_COLUMN_NAME]),

                # Composite: optimal for live record joins (most common pattern)
                (liveRecordsIndexName, [YellowSchemaConstants.KEY_HASH_COLUMN_NAME, YellowSchemaConstants.BATCH_OUT_COLUMN_NAME]),

                # For forensic queries: finding recently closed records
                (batchInIndexName, [YellowSchemaConstants.BATCH_IN_COLUMN_NAME]),

                # For forensic history queries
                (batchRangeIndexName, [YellowSchemaConstants.BATCH_IN_COLUMN_NAME, YellowSchemaConstants.BATCH_OUT_COLUMN_NAME])
            ])
        else:
            # For live-only mode, we can create an index on ds_surf_all_hash for change detection
            allHashIndexName: str = self.dp.psp.namingMapper.fmtTVI(f"idx_{tableName}_all_hash")
            indexes.extend([
                # For live-only mode: change detection performance
                (allHashIndexName, [YellowSchemaConstants.ALL_HASH_COLUMN_NAME])
            ])

        for index_name, columns in indexes:
            # Check if index already exists
            # Check if index already exists using database operations
            index_exists = self.db_ops.check_index_exists(mergeConnection, tableName, index_name)

            if not index_exists:
                try:
                    with log_operation_timing(logger, "create_merge_index", index_name=index_name):
                        index_sql = self.db_ops.create_index_sql(index_name, tableName, columns)
                        mergeConnection.execute(sqlalchemy.text(index_sql))
                    logger.info("Created merge index successfully", index_name=index_name, table_name=tableName)
                except Exception as e:
                    logger.error("Failed to create merge index",
                                 index_name=index_name,
                                 table_name=tableName,
                                 error=str(e))

    def ensureUniqueConstraintExists(self, mergeConnection: Connection, tableName: str, columnName: str) -> None:
        """Ensure that a unique constraint exists on the specified column"""
        # Use database-specific constraint checking
        constraint_exists = self.db_ops.check_unique_constraint_exists(mergeConnection, tableName, columnName)

        if not constraint_exists:
            # Create the unique constraint using database-specific method
            constraint_name = self.db_ops.create_unique_constraint(mergeConnection, tableName, columnName)
            logger.debug("Creating unique constraint",
                         constraint_name=constraint_name,
                         table_name=tableName,
                         column_name=columnName)
            logger.info("Successfully created unique constraint",
                        constraint_name=constraint_name,
                        table_name=tableName,
                        column_name=columnName)

    def _createOrUpdateTableWithPrimaryKeyHandling(self, mergeConnection: Connection, inspector: Inspector,
                                                   table: Table, tableName: str, tableType: str) -> None:
        """Common method to create or update a table, handling primary key changes by modifying constraints"""

        if not inspector.has_table(tableName):
            # Table doesn't exist, create it
            table.create(mergeConnection)
            logger.info(f"Created {tableType} table", table_name=tableName)
            return

        # Table exists, check if primary key needs to be updated
        current_table = Table(tableName, MetaData(), autoload_with=mergeConnection)

        # Get current primary key columns
        current_pk_columns = []
        for col in current_table.columns:
            if col.primary_key:
                current_pk_columns.append(col.name)

        # Get desired primary key columns from the table
        desired_pk_columns = []
        for col in table.columns:
            if col.primary_key:
                desired_pk_columns.append(col.name)

        # Check if primary key needs to be changed
        if set(current_pk_columns) != set(desired_pk_columns):
            logger.info(f"Primary key change detected for {tableType} table",
                        table_name=tableName,
                        current_pk=current_pk_columns,
                        desired_pk=desired_pk_columns)

            # Modify primary key constraint without dropping table
            # Get the primary key constraint name
            pk_constraints = inspector.get_pk_constraint(tableName)
            pk_name = pk_constraints.get('name', f"{tableName}_pkey")

            # Drop the existing primary key constraint
            mergeConnection.execute(sqlalchemy.text(f"ALTER TABLE {tableName} DROP CONSTRAINT {pk_name}"))
            logger.info("Dropped primary key constraint", table_name=tableName, constraint_name=pk_name)

            # Add the new primary key constraint
            pk_columns_str = ", ".join(desired_pk_columns)
            mergeConnection.execute(sqlalchemy.text(f"ALTER TABLE {tableName} ADD PRIMARY KEY ({pk_columns_str})"))
            logger.info("Added new primary key constraint", table_name=tableName, pk_columns=desired_pk_columns)
        else:
            # Primary key is the same, use normal update
            createOrUpdateTable(mergeConnection, inspector, table)

    def createOrUpdateForensicTable(self, mergeConnection: Connection, inspector: Inspector, mergeTable: Table, tableName: str) -> None:
        """Create or update a forensic table, handling primary key changes by modifying constraints"""
        self._createOrUpdateTableWithPrimaryKeyHandling(mergeConnection, inspector, mergeTable, tableName, "forensic")

    def createOrUpdateStagingTable(self, mergeConnection: Connection, inspector: Inspector, stagingTable: Table, tableName: str) -> None:
        """Create or update a staging table, handling primary key changes by modifying constraints"""
        self._createOrUpdateTableWithPrimaryKeyHandling(mergeConnection, inspector, stagingTable, tableName, "staging")

    def reconcileStagingTableSchemas(self, mergeConnection: Connection, inspector: Inspector, store: Datastore) -> None:
        """This will make sure the staging table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = self.getPhysStagingTableNameForDataset(dataset)
            stagingTable: Table = self.getStagingSchemaForDataset(dataset, tableName, mergeConnection)
            # Handle primary key changes for staging tables
            self.createOrUpdateStagingTable(mergeConnection, inspector, stagingTable, tableName)
            # Create performance indexes for staging table
            self.createStagingTableIndexes(mergeConnection, tableName)

    def reconcileMergeTableSchemas(self, mergeConnection: Connection, inspector: Inspector, store: Datastore) -> None:
        """This will make sure the merge table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = self.getPhysMergeTableNameForDataset(dataset)
            mergeTable: Table = self.getMergeSchemaForDataset(dataset, tableName, mergeConnection)

            # For forensic mode, we need to handle primary key changes
            if self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
                self.createOrUpdateForensicTable(mergeConnection, inspector, mergeTable, tableName)
            else:
                createOrUpdateTable(mergeConnection, inspector, mergeTable)
                # Only add unique constraint on key_hash for live-only mode
                # In forensic mode, multiple records can have the same key_hash (different versions)
                self.ensureUniqueConstraintExists(mergeConnection, tableName, "ds_surf_key_hash")

            # Create performance indexes for merge table
            self.createMergeTableIndexes(mergeConnection, tableName)

    def _normalizeNameComponent(self, name: str) -> str:
        """Normalize a name component for view naming consistency"""
        return name.lower().replace(' ', '_').replace('-', '_')

    def getPhysWorkspaceViewName(self, workspace_name: str, dsg_name: str) -> str:
        """Generate a workspace view name following the pattern: dataplatform/workspace/dsg/store/dataset_view"""
        dp_name = self._normalizeNameComponent(self.dp.name)
        ws_name = self._normalizeNameComponent(workspace_name)
        dsg_name = self._normalizeNameComponent(dsg_name)
        store_name = self._normalizeNameComponent(self.store.name)
        dataset_name = self._normalizeNameComponent(self.dataset.name)

        return self.dp.psp.namingMapper.fmtTVI(f"{dp_name}_{ws_name}_{dsg_name}_{store_name}_{dataset_name}_view")

    def getPhysWorkspaceFullViewName(self, workspace_name: str, dsg_name: str) -> str:
        """Generate a full workspace view name with _full suffix for forensic platforms"""
        dp_name = self._normalizeNameComponent(self.dp.name)
        ws_name = self._normalizeNameComponent(workspace_name)
        dsg_name = self._normalizeNameComponent(dsg_name)
        store_name = self._normalizeNameComponent(self.store.name)
        dataset_name = self._normalizeNameComponent(self.dataset.name)

        return self.dp.psp.namingMapper.fmtTVI(f"{dp_name}_{ws_name}_{dsg_name}_{store_name}_{dataset_name}_view_full")

    def getPhysWorkspaceLiveViewName(self, workspace_name: str, dsg_name: str) -> str:
        """Generate a live workspace view name with _live suffix for both platform types"""
        dp_name = self._normalizeNameComponent(self.dp.name)
        ws_name = self._normalizeNameComponent(workspace_name)
        dsg_name = self._normalizeNameComponent(dsg_name)
        store_name = self._normalizeNameComponent(self.store.name)
        dataset_name = self._normalizeNameComponent(self.dataset.name)

        return self.dp.psp.namingMapper.fmtTVI(f"{dp_name}_{ws_name}_{dsg_name}_{store_name}_{dataset_name}_view_live")


class BatchStatus(Enum):
    """This is the status of a batch"""
    STARTED = "started"  # The batch is started, ingestion is in progress
    INGESTED = "ingested"  # The batch is ingested, merge is in progress
    COMMITTED = "committed"  # The batch is committed, no more work to do
    FAILED = "failed"  # The batch failed, reset batch to try again


class BatchState(BaseModel):
    """This is the state of a batch being processed. It provides a list of datasets which need to be
    ingested still and for the dataset currently being ingested, where to start ingestion from in terms
    of an offset in the source table."""

    all_datasets: List[str]
    current_dataset_index: int = 0
    current_offset: int = 0
    schema_versions: dict[str, str] = Field(default_factory=dict)
    # This is used to store state for the next batch to use during ingestion/merge. It can also be used to hold state for the current batch
    # when the curent batch is too large to do in a single job. It's the restart point for both the current batch and the next batch when
    # the batch is closed.
    job_state: dict[str, Any] = Field(default_factory=dict)

    def reset(self) -> None:
        """This resets the state to the start of the batch"""
        self.current_dataset_index = 0
        self.current_offset = 0

    def getCurrentDataset(self) -> str:
        """This returns the current dataset"""
        return self.all_datasets[self.current_dataset_index]

    def moveToNextDataset(self) -> None:
        """This moves to the next dataset"""
        self.current_dataset_index += 1
        self.current_offset = 0

    def hasMoreDatasets(self) -> bool:
        """This returns True if there are more datasets to ingest"""
        return self.current_dataset_index < len(self.all_datasets)


class KubernetesEnvVarsCredentialStore(CredentialStore):
    """This acts as a factory to create credentials and allow DataPlatforms to get credentials. It tries to hide the mechanism. Whether
    this uses local files or env variables is hidden from the DataPlatform. The secrets exist within a single namespace. This code returns
    the various types of supported credentials in methods which are called by the DataPlatform."""
    def __init__(self, name: str, locs: set[LocationKey]):
        super().__init__(name, locs)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
            }
        )
        return rc

    def checkCredentialIsAvailable(self, cred: Credential, tree: ValidationTree) -> None:
        """This is used to check if a Credential is supported by this store."""
        pass

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and \
            isinstance(other, KubernetesEnvVarsCredentialStore) and \
            self.name == other.name and \
            self.locs == other.locs

    def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
        """This returns the username and password for the credential"""
        # Fetch the user and password from the environment variables with a prefix of the credential name
        # and a suffix of _USER and _PASSWORD
        user: Optional[str] = os.getenv(f"{cred.name}_USER")
        password: Optional[str] = os.getenv(f"{cred.name}_PASSWORD")
        if user is None or password is None:
            raise CredentialNotAvailableException(cred, "user or password is None")
        return user, password

    def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
        """This fetches the credential and returns a tuple with the public and private key
        strings and an the private key password"""
        """These all need to be in environment variables"""
        pub_key: Optional[str] = os.getenv(f"{cred.name}_PUB")
        prv_key: Optional[str] = os.getenv(f"{cred.name}_PRV")
        pwd: Optional[str] = os.getenv(f"{cred.name}_PWD")
        if pub_key is None or prv_key is None or pwd is None:
            raise CredentialNotAvailableException(cred, "pub_key or prv_key or pwd is None")
        return pub_key, prv_key, pwd

    def getAsToken(self, cred: Credential) -> str:
        """This fetches the credential and returns a token. This is used for API tokens."""
        envName: str = f"{cred.name}_TOKEN"
        token: Optional[str] = os.getenv(envName)
        if token is None:
            raise CredentialNotAvailableException(cred, f"token for {envName} is None")
        return token

    def isLegalEnvVarName(self, name: str) -> bool:
        """This checks if the name is a legal environment variable name."""
        return re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name) is not None

    def lintCredential(self, cred: Credential, tree: ValidationTree) -> None:
        """This checks that the type is supported and the name is compatible with an environment variable name."""
        # First check the name is compatible with an environment variable name
        if not cred.name.isidentifier():
            tree.addRaw(ObjectWrongType(cred, Credential, ProblemSeverity.ERROR))
        # Then check the type is either secret, api token or user password
        if cred.credentialType not in [CredentialType.API_KEY_PAIR, CredentialType.API_TOKEN, CredentialType.USER_PASSWORD]:
            tree.addRaw(CredentialTypeNotSupportedProblem(cred, [CredentialType.API_KEY_PAIR, CredentialType.API_TOKEN, CredentialType.USER_PASSWORD]))
            return
        # Then check the name is compatible with an environment variable name
        if not self.isLegalEnvVarName(cred.name):
            tree.addProblem("Credential name not compatible with an environment variable name", ProblemSeverity.ERROR)
            return


class YellowPlatformExecutor(DataPlatformExecutor):
    def __init__(self):
        super().__init__()

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass


class IngestionType(Enum):
    """This is the type of ingestion. This is used in infrastructure DAG generation to figure out which parameters/env variables
    to provide to the kubernetes ingestion/merge job."""
    KAFKA = "kafka"  # Provide credentials for a Kafka cluster
    SQL_SOURCE = "sql_source"  # Provide credentials for a SQL source database, works for all SQL merge types


class YellowGraphHandler(DataPlatformGraphHandler):
    """This takes the graph and then implements the data pipeline described in the graph using the technology stack
    pattern implemented by this platform. This platform supports ingesting data from Kafka confluence connectors. It
    takes the data from kafka topics and writes them to a SQL staging table. A seperate job scheduled by airflow
    then runs periodically and merges the staging data in to a MERGE table as a batch. Any workspaces can also query the
    data in the MERGE tables through Workspace specific views."""
    def __init__(self, dp: 'YellowDataPlatform', graph: PlatformPipelineGraph) -> None:
        super().__init__(graph)
        self.dp: YellowDataPlatform = dp
        self.env: Environment = Environment(
            loader=PackageLoader('datasurface.platforms.yellow.templates', 'jinja'),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def getInternalDataContainers(self) -> set[DataContainer]:
        """This returns any DataContainers used by the platform."""
        return {self.dp.psp.kafkaConnectCluster, self.dp.psp.mergeStore}

    def createJinjaTemplate(self, name: str) -> Template:
        template: Template = self.env.get_template(name, None)
        return template

    def createTerraformForAllIngestedNodes(self, eco: Ecosystem, tree: ValidationTree) -> str:
        """This creates a terraform file for all the Kafka ingested nodes in the graph using jinja templates which are found
        in the templates directory. It creates a sink connector to copy from the topics to a SQL
        staging file and a cluster link resource if required to recreate the topics on this cluster.
        Any errors during generation will just be added as ValidationProblems to the tree using the appropriate
        subtree so the user can see which object caused the errors. The caller method will check if the tree
        has errors and stop the generation process.

        Note: SQL snapshot ingestion doesn't require Kafka infrastructure, so this method only handles Kafka ingestion."""
        template: Template = self.createJinjaTemplate('kafka_topic_to_staging.jinja2')

        # Ensure the platform is the correct type early on
        if not isinstance(self.graph.platform, YellowDataPlatform):
            logger.error("Platform associated with the graph is not a YellowDataPlatform",
                         platform_type=type(self.graph.platform).__name__)
            tree.addRaw(ObjectWrongType(self.graph.platform, YellowDataPlatform, ProblemSeverity.ERROR))
            return ""

        platform: YellowDataPlatform = self.graph.platform

        ingest_nodes: list[dict[str, Any]] = []
        for storeName in self.graph.storesToIngest:
            try:
                storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(storeName)
                store: Datastore = storeEntry.datastore
                yu: YellowDatasetUtilities = YellowDatasetUtilities(eco, self.dp.psp.credStore, self.dp, store)

                if isinstance(store.cmd, KafkaIngestion):
                    # For each dataset in the store
                    for dataset in store.datasets.values():
                        datasetName = dataset.name
                        # Example: Create a unique connector name based on store and dataset
                        connector_name = f"{storeName}_{datasetName}".replace("-", "_")  # Terraform identifier

                        # Determine other node-specific attributes
                        kafka_topic = f"{storeName}.{datasetName}"  # Example topic naming convention
                        target_table_name = yu.getPhysStagingTableNameForDataset(dataset)
                        input_data_format = "JSON"  # Default or derive from schema/metadata

                        # These should not be None after passing lint
                        assert dataset.originalSchema is not None
                        assert dataset.originalSchema.primaryKeyColumns is not None
                        assert dataset.originalSchema.primaryKeyColumns.colNames is not None

                        # Build node-specific config
                        node_data = {
                            "connector_name": connector_name,
                            "kafka_topic": kafka_topic,
                            "target_table_name": target_table_name,
                            "input_data_format": input_data_format,
                            "tasks_max": 1,  # Default
                            "primary_key_fields": dataset.originalSchema.primaryKeyColumns.colNames
                            # Add node.connector_config_overrides if needed/available
                        }
                        ingest_nodes.append(node_data)

                elif isinstance(store.cmd, SQLSnapshotIngestion):
                    # SQL snapshot ingestion doesn't need Kafka infrastructure
                    # The SnapshotMergeJob will handle the ingestion directly from source database
                    continue
                elif isinstance(store.cmd, SQLWatermarkSnapshotDeltaIngestion):
                    # SQL watermark snapshot delta ingestion doesn't need Kafka infrastructure
                    # The SnapshotMergeJob will handle the ingestion directly from source database
                    continue
                elif isinstance(store.cmd, DataTransformerOutput):
                    # DataTransformerOutput doesn't need Kafka infrastructure
                    continue
                else:
                    tree.addRaw(ObjectMissing(store, "cmd is none or is not supported ingestion type", ProblemSeverity.ERROR))
                    continue

            except ObjectDoesntExistException:
                tree.addRaw(UnknownObjectReference(storeName, ProblemSeverity.ERROR))
                return ""

        # Lint Workspace data transformers, only PythonRepoCodeArtifacts are supported
        for dsgRoot in self.graph.roots:
            workspace: Workspace = dsgRoot.workspace
            if workspace.dataTransformer is not None:
                if not isinstance(workspace.dataTransformer.code, PythonRepoCodeArtifact):
                    tree.addRaw(ObjectWrongType(workspace.dataTransformer.code, PythonRepoCodeArtifact, ProblemSeverity.ERROR))

        # If no Kafka ingestion nodes, return empty terraform
        if not ingest_nodes:
            return "# No Kafka ingestion nodes found - SQL snapshot ingestion doesn't require Kafka infrastructure"

        # Prepare the global context for the template
        try:
            pg_user, pg_password = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.mergeRW_Credential)
            # Prepare Kafka API keys if needed (using connectCredentials)
            # kafka_api_key, kafka_api_secret = self.getKafkaKeysFromCredential(platform.connectCredentials)
            # TODO: Implement getKafkaKeysFromCredential similar to getPostgresUserPasswordFromCredential
            kafka_api_key = "placeholder_kafka_api_key"  # Placeholder
            kafka_api_secret = "placeholder_kafka_api_secret"  # Placeholder

            context: dict[str, Any] = {
                "ingest_nodes": ingest_nodes,
                "database_name": self.dp.psp.mergeStore.databaseName,
                "database_user": pg_user,
                "database_password": pg_password,
                "kafka_api_key": kafka_api_key,  # Placeholder
                "kafka_api_secret": kafka_api_secret,  # Placeholder
                # Add default_connector_config if defined in KPSGraphHandler
            }
            if isinstance(self.dp.psp.mergeStore, HostPortSQLDatabase):
                context["database_host"] = self.dp.psp.mergeStore.hostPortPair.hostName
                context["database_port"] = self.dp.psp.mergeStore.hostPortPair.port
            else:
                raise ValueError(f"Unsupported merge store type: {type(self.dp.psp.mergeStore)}")

            # Render the template once with the full context
            with log_operation_timing(logger, "terraform_code_generation", platform_name=platform.name):
                code: str = template.render(context)

            logger.info("Generated Terraform code successfully",
                        platform_name=platform.name,
                        ingestion_nodes_count=len(ingest_nodes))
            return code

        except CredentialNotAvailableException as e:
            tree.addRaw(CredentialNotAvailableProblem(e.cred, e.issue))
        except Exception as e:
            tree.addRaw(UnexpectedExceptionProblem(e))

        """Must be an issue, just return an empty string, the linting will have the details"""
        return ""

    def lintKafkaIngestion(self, store: Datastore, storeTree: ValidationTree):
        """Kafka ingestions can only be single dataset. Each dataset is published on a different topic."""
        if not isinstance(store.cmd, KafkaIngestion):
            storeTree.addRaw(UnsupportedIngestionType(store, self.graph.platform, ProblemSeverity.ERROR))
        else:
            cmdTree: ValidationTree = storeTree.addSubTree(store.cmd)
            if store.cmd.singleOrMultiDatasetIngestion is None:
                cmdTree.addRaw(ObjectMissing(store, IngestionConsistencyType, ProblemSeverity.ERROR))
            elif store.cmd.singleOrMultiDatasetIngestion != IngestionConsistencyType.SINGLE_DATASET:
                storeTree.addRaw(DatasetConsistencyNotSupported(store, store.cmd.singleOrMultiDatasetIngestion, self.graph.platform, ProblemSeverity.ERROR))

    def lintSQLSnapshotIngestion(self, store: Datastore, storeTree: ValidationTree):
        """SQL snapshot ingestions can be single or multi dataset. Each dataset is ingested from a source table."""
        if not isinstance(store.cmd, SQLSnapshotIngestion):
            storeTree.addRaw(UnsupportedIngestionType(store, self.graph.platform, ProblemSeverity.ERROR))
        else:
            self.lintSQLIngestion(store, storeTree)

    def lintSQLIngestion(self, store: Datastore, storeTree: ValidationTree):
        """SQL ingestions can be single or multi dataset. Each dataset is ingested from a source table."""
        if not isinstance(store.cmd, SQLIngestion):
            storeTree.addRaw(UnsupportedIngestionType(store, self.graph.platform, ProblemSeverity.ERROR))
        else:
            cmdTree: ValidationTree = storeTree.addSubTree(store.cmd)
            # Check the trigger if specified in a cron trigger
            if store.cmd.stepTrigger is None:
                cmdTree.addRaw(ObjectMissing(store, "trigger", ProblemSeverity.ERROR))
            elif not isinstance(store.cmd.stepTrigger, (CronTrigger, ExternallyTriggered)):
                cmdTree.addRaw(ObjectNotSupportedByDataPlatform(store.cmd.stepTrigger, [CronTrigger, ExternallyTriggered], ProblemSeverity.ERROR))
            if store.cmd.singleOrMultiDatasetIngestion is None:
                cmdTree.addRaw(ObjectMissing(store, IngestionConsistencyType, ProblemSeverity.ERROR))
            # Both single and multi dataset are supported for SQL snapshot ingestion
            if store.cmd.credential is None:
                cmdTree.addRaw(ObjectMissing(store.cmd, "credential", ProblemSeverity.ERROR))
            if store.cmd.dataContainer is None:
                cmdTree.addRaw(ObjectMissing(store.cmd, "dataContainer", ProblemSeverity.ERROR))
            elif not isinstance(store.cmd.dataContainer, (PostgresDatabase, MySQLDatabase, OracleDatabase, SQLServerDatabase)):
                cmdTree.addRaw(ObjectNotSupportedByDataPlatform(
                    store.cmd.dataContainer,
                    [PostgresDatabase, MySQLDatabase, OracleDatabase, SQLServerDatabase],
                    ProblemSeverity.ERROR
                ))

    def lintSQLWatermarkSnapshotDeltaIngestion(self, store: Datastore, storeTree: ValidationTree):
        """SQL watermark snapshot delta ingestions can be single or multi dataset. Each dataset is ingested from a source table."""
        if not isinstance(store.cmd, SQLWatermarkSnapshotDeltaIngestion):
            storeTree.addRaw(UnsupportedIngestionType(store, self.graph.platform, ProblemSeverity.ERROR))
        else:
            self.lintSQLIngestion(store, storeTree)

    def lintGraph(self, eco: Ecosystem, credStore: 'CredentialStore', tree: ValidationTree) -> None:
        """This should be called execute graph. This is where the graph is validated and any issues are reported. If there are
        no issues then the graph is executed. Executed here means
        1. Terraform file which creates kafka connect connectors to ingest topics to SQL tables (for Kafka ingestion).
        2. Airflow DAG which has all the needed MERGE jobs. The Jobs in Airflow should be parameterized and the parameters
        would have enough information such as DataStore name, Datasets names. The Ecosystem git clone should be on the local file
        system and the path provided to the DAG. It can then get everything it needs from that.

        The MERGE job are responsible for node type reconciliation such as creating tables/views for staging, merge tables
        and Workspace views. It's also responsible to keep them up to date. This happens before the merge job is run. This seems
        like something that will be done by a DataSurface service as it's pretty standard.

        As validation/linting errors are found then they are added as ValidationProblems to the tree."""

        if not isinstance(self.graph.platform, YellowDataPlatform):
            tree.addRaw(ObjectWrongType(self.graph.platform, YellowDataPlatform, ProblemSeverity.ERROR))
            return

        # Validate ingestion types for all stores
        for storeName in self.graph.storesToIngest:
            store: Datastore = eco.cache_getDatastoreOrThrow(storeName).datastore

            storeTree: ValidationTree = tree.addSubTree(store)

            # If there is a PIP for the datastore, check if there is a compatible PIP for this DataPlatform
            if eco.getPrimaryIngestionPlatformsForDatastore(storeName) is not None:
                pip: Optional[YellowDataPlatform] = self.dp.getCompatiblePIPToUseForDatastore(eco, storeName)
                if pip is None:
                    storeTree.addRaw(ObjectMissing(store, "No compatible PIP found", ProblemSeverity.ERROR))

            if store.cmd is None:
                storeTree.addRaw(ObjectMissing(store, "cmd", ProblemSeverity.ERROR))
                return
            elif isinstance(store.cmd, KafkaIngestion):
                self.lintKafkaIngestion(store, storeTree)
            elif isinstance(store.cmd, SQLSnapshotIngestion):
                self.lintSQLSnapshotIngestion(store, storeTree)
            elif isinstance(store.cmd, SQLWatermarkSnapshotDeltaIngestion):
                self.lintSQLWatermarkSnapshotDeltaIngestion(store, storeTree)
            elif isinstance(store.cmd, DataTransformerOutput):
                # Nothing to do for DataTransformerOutput
                pass
            else:
                storeTree.addRaw(ObjectNotSupportedByDataPlatform(
                    store, [KafkaIngestion, SQLSnapshotIngestion, SQLWatermarkSnapshotDeltaIngestion], ProblemSeverity.ERROR))

        # For the DSGs assigned to the platform, check all Workspace requirements are supported by the platform
        for dsgRoot in self.graph.roots:
            workspace: Workspace = dsgRoot.workspace
            wsTree: ValidationTree = tree.addSubTree(workspace)
            pc: Optional[DataPlatformChooser] = dsgRoot.dsg.platformMD
            dsgTree: ValidationTree = wsTree.addSubTree(dsgRoot.dsg)
            if workspace.dataTransformer:
                dtTree: ValidationTree = wsTree.addSubTree(workspace.dataTransformer)
                dt: DataTransformer = workspace.dataTransformer
                if dt.code is None:
                    dtTree.addRaw(AttributeNotSet("code"))
                if not isinstance(dt.code, PythonRepoCodeArtifact):
                    dtTree.addRaw(ObjectNotSupportedByDataPlatform(dt, [PythonRepoCodeArtifact], ProblemSeverity.ERROR))

                # YellowDataPlatform supports None (sensor-based) or CronTrigger (scheduled) for DataTransformers
                if dt.trigger is not None:
                    if not isinstance(dt.trigger, CronTrigger):
                        dtTree.addRaw(ObjectNotSupportedByDataPlatform(dt.trigger, [CronTrigger], ProblemSeverity.ERROR))
            else:
                if pc is None:
                    dsgTree.addRaw(AttributeNotSet("platformMD"))

            if pc is not None:
                if isinstance(pc, WorkspacePlatformConfig):
                    pcTree: ValidationTree = dsgTree.addSubTree(pc)
                    if self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD1:
                        if pc.retention.milestoningStrategy != DataMilestoningStrategy.LIVE_ONLY:
                            pcTree.addRaw(AttributeValueNotSupported(pc, [DataMilestoningStrategy.LIVE_ONLY.name], ProblemSeverity.ERROR))
                    # batch_milestoned supports LIVE_WITH_FORENSIC_HISTORY and FORENSIC
                else:
                    dsgTree.addRaw(ObjectWrongType(pc, WorkspacePlatformConfig, ProblemSeverity.ERROR))

        # Check CodeArtifacts on DataTransformer nodes are compatible with the PSP on the Ecosystem
        # TODO This is wrong
        for psp in eco.platformServicesProviders:
            for node in self.graph.nodes.values():
                if isinstance(node, DataTransformerNode):
                    if node.workspace.dataTransformer is not None:
                        dt: DataTransformer = node.workspace.dataTransformer
                        dt.code.lint(eco, tree)

    def getScheduleStringForTrigger(self, trigger: StepTrigger) -> str:
        """This returns the schedule string for a trigger"""
        if isinstance(trigger, CronTrigger):
            return trigger.cron
        elif isinstance(trigger, ExternallyTriggered):
            return "None"
        else:
            raise ValueError(f"Unsupported trigger type: {type(trigger)}")

    def getIngestionJobLimits(self, storeName: str, datasetName: Optional[str] = None) -> K8sIngestionHint:
        # Default job resource limits
        default_ingestion_hint: K8sIngestionHint = K8sIngestionHint(
            storeName=self.dp.name,
            resourceLimits=K8sResourceLimits(
                requested_memory=StorageRequirement("256M"),
                limits_memory=StorageRequirement("1G"),
                requested_cpu=0.1,
                limits_cpu=0.5
            )
        )
        rc: K8sIngestionHint = default_ingestion_hint
        assert self.dp.psp is not None
        user_ingestion_hint: Optional[PlatformIngestionHint] = self.dp.psp.getIngestionJobHint(storeName, datasetName)
        if user_ingestion_hint is not None:
            rc = cast(K8sIngestionHint, user_ingestion_hint)
        return rc

    def populateDAGConfigurations(self, eco: Ecosystem, issueTree: ValidationTree) -> None:
        """Populate the database with ingestion stream configurations for dynamic DAG factory"""
        from sqlalchemy import text
        from datasurface.platforms.yellow.db_utils import createEngine

        # Get database connection
        user, password = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.mergeRW_Credential)
        engine = createEngine(self.dp.psp.mergeStore, user, password)

        # Build the ingestion_streams context from the graph
        ingestion_streams: list[dict[str, Any]] = []

        # For every ingestion node, create a stream configuration and insert it into the database
        for node in self.graph.nodes.values():
            if not isinstance(node, IngestionNode):
                continue

            storeName = node.storeName

            try:
                storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(storeName)
                store: Datastore = storeEntry.datastore
                store.cmd = self.dp.getEffectiveCMDForDatastore(eco, store)

                if isinstance(store.cmd, (KafkaIngestion, SQLSnapshotIngestion, SQLWatermarkSnapshotDeltaIngestion)):
                    # Determine if this is single or multi dataset ingestion
                    is_single_dataset = store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET

                    assert store.cmd.stepTrigger is not None
                    if is_single_dataset:
                        # For single dataset, create a separate entry for each dataset
                        for dataset in store.datasets.values():
                            job_hint: K8sIngestionHint = self.getIngestionJobLimits(storeName, dataset.name)

                            stream_key = f"{storeName}_{dataset.name}"
                            stream_config = {
                                "stream_key": stream_key,
                                "priority": node.priority.priority.value,
                                "single_dataset": True,
                                "datasets": [dataset.name],
                                "store_name": storeName,
                                "dataset_name": dataset.name,
                                "ingestion_type": IngestionType.KAFKA.value if isinstance(store.cmd, KafkaIngestion) else IngestionType.SQL_SOURCE.value,
                                "schedule_string": self.getScheduleStringForTrigger(store.cmd.stepTrigger),
                                "job_limits": job_hint.to_k8s_json()
                            }

                            # Add credential information for this stream
                            if stream_config["ingestion_type"] == IngestionType.KAFKA.value:
                                stream_config["kafka_connect_credential_secret_name"] = K8sUtils.to_k8s_name(
                                    self.dp.psp.connectCredentials.name)
                            elif stream_config["ingestion_type"] == IngestionType.SQL_SOURCE.value:
                                # For SQL snapshot ingestion, we need the source database credentials
                                if isinstance(store.cmd, SQLSnapshotIngestion) and store.cmd.credential is not None:
                                    stream_config["source_credential_secret_name"] = K8sUtils.to_k8s_name(store.cmd.credential.name)

                            ingestion_streams.append(stream_config)
                    else:
                        # For multi dataset, create one entry for the entire store
                        job_hint: K8sIngestionHint = self.getIngestionJobLimits(storeName)
                        stream_config = {
                            "stream_key": storeName,
                            "priority": node.priority.priority.value,
                            "single_dataset": False,
                            "datasets": [dataset.name for dataset in store.datasets.values()],
                            "store_name": storeName,
                            "ingestion_type": IngestionType.KAFKA.value if isinstance(store.cmd, KafkaIngestion) else IngestionType.SQL_SOURCE.value,
                            "schedule_string": self.getScheduleStringForTrigger(store.cmd.stepTrigger),
                            "job_limits": job_hint.to_k8s_json()
                        }

                        # Add credential information for this stream
                        if stream_config["ingestion_type"] == IngestionType.KAFKA.value:
                            stream_config["kafka_connect_credential_secret_name"] = K8sUtils.to_k8s_name(
                                self.dp.psp.connectCredentials.name)
                        elif stream_config["ingestion_type"] == IngestionType.SQL_SOURCE.value:
                            # For SQL snapshot ingestion, we need the source database credentials
                            if isinstance(store.cmd, SQLSnapshotIngestion) and store.cmd.credential is not None:
                                stream_config["source_credential_secret_name"] = K8sUtils.to_k8s_name(store.cmd.credential.name)

                        ingestion_streams.append(stream_config)
                elif isinstance(store.cmd, DataTransformerOutput):
                    # DataTransformerOutput doesn't need Kafka infrastructure
                    continue
                else:
                    issueTree.addRaw(ObjectNotSupportedByDataPlatform(
                        store, [KafkaIngestion, SQLSnapshotIngestion, SQLWatermarkSnapshotDeltaIngestion], ProblemSeverity.ERROR))
                    continue

            except ObjectDoesntExistException:
                issueTree.addRaw(UnknownObjectReference(storeName, ProblemSeverity.ERROR))
                continue

        try:
            gitRepo: GitHubRepository = cast(GitHubRepository, eco.liveRepo)

            # Extract git repository owner and name from the full repository name
            git_repo_parts = gitRepo.repositoryName.split('/')
            if len(git_repo_parts) != 2:
                raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
            git_repo_owner, git_repo_name = git_repo_parts

            # Common context for all streams (platform-level configuration)
            common_context: dict[str, Any] = self.dp.psp.yp_assm.createYAMLContext(eco)
            merge_db_driver, merge_db_query = getDriverNameAndQueryForDataContainer(self.dp.psp.mergeStore)
            common_context.update(
                {
                    "namespace_name": self.dp.psp.yp_assm.namespace,
                    "platform_name": K8sUtils.to_k8s_name(self.dp.name),
                    "original_platform_name": self.dp.name,  # Original platform name for job execution
                    "ecosystem_name": eco.name,  # Original ecosystem name
                    "ecosystem_k8s_name": K8sUtils.to_k8s_name(eco.name),  # K8s-safe ecosystem name for volume claims
                    "merge_db_driver": merge_db_driver,
                    "merge_db_database": self.dp.psp.mergeStore.databaseName,
                    "merge_db_credential_secret_name": K8sUtils.to_k8s_name(self.dp.psp.mergeRW_Credential.name),
                    "git_credential_secret_name": K8sUtils.to_k8s_name(self.dp.psp.gitCredential.name),
                    "git_credential_name": self.dp.psp.gitCredential.name,  # Original credential name for job parameter
                    "datasurface_docker_image": self.dp.psp.datasurfaceDockerImage,
                    "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                    "git_repo_branch": gitRepo.branchName,
                    "git_repo_name": gitRepo.repositoryName,
                    "git_repo_owner": git_repo_owner,
                    "git_repo_repo_name": git_repo_name,
                    # Git cache configuration variables
                    "pv_storage_class": self.dp.psp.pv_storage_class,
                }
            )
            if isinstance(self.dp.psp.mergeStore, HostPortSQLDatabase):
                common_context["merge_db_hostname"] = self.dp.psp.mergeStore.hostPortPair.hostName
                common_context["merge_db_port"] = self.dp.psp.mergeStore.hostPortPair.port
            else:
                raise ValueError(f"Unsupported merge store type: {type(self.dp.psp.mergeStore)}")
            if merge_db_query:
                common_context["merge_db_query"] = merge_db_query

            # Get table name
            table_name = self.dp.getPhysDAGTableName()

            # Store configurations in database
            with engine.begin() as connection:
                # First, delete all existing records for this platform to ensure clean state
                connection.execute(text(f"DELETE FROM {table_name}"))
                logger.info("Cleared existing DAG configurations",
                            table_name=table_name,
                            platform_name=self.dp.name)

                # Then insert all current configurations
                for stream in ingestion_streams:
                    # Create complete context for this stream
                    stream_context = common_context.copy()
                    stream_context.update(stream)  # Add stream properties directly to context

                    # Convert to JSON
                    config_json = json.dumps(stream_context)

                    # Insert the new configuration
                    connection.execute(text(f"""
                        INSERT INTO {table_name} (stream_key, config_json, status, created_at, updated_at)
                        VALUES (:stream_key, :config_json, 'active', NOW(), NOW())
                    """), {
                        "stream_key": stream["stream_key"],
                        "config_json": config_json
                    })

            logger.info("Populated ingestion stream configurations",
                        table_name=table_name,
                        platform_name=self.dp.name,
                        stream_count=len(ingestion_streams))

        except Exception as e:
            issueTree.addRaw(UnexpectedExceptionProblem(e))

    def populateDataTransformerConfigurations(self, eco: Ecosystem, issueTree: ValidationTree) -> None:
        """Populate the database with DataTransformer configurations for dynamic DAG factory"""
        from sqlalchemy import text
        from datasurface.platforms.yellow.db_utils import createEngine

        # Get database connection
        user, password = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.mergeRW_Credential)
        engine = createEngine(self.dp.psp.mergeStore, user, password)

        default_transformer_hint: K8sDataTransformerHint = K8sDataTransformerHint(
            workspaceName=self.dp.name,
            resourceLimits=K8sResourceLimits(
                requested_memory=StorageRequirement("512M"),
                limits_memory=StorageRequirement("2G"),
                requested_cpu=0.2,
                limits_cpu=1.0
            )
        )

        # Build the DataTransformer configurations from the graph
        datatransformer_configs: list[dict[str, Any]] = []

        for node in self.graph.nodes.values():
            if not isinstance(node, DataTransformerNode):
                continue

            workspaceName = node.workspace.name
            workspace = self.graph.eco.cache_getWorkspaceOrThrow(workspaceName).workspace

            # Only process workspaces that have a DataTransformer
            if workspace.dataTransformer is not None:
                try:
                    # Get the output datastore
                    outputDatastore = workspace.dataTransformer.outputDatastore

                    # Build list of input DAG IDs that this DataTransformer depends on
                    input_dag_ids: list[str] = []
                    input_dataset_list: list[str] = []

                    # Get all datasets used by this workspace (inputs to the DataTransformer)
                    for dsg in workspace.dsgs.values():
                        for sink in dsg.sinks.values():
                            # Map to the ingestion DAG ID using the same logic as populateDAGConfigurations
                            storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(sink.storeName)
                            store: Datastore = storeEntry.datastore

                            # Determine DAG ID based on store type
                            if isinstance(store.cmd, (KafkaIngestion, SQLSnapshotIngestion)):
                                # Regular ingestion store
                                is_single_dataset = store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET

                                # Use the same stream_key logic as the ingestion factory
                                if is_single_dataset:
                                    ydu: YellowDatasetUtilities = YellowDatasetUtilities(eco, self.dp.psp.credStore, self.dp, store, sink.datasetName)
                                    stream_key = ydu.getIngestionStreamKey()
                                else:
                                    ydu: YellowDatasetUtilities = YellowDatasetUtilities(eco, self.dp.psp.credStore, self.dp, store)
                                    stream_key = ydu.getIngestionStreamKey()

                                # Regular ingestion DAG naming pattern
                                dag_id = f"{K8sUtils.to_k8s_name(self.dp.name)}__{stream_key}_ingestion"

                            elif isinstance(store.cmd, DataTransformerOutput):
                                # DataTransformer output store - use dt_ingestion naming pattern
                                dag_id = f"{K8sUtils.to_k8s_name(self.dp.name)}__{sink.storeName}_dt_ingestion"

                            else:
                                # Unsupported store type, skip
                                continue

                            if dag_id not in input_dag_ids:
                                input_dag_ids.append(dag_id)

                            input_dataset_list.append(f"{sink.storeName}#{sink.datasetName}")

                    # Get output dataset list
                    output_dataset_list: list[str] = [dataset.name for dataset in outputDatastore.datasets.values()]

                    user_hint: Optional[PlatformDataTransformerHint] = self.dp.psp.getDataTransformerJobHint(workspaceName)
                    if user_hint is not None:
                        job_hint = cast(K8sDataTransformerHint, user_hint)
                    else:
                        job_hint = default_transformer_hint

                    output_job_hint: K8sIngestionHint = self.getIngestionJobLimits(outputDatastore.name)

                    # Create the configuration
                    dt_config = {
                        "workspace_name": workspaceName,
                        "output_datastore_name": outputDatastore.name,
                        "input_dag_ids": input_dag_ids,
                        "priority": node.priority.priority.value,
                        "input_dataset_list": input_dataset_list,
                        "output_dataset_list": output_dataset_list,
                        "job_limits": job_hint.to_k8s_json(),
                        "output_job_limits": output_job_hint.to_k8s_json()
                    }

                    assert isinstance(workspace.dataTransformer.code, PythonRepoCodeArtifact)
                    if workspace.dataTransformer.code.repo.credential is not None:
                        dt_config["git_credential_secret_name"] = K8sUtils.to_k8s_name(workspace.dataTransformer.code.repo.credential.name)

                    # Add schedule information if DataTransformer has a trigger
                    if workspace.dataTransformer.trigger is not None:
                        if isinstance(workspace.dataTransformer.trigger, CronTrigger):
                            dt_config["schedule_string"] = workspace.dataTransformer.trigger.cron
                        else:
                            # This shouldn't happen due to linting, but handle gracefully
                            dt_config["schedule_string"] = None
                    # If no trigger, omit schedule_string (sensor-based mode)

                    datatransformer_configs.append(dt_config)

                except Exception as e:
                    issueTree.addRaw(UnexpectedExceptionProblem(e))
                    continue

        try:
            gitRepo: GitHubRepository = cast(GitHubRepository, eco.liveRepo)

            # Extract git repository owner and name from the full repository name
            git_repo_parts = gitRepo.repositoryName.split('/')
            if len(git_repo_parts) != 2:
                raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
            git_repo_owner, git_repo_name = git_repo_parts

            # Common context for all DataTransformers (platform-level configuration)
            common_context: dict[str, Any] = self.dp.psp.yp_assm.createYAMLContext(eco)
            merge_db_driver, merge_db_query = getDriverNameAndQueryForDataContainer(self.dp.psp.mergeStore)
            common_context.update(
                {
                    "platform_name": K8sUtils.to_k8s_name(self.dp.name),
                    "original_platform_name": self.dp.name,  # Original platform name for job execution
                    "ecosystem_name": eco.name,  # Original ecosystem name
                    "ecosystem_k8s_name": K8sUtils.to_k8s_name(eco.name),  # K8s-safe ecosystem name for volume claims
                    "merge_db_driver": merge_db_driver,
                    "merge_db_database": self.dp.psp.mergeStore.databaseName,
                    "merge_db_credential_secret_name": K8sUtils.to_k8s_name(self.dp.psp.mergeRW_Credential.name),
                    "git_credential_secret_name": K8sUtils.to_k8s_name(self.dp.psp.gitCredential.name),
                    "git_credential_name": self.dp.psp.gitCredential.name,  # Original credential name for job parameter
                    "datasurface_docker_image": self.dp.psp.datasurfaceDockerImage,
                    "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                    "git_repo_branch": gitRepo.branchName,
                    "git_repo_name": gitRepo.repositoryName,
                    "git_repo_owner": git_repo_owner,
                    "git_repo_repo_name": git_repo_name,
                    # Git cache configuration variables
                    "pv_storage_class": self.dp.psp.pv_storage_class,
                }
            )
            if isinstance(self.dp.psp.mergeStore, HostPortSQLDatabase):
                common_context["merge_db_hostname"] = self.dp.psp.mergeStore.hostPortPair.hostName
                common_context["merge_db_port"] = self.dp.psp.mergeStore.hostPortPair.port
            else:
                raise ValueError(f"Unsupported merge store type: {type(self.dp.psp.mergeStore)}")
            if merge_db_query:
                common_context["merge_db_query"] = merge_db_query

            # Get table name
            table_name = self.dp.getPhysDataTransformerTableName()

            # Store configurations in database
            with engine.begin() as connection:
                # First, delete all existing records for this platform to ensure clean state
                connection.execute(text(f"DELETE FROM {table_name}"))
                logger.info("Cleared existing DataTransformer configurations",
                            table_name=table_name,
                            platform_name=self.dp.name)

                # Then insert all current configurations
                for dt_config in datatransformer_configs:
                    # Create complete context for this DataTransformer
                    dt_context = common_context.copy()
                    dt_context.update(dt_config)  # Add DataTransformer properties directly to context

                    # Convert to JSON
                    config_json = json.dumps(dt_context)

                    # Insert the new configuration
                    connection.execute(text(f"""
                        INSERT INTO {table_name} (workspace_name, config_json, status, created_at, updated_at)
                        VALUES (:workspace_name, :config_json, 'active', NOW(), NOW())
                    """), {
                        "workspace_name": dt_config["workspace_name"],
                        "config_json": config_json
                    })

            logger.info("Populated DataTransformer configurations",
                        table_name=table_name,
                        platform_name=self.dp.name,
                        config_count=len(datatransformer_configs))

        except Exception as e:
            issueTree.addRaw(UnexpectedExceptionProblem(e))

    def renderGraph(self, credStore: 'CredentialStore', issueTree: ValidationTree) -> dict[str, str]:
        """This is called by the RenderEngine to instruct a DataPlatform to render the
        intention graph that it manages. For this platform it returns a dictionary containing a terraform
        file which configures all the kafka connect sink connectors to copy datastores using
        kafka ingestion capture meta data to SQL staging tables. It also populates the database
        with ingestion stream configurations for the factory DAG to use."""
        # First create the terraform file
        terraform_code: str = self.createTerraformForAllIngestedNodes(self.graph.eco, issueTree)

        # Populate database with ingestion stream configurations
        self.populateDAGConfigurations(self.graph.eco, issueTree)

        # Populate database with DataTransformer configurations
        self.populateDataTransformerConfigurations(self.graph.eco, issueTree)

        # Generate comprehensive secrets documentation
        secrets_documentation: str = self.generateSecretsDocumentation(self.graph.eco, issueTree)

        # Generate operational network policy with current database ports
        network_policy: str = self.generateOperationalNetworkPolicy(self.graph.eco, issueTree)

        # Return terraform, secrets documentation, and network policy
        result: dict[str, str] = {
            "terraform_code": terraform_code,
            f"{self.dp.name}-secrets.md": secrets_documentation,
            f"{self.dp.name}-operational-network-policy.yaml": network_policy
        }
        return result

    def generateSecretsDocumentation(self, eco: Ecosystem, issueTree: ValidationTree) -> str:
        """Generate comprehensive documentation listing all required Kubernetes secrets for this platform"""

        # Collect all unique credentials needed
        secrets_info: dict[str, dict[str, Any]] = {}

        # Platform-level credentials (always required)
        platform_secrets = [
            {
                'credential': self.dp.psp.mergeRW_Credential,
                'purpose': 'SQL Read/Write database access for merge store',
                'k8s_name': K8sUtils.to_k8s_name(self.dp.psp.mergeRW_Credential.name),
                'category': 'Platform Core'
            },
            {
                'credential': self.dp.psp.gitCredential,
                'purpose': 'Git repository access for ecosystem model',
                'k8s_name': K8sUtils.to_k8s_name(self.dp.psp.gitCredential.name),
                'category': 'Platform Core'
            },
            {
                'credential': self.dp.psp.connectCredentials,
                'purpose': 'Kafka Connect cluster API access',
                'k8s_name': K8sUtils.to_k8s_name(self.dp.psp.connectCredentials.name),
                'category': 'Platform Core'
            }
        ]

        # Add platform secrets
        for secret_info in platform_secrets:
            secrets_info[secret_info['k8s_name']] = secret_info

        # Ingestion-specific credentials
        for storeName in self.graph.storesToIngest:
            try:
                storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(storeName)
                store: Datastore = storeEntry.datastore

                if isinstance(store.cmd, IngestionMetadata) and store.cmd.credential is not None:
                    k8s_name = K8sUtils.to_k8s_name(store.cmd.credential.name)
                    if k8s_name not in secrets_info:
                        secrets_info[k8s_name] = {
                            'credential': store.cmd.credential,
                            'purpose': f'Source database access for {storeName} SQL snapshot ingestion',
                            'k8s_name': k8s_name,
                            'category': 'Ingestion Source',
                            'stores': [storeName]
                        }
                    else:
                        # Same credential used for multiple stores
                        if 'stores' not in secrets_info[k8s_name]:
                            secrets_info[k8s_name]['stores'] = []
                        secrets_info[k8s_name]['stores'].append(storeName)
                        secrets_info[k8s_name]['purpose'] = f"Source database access for {', '.join(secrets_info[k8s_name]['stores'])} SQL snapshot ingestion"

            except ObjectDoesntExistException:
                issueTree.addRaw(UnknownObjectReference(storeName, ProblemSeverity.ERROR))
                continue

        # DataTransformer repository credentials
        for dsgRoot in self.graph.roots:
            workspace: Workspace = dsgRoot.workspace
            workspaceName = workspace.name
            if workspace.dataTransformer is not None:
                assert isinstance(workspace.dataTransformer.code, PythonRepoCodeArtifact)
                if workspace.dataTransformer.code.repo.credential is not None:
                    k8s_name = K8sUtils.to_k8s_name(workspace.dataTransformer.code.repo.credential.name)
                    if k8s_name not in secrets_info:
                        secrets_info[k8s_name] = {
                            'credential': workspace.dataTransformer.code.repo.credential,
                            'purpose': f'Git repository access for {workspaceName} DataTransformer code',
                            'k8s_name': k8s_name,
                            'category': 'DataTransformer Repository',
                            'workspaces': [workspaceName],
                            'repo': workspace.dataTransformer.code.repo.repositoryName
                        }
                    else:
                        # Same credential used for multiple DataTransformers
                        if 'workspaces' not in secrets_info[k8s_name]:
                            secrets_info[k8s_name]['workspaces'] = []
                        secrets_info[k8s_name]['workspaces'].append(workspaceName)
                        secrets_info[k8s_name]['purpose'] = f"Git repository access for {', '.join(secrets_info[k8s_name]['workspaces'])} DataTransformer code"

        # Generate markdown documentation
        gitRepo: GitHubRepository = cast(GitHubRepository, eco.liveRepo)

        markdown_lines = [
            f"# Kubernetes Secrets for {self.dp.name}",
            "",
            f"This document lists all Kubernetes secrets required for the `{self.dp.name}` YellowDataPlatform deployment.",
            "",
            f"**Platform:** {self.dp.name}",
            f"**Namespace:** {self.dp.psp.yp_assm.namespace}",
            f"**Ecosystem Repository:** {gitRepo.repositoryName}",
            "",
            "## Overview",
            "",
            f"Total secrets required: **{len(secrets_info)}**",
            "",
            "| Category | Count |",
            "| --- | --- |"
        ]

        # Count by category
        category_counts = {}
        for secret in secrets_info.values():
            category = secret['category']
            category_counts[category] = category_counts.get(category, 0) + 1

        for category, count in sorted(category_counts.items()):
            markdown_lines.append(f"| {category} | {count} |")

        markdown_lines.extend([
            "",
            "## Required Secrets",
            ""
        ])

        # Group secrets by category
        by_category = {}
        for secret in secrets_info.values():
            category = secret['category']
            if category not in by_category:
                by_category[category] = []
            by_category[category].append(secret)

        # Generate sections for each category
        for category in sorted(by_category.keys()):
            markdown_lines.extend([
                f"### {category}",
                ""
            ])

            for secret in sorted(by_category[category], key=lambda x: x['k8s_name']):
                credential = secret['credential']
                k8s_name = secret['k8s_name']
                purpose = secret['purpose']

                markdown_lines.extend([
                    f"#### `{k8s_name}`",
                    "",
                    f"**Purpose:** {purpose}",
                    f"**Type:** {credential.credentialType.name.lower()}",
                    f"**Original Name:** `{credential.name}`",
                    ""
                ])

                # Add specific details based on category
                if category == "DataTransformer Repository" and 'repo' in secret:
                    markdown_lines.extend([
                        f"**Repository:** `{secret['repo']}`",
                        f"**Workspaces:** {', '.join(secret.get('workspaces', []))}",
                        ""
                    ])
                elif category == "Ingestion Source" and 'stores' in secret:
                    markdown_lines.extend([
                        f"**Datastores:** {', '.join(secret['stores'])}",
                        ""
                    ])

                # Generate kubectl command based on credential type
                if credential.credentialType == CredentialType.USER_PASSWORD:
                    markdown_lines.extend([
                        "**Create Command:**",
                        "```bash",
                        f"kubectl create secret generic {k8s_name} \\",
                        "  --from-literal=POSTGRES_USER='your-username' \\",
                        "  --from-literal=POSTGRES_PASSWORD='your-password' \\",
                        f"  --namespace {self.dp.psp.yp_assm.namespace}",
                        "```",
                        ""
                    ])
                elif credential.credentialType == CredentialType.API_TOKEN:
                    markdown_lines.extend([
                        "**Create Command:**",
                        "```bash",
                        f"kubectl create secret generic {k8s_name} \\",
                        "  --from-literal=token='your-api-token' \\",
                        f"  --namespace {self.dp.psp.yp_assm.namespace}",
                        "```",
                        ""
                    ])
                elif credential.credentialType == CredentialType.API_KEY_PAIR:
                    markdown_lines.extend([
                        "**Create Command:**",
                        "```bash",
                        f"kubectl create secret generic {k8s_name} \\",
                        "  --from-literal=api_key='your-api-key' \\",
                        "  --from-literal=api_secret='your-api-secret' \\",
                        f"  --namespace {self.dp.psp.yp_assm.namespace}",
                        "```",
                        ""
                    ])

        markdown_lines.extend([
            "## Verification",
            "",
            "To verify all secrets are created correctly:",
            "",
            "```bash",
            "# List all secrets in the namespace",
            f"kubectl get secrets -n {self.dp.psp.yp_assm.namespace}",
            "",
            "# Verify specific secrets exist",
        ])

        for k8s_name in sorted(secrets_info.keys()):
            markdown_lines.append(f"kubectl get secret {k8s_name} -n {self.dp.psp.yp_assm.namespace}")

        markdown_lines.extend([
            "```",
            "",
            "## Notes",
            "",
            "- **Security:** Never commit actual secret values to version control",
            "- **Rotation:** Plan for regular credential rotation",
            "- **Access:** Use RBAC to limit secret access to necessary pods only",
            "- **Backup:** Consider backing up secret definitions (without values) for disaster recovery",
            "",
            f"Generated automatically by DataSurface for platform `{self.dp.name}`"
        ])

        return "\n".join(markdown_lines)

    def generateOperationalPorts(self, eco: Ecosystem) -> set[int]:
        """Generate required database ports for operational use by scanning this platform's graph"""
        required_ports = self.graph.generatePorts()

        # Add platform-specific ports (merge store)
        required_ports.update(self.graph.getPortsForDataContainer(self.dp.psp.mergeStore))

        return required_ports

    def generateOperationalNetworkPolicy(self, eco: Ecosystem, issueTree: ValidationTree) -> str:
        """Generate operational network policy YAML based on current ecosystem database ports"""
        from datetime import datetime

        # Scan ecosystem model for all database ports that need network access (operational mode)
        required_ports: set[int] = self.generateOperationalPorts(eco)

        # Generate YAML content
        yaml_lines = [
            "# Operational Network Policy for External Database Access",
            f"# Generated for platform: {self.dp.name}",
            f"# Namespace: {self.dp.psp.yp_assm.namespace}",
            f"# Generated at: {datetime.now().isoformat()}",
            "#",
            "# Apply this network policy to allow pods to connect to external databases",
            "# discovered from the current ecosystem model operational state.",
            "# Includes ports from:",
            "#   - Datastores being ingested by this platform",
            "#   - Workspaces serviced by this platform",
            "#   - Platform merge store",
            "#",
            f"# Required database ports: {sorted(list(required_ports))}",
            "",
            "apiVersion: networking.k8s.io/v1",
            "kind: NetworkPolicy",
            "metadata:",
            f"  name: {K8sUtils.to_k8s_name(self.dp.name)}-operational-database-access",
            f"  namespace: {self.dp.psp.yp_assm.namespace}",
            "  labels:",
            f"    app: {K8sUtils.to_k8s_name(self.dp.name)}",
            "    component: operational-network-policy",
            "spec:",
            "  podSelector: {}",
            "  policyTypes:",
            "    - Egress",
            "  egress:",
            "    # Allow all outbound traffic (needed for external database connections)",
            "    - to:",
            "        - ipBlock:",
            "            cidr: 0.0.0.0/0",
            "    # Allow DNS resolution",
            "    - to: []",
            "      ports:",
            "        - protocol: UDP",
            "          port: 53",
            "        - protocol: TCP",
            "          port: 53",
            "    # Allow database ports discovered from operational ecosystem state",
            "    - to:",
            "        - ipBlock:",
            "            cidr: 0.0.0.0/0",
            "      ports:"
        ]

        # Add each discovered port
        for port in sorted(list(required_ports)):
            yaml_lines.extend([
                "        - protocol: TCP",
                f"          port: {port}"
            ])

        return "\n".join(yaml_lines)


class KafkaConnectCluster(DataContainer):
    def __init__(self, name: str, locs: set[LocationKey], restAPIUrlString: str, kafkaServer: KafkaServer, caCert: Optional[Credential] = None) -> None:
        super().__init__(name, locs)
        self.restAPIUrlString: str = restAPIUrlString
        self.kafkaServer: KafkaServer = kafkaServer
        self.caCertificate: Optional[Credential] = caCert

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
                "restAPIUrlString": self.restAPIUrlString,
                "kafkaServer": self.kafkaServer.to_json()
            }
        )
        if (self.caCertificate):
            rc.update(
                {
                    "caCertificate": self.caCertificate.to_json()
                }
            )
        return rc

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        """This should validate the data container and its associated parts but it cannot validate the usage of the DataPlatform
        as the graph must be generated for that to happen. The lintGraph method on the KPSGraphHandler does
        that as well as generating the terraform, airflow and other artifacts."""
        super().lint(eco, tree)
        self.kafkaServer.lint(eco, tree.addSubTree(self.kafkaServer))

    def getNamingAdapter(self) -> Optional['DataContainerNamingMapper']:
        """Returns None as naming is handled by the platform."""
        return None


class YellowMilestoneStrategy(Enum):
    SCD1 = "live_only"
    """Only the latest version of each record should be retained (SCD1).

    Notes:
    - Strict SCD1 implies deletions are applied so that the target matches the source's current state.
    - If the upstream feed lacks delete events and you do not reconcile deletes via snapshots, this behaves as
      upsert-only (insert/update only). Be explicit about that contract in ingestion settings.
    """
    SCD2 = "batch_milestoned"
    """Full history with versioned rows (SCD2). Again, the ability to capture DELETEs is key for full fidelity.
    Versioning is done using batch ids and a live sentinel batch.
    """


class YellowGenericDataPlatform(DataPlatform['YellowPlatformServiceProvider']):
    def __init__(self, name: str, doc: Documentation, executor: DataPlatformExecutor) -> None:
        super().__init__(name, doc, executor)

    @abstractmethod
    def createPlatformTables(self, eco: Ecosystem, mergeEngine: Engine, inspector: Inspector) -> None:
        """This creates the tables for the data platform in the merge engine."""
        raise NotImplementedError("This is an abstract method")


class K8sImagePullPolicy(Enum):
    """This is the image pull policy for the kubernetes pods."""
    ALWAYS = "Always"
    IF_NOT_PRESENT = "IfNotPresent"
    NEVER = "Never"


class YellowPlatformServiceProvider(PlatformServicesProvider):
    """This provides basic kubernetes services to the YellowDataPlatforms it manages. It provides
    a kubernetes namespace, an airflow instance and a merge store."""

    def __init__(self, name: str, locs: set[LocationKey], doc: Documentation,
                 gitCredential: Credential,
                 connectCredentials: Credential,
                 merge_datacontainer: SQLDatabase,
                 mergeRW_Credential: Credential,
                 yp_assembly: K8sAssemblyFactory,
                 kafkaConnectName: str = "kafka-connect",
                 kafkaClusterName: str = "kafka",
                 airflowName: str = "airflow",
                 dataPlatforms: list['DataPlatform'] = [],
                 datasurfaceDockerImage: str = "datasurface/datasurface:latest",
                 pv_storage_class: str = "standard",
                 image_pull_policy: K8sImagePullPolicy = K8sImagePullPolicy.IF_NOT_PRESENT,
                 hints: dict[str, PlatformRuntimeHint] = dict()):
        super().__init__(
            name,
            locs,
            KubernetesEnvVarsCredentialStore(
                name=f"{name}-cred-store", locs=locs
                ),
            dataPlatforms=dataPlatforms,
            hints=hints
            )

        # TODO: remove this once we have a way to set the git cache nfs server node
        # git_cache_enabled = True
        self.yp_assm: K8sAssemblyFactory = yp_assembly
        self.gitCredential: Credential = gitCredential
        self.mergeRW_Credential: Credential = mergeRW_Credential
        self.connectCredentials: Credential = connectCredentials
        self.kafkaConnectName: str = kafkaConnectName
        self.kafkaClusterName: str = kafkaClusterName
        self.airflowName: str = airflowName
        self.datasurfaceDockerImage: str = datasurfaceDockerImage
        self.pv_storage_class: str = pv_storage_class
        self.mergeStore: SQLDatabase = merge_datacontainer
        self.namingMapper: DataContainerNamingMapper = self.mergeStore.getNamingAdapter()
        self.image_pull_policy: K8sImagePullPolicy = image_pull_policy
        self.kafkaConnectCluster = KafkaConnectCluster(
            name=kafkaConnectName,
            locs=self.locs,
            restAPIUrlString=f"http://{kafkaConnectName}-service.{self.yp_assm.namespace}.svc.cluster.local:8083",
            kafkaServer=KafkaServer(
                name=kafkaClusterName,
                locs=self.locs,
                bootstrapServers=HostPortPairList([HostPortPair(f"{kafkaClusterName}-service.{self.yp_assm.namespace}.svc.cluster.local", 9092)])
            )
        )

    def getGitCachePVRootName(self) -> str:
        """This returns the root name for the git cache PV."""
        return f"{K8sUtils.to_k8s_name(self.name)}-git-model-cache"

    def getGitCachePVC(self) -> str:
        return f"{self.getGitCachePVRootName()}-pvc"

    def getGitCachePV(self) -> str:
        return f"{self.getGitCachePVRootName()}-pv"

    def getNFSServerPVRootName(self) -> str:
        """This returns the root name for the NFS server PV."""
        return f"{K8sUtils.to_k8s_name(self.name)}-nfs-server"

    def getNFSServerPVC(self) -> str:
        return f"{self.getNFSServerPVRootName()}-pvc"

    def getNFSServerPV(self) -> str:
        return f"{self.getNFSServerPVRootName()}-pv"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
                "namespace": self.yp_assm.namespace,
                "mergeRW_Credential": self.mergeRW_Credential.to_json(),
                "gitCredential": self.gitCredential.to_json(),
                "connectCredentials": self.connectCredentials.to_json(),
                "kafkaConnectName": self.kafkaConnectName,
                "kafkaClusterName": self.kafkaClusterName,
                "mergeStore": self.mergeStore.to_json(),
                "airflowName": self.airflowName,
                "datasurfaceDockerImage": self.datasurfaceDockerImage,
                "pv_storage_class": self.pv_storage_class,
                "image_pull_policy": self.image_pull_policy.value
            }
        )
        return rc

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        """This should validate the platform and its associated parts but it cannot validate the usage of the DataPlatform
        as the graph must be generated for that to happen. The lintGraph method on the KPSGraphHandler does
        that as well as generating the terraform, airflow and other artifacts."""
        super().lint(eco, tree)
        if self.mergeRW_Credential.credentialType != CredentialType.USER_PASSWORD:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.mergeRW_Credential, [CredentialType.USER_PASSWORD]))
        if self.connectCredentials.credentialType != CredentialType.API_TOKEN:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.connectCredentials, [CredentialType.API_TOKEN]))
        if self.gitCredential.credentialType != CredentialType.API_TOKEN:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.gitCredential, [CredentialType.API_TOKEN]))
        self.yp_assm.lint(eco, tree.addSubTree(self.yp_assm))

        # Check the hints are the right type
        for hint in self.hints.values():
            if not isinstance(hint, K8sIngestionHint) and not isinstance(hint, K8sDataTransformerHint):
                tree.addRaw(ObjectNotSupportedByDataPlatform(hint, [K8sIngestionHint, K8sDataTransformerHint], ProblemSeverity.ERROR))

        # check the ecosystem repository is a GitHub repository, we're only supporting GitHub for now
        if not isinstance(eco.liveRepo, GitHubRepository):
            tree.addRaw(ObjectNotSupportedByDataPlatform(eco.liveRepo, [GitHubRepository], ProblemSeverity.ERROR))

        # Check all dataplatforms are generic yellow
        dp: DataPlatform
        for dp in self.dataPlatforms.values():
            if not isinstance(dp, YellowGenericDataPlatform):
                tree.addRaw(ObjectNotSupportedByDataPlatform(dp, [YellowGenericDataPlatform], ProblemSeverity.ERROR))

        self.kafkaConnectCluster.lint(eco, tree.addSubTree(self.kafkaConnectCluster))
        self.mergeStore.lint(eco, tree.addSubTree(self.mergeStore))
        for loc in self.locs:
            loc.lint(tree.addSubTree(loc))
        eco.checkAllRepositoriesInEcosystem(tree, [GitHubRepository])

    def _getKafkaBootstrapServers(self) -> str:
        """Calculate the Kafka bootstrap servers from the created Kafka cluster."""
        return f"{self.kafkaClusterName}-service.{self.yp_assm.namespace}.svc.cluster.local:9092"

    def mergeHandler(self, eco: 'Ecosystem', basePlatformDir: str):
        """This is the merge handler implementation."""
        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)
        dp: DataPlatform
        for dp in self.dataPlatforms.values():

            # Get the platform graph
            platformGraph: PlatformPipelineGraph = graph.roots[dp.name]
            platformGraphHandler: DataPlatformGraphHandler = dp.createGraphHandler(platformGraph)

            # Create the platform directory if it doesn't exist
            os.makedirs(os.path.join(basePlatformDir, dp.name), exist_ok=True)

            tree: ValidationTree = ValidationTree(eco)
            files: dict[str, str] = platformGraphHandler.renderGraph(dp.getCredentialStore(), tree)

            if tree.hasWarnings() or tree.hasErrors():
                tree.printTree()

            if tree.hasErrors():
                raise Exception(f"Error generating platform files for {dp.name}")

            # Write the files to the platform directory
            for name, content in files.items():
                with open(os.path.join(basePlatformDir, dp.name, name), "w") as f:
                    f.write(content)

        # Populate the factroy DAG table with a record per active YellowDataPlatform
        self.populateFactoryDAGConfigurations(eco)

    def populateFactoryDAGConfigurations(self, eco: Ecosystem) -> None:
        """Populate the database with factory DAG configurations for meta-factory creation"""

        # Get database connection
        user, password = self.credStore.getAsUserPassword(self.mergeRW_Credential)
        engine = createEngine(self.mergeStore, user, password)

        try:
            gitRepo: GitHubRepository = cast(GitHubRepository, eco.liveRepo)

            # Extract git repository owner and name from the full repository name
            git_repo_parts = gitRepo.repositoryName.split('/')
            if len(git_repo_parts) != 2:
                raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
            git_repo_owner, git_repo_name = git_repo_parts

            factory_configs: list[dict[str, Any]] = []
            for dpRaw in self.dataPlatforms.values():
                dp: YellowDataPlatform = cast(YellowDataPlatform, dpRaw)
                # Common context for all factory DAGs (platform-level configuration)
                common_context: dict[str, Any] = self.yp_assm.createYAMLContext(eco)
                merge_db_driver, merge_db_query = getDriverNameAndQueryForDataContainer(self.mergeStore)
                common_context.update(
                    {
                        "namespace_name": self.yp_assm.namespace,
                        "platform_name": K8sUtils.to_k8s_name(dp.name),
                        "original_platform_name": dp.name,  # Original platform name for job execution
                        "ecosystem_name": eco.name,  # Original ecosystem name
                        "ecosystem_k8s_name": K8sUtils.to_k8s_name(eco.name),  # K8s-safe ecosystem name for volume claims
                        "merge_db_driver": merge_db_driver,
                        "merge_db_database": self.mergeStore.databaseName,
                        "merge_db_credential_secret_name": K8sUtils.to_k8s_name(self.mergeRW_Credential.name),
                        "git_credential_secret_name": K8sUtils.to_k8s_name(self.gitCredential.name),
                        "git_credential_name": self.gitCredential.name,  # Original credential name for job parameter
                        "datasurface_docker_image": self.datasurfaceDockerImage,
                        "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                        "git_repo_branch": gitRepo.branchName,
                        "git_repo_name": gitRepo.repositoryName,
                        "git_repo_owner": git_repo_owner,
                        "git_repo_repo_name": git_repo_name,
                        # Physical table names for factory DAGs to query
                        "phys_dag_table_name": dp.getPhysDAGTableName(),
                        "phys_datatransformer_table_name": dp.getPhysDataTransformerTableName(),
                        # Git cache configuration variables
                        "pv_storage_class": self.pv_storage_class
                    })
                if isinstance(self.mergeStore, HostPortSQLDatabase):
                    common_context["merge_db_hostname"] = self.mergeStore.hostPortPair.hostName
                    common_context["merge_db_port"] = self.mergeStore.hostPortPair.port
                else:
                    raise ValueError(f"Unsupported merge store type: {type(self.mergeStore)}")
                if merge_db_query:
                    common_context["merge_db_query"] = merge_db_query

                # Factory DAG configurations to create
                factory_configs.append(
                    {
                        "platform_name": dp.name,
                        "factory_type": "platform",
                        "config_json": json.dumps(common_context)
                    })
                factory_configs.append(
                    {
                        "platform_name": dp.name,
                        "factory_type": "datatransformer",
                        "config_json": json.dumps(common_context)
                    }
                )

            # Get table name (use consistent method like other DAG tables)
            table_name = self.getPhysFactoryDAGTableName()

            # Store configurations in database
            with engine.begin() as connection:
                # First, delete all existing records to ensure clean state (same pattern as other methods)
                connection.execute(text(f"DELETE FROM {table_name}"))
                logger.info("Cleared existing factory DAG configurations",
                            table_name=table_name,
                            psp_name=self.name)

                # Then insert all current configurations
                for factory_config in factory_configs:
                    # Insert the new configuration
                    connection.execute(text(f"""
                        INSERT INTO {table_name} (platform_name, factory_type, config_json, status, created_at, updated_at)
                        VALUES (:platform_name, :factory_type, :config_json, 'active', NOW(), NOW())
                    """), factory_config)

            logger.info("Populated factory DAG configurations",
                        table_name=table_name,
                        psp_name=self.name,
                        config_count=len(factory_configs))

        except Exception:
            raise

    def createTemplateContext(self, eco: Ecosystem) -> dict[str, Any]:
        # Prepare template context with all required variables
        gitRepo: GitHubRepository = cast(GitHubRepository, eco.liveRepo)

        # Extract git repository owner and name from the full repository name
        git_repo_parts = gitRepo.repositoryName.split('/')
        if len(git_repo_parts) != 2:
            raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
        git_repo_owner, git_repo_name = git_repo_parts

        context: dict[str, Any] = self.yp_assm.createYAMLContext(eco)
        context.update(
            {
                "psp_name": self.name,
                "psp_k8s_name": K8sUtils.to_k8s_name(self.name),
                "ecosystem_name": eco.name,  # Original ecosystem name
                "ecosystem_k8s_name": K8sUtils.to_k8s_name(eco.name),  # K8s-safe ecosystem name for volume claims
                "datasurface_docker_image": self.datasurfaceDockerImage,
                "git_credential_secret_name": K8sUtils.to_k8s_name(self.gitCredential.name),
                "git_credential_name": self.gitCredential.name,  # Original credential name for job parameter
                "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                "git_repo_branch": gitRepo.branchName,
                "git_repo_name": gitRepo.repositoryName,
                "git_repo_owner": git_repo_owner,
                "git_repo_repo_name": git_repo_name,
                "ingestion_streams": {},  # Empty for bootstrap - no ingestion streams yet
                # Git cache configuration variables
                "pv_storage_class": self.pv_storage_class,
            })
        return context

    def createTemplateContextForDataPlatform(self, eco: Ecosystem, dataPlatform: DataPlatform) -> dict[str, Any]:
        """This creates the template context for a data platform. It's used to generate the kubernetes yaml file for the data platform."""
        context: dict[str, Any] = self.createTemplateContext(eco)
        context.update(
            {
                "platform_name": K8sUtils.to_k8s_name(dataPlatform.name),
                "original_platform_name": dataPlatform.name,
            }
        )
        return context

    def getTableForPSP(self, tableName: str) -> str:
        """This returns the table name for the platform"""
        return f"{self.name}_{tableName}".lower()

    def getPhysFactoryDAGTableName(self) -> str:
        """This returns the name of the Factory DAG table"""
        return self.namingMapper.fmtTVI(self.getTableForPSP("factory_dags"))

    def getFactoryDAGTable(self, engine: Optional[Engine] = None) -> Table:
        """This constructs the sqlalchemy table for Factory DAG configurations."""
        # Use database-specific datetime type - default to TIMESTAMP for compatibility
        if engine is not None and hasattr(engine, 'dialect') and 'mssql' in str(engine.dialect.name):
            from sqlalchemy.types import DATETIME
            datetime_type = DATETIME()
        else:
            datetime_type = TIMESTAMP()
        t: Table = Table(self.getPhysFactoryDAGTableName(), MetaData(),
                         Column("platform_name", sqlalchemy.String(length=255), primary_key=True),
                         Column("factory_type", sqlalchemy.String(length=50), primary_key=True),
                         Column("config_json", sqlalchemy.String(length=8192)),
                         Column("status", sqlalchemy.String(length=50)),
                         Column("created_at", datetime_type),
                         Column("updated_at", datetime_type))
        return t

    @staticmethod
    def to_python_name(name: str) -> str:
        """Convert a name to a valid Python module name."""
        name = name.lower().replace(' ', '_').replace('-', '_')
        name = re.sub(r'[^a-z0-9_]', '', name)
        name = re.sub(r'_+', '_', name)
        return name.strip('_')

    def generateBootstrapArtifacts(self, eco: Ecosystem, ringLevel: int) -> dict[str, str]:
        """This generates a kubernetes yaml file for the data platform using a jinja2 template.
        This doesn't need an intention graph, it's just for boot-strapping.
        Our bootstrap file would be a postgres instance, a kafka cluster, a kafka connect cluster and an airflow instance. It also
        needs to create the DAG for the infrastructure and the factory DAG for dynamic ingestion stream generation."""

        logger.info("Generating bootstrap artifacts",
                    platform_name=self.name,
                    ring_level=ringLevel)
        if ringLevel == 0:
            # Create Jinja2 environment
            env: Environment = Environment(
                loader=PackageLoader('datasurface.platforms.yellow.templates', 'jinja'),
                autoescape=select_autoescape(['html', 'xml'])
            )

            # Load the bootstrap template
            # This defines the kubernetes services for the Yellow environment which the data platforms run inside.
            assembly: Assembly = self.yp_assm.createAssembly(
                mergeRW_Credential=self.mergeRW_Credential,
                mergeDB=self.mergeStore)

            # Load the infrastructure DAG template

            gitRepo: GitHubRepository = cast(GitHubRepository, eco.liveRepo)

            # Extract git repository owner and name from the full repository name
            git_repo_parts = gitRepo.repositoryName.split('/')
            if len(git_repo_parts) != 2:
                raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
            git_repo_owner, git_repo_name = git_repo_parts

            # Prepare template context with all required variables
            context: dict[str, Any] = self.yp_assm.createYAMLContext(eco)
            merge_db_driver, merge_db_query = getDriverNameAndQueryForDataContainer(self.mergeStore)
            context.update(
                {
                    "psp_k8s_name": K8sUtils.to_k8s_name(self.name),
                    "psp_name": self.name,  # Original platform name for job execution
                    "ecosystem_name": eco.name,  # Original ecosystem name
                    "ecosystem_k8s_name": K8sUtils.to_k8s_name(eco.name),  # K8s-safe ecosystem name for volume claims
                    "merge_db_driver": merge_db_driver,
                    "merge_db_database": self.mergeStore.databaseName,
                    "merge_db_credential_secret_name": K8sUtils.to_k8s_name(self.mergeRW_Credential.name),
                    "airflow_name": K8sUtils.to_k8s_name(self.airflowName),
                    "airflow_credential_secret_name": K8sUtils.to_k8s_name(self.mergeRW_Credential.name),  # Airflow uses postgres creds
                    "kafka_cluster_name": K8sUtils.to_k8s_name(self.kafkaClusterName),
                    "kafka_connect_name": K8sUtils.to_k8s_name(self.kafkaConnectName),
                    "kafka_bootstrap_servers": self._getKafkaBootstrapServers(),
                    "datasurface_docker_image": self.datasurfaceDockerImage,
                    "git_credential_secret_name": K8sUtils.to_k8s_name(self.gitCredential.name),
                    "git_credential_name": self.gitCredential.name,  # Original credential name for job parameter
                    "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                    "git_repo_branch": gitRepo.branchName,
                    "git_repo_name": gitRepo.repositoryName,
                    "git_repo_owner": git_repo_owner,
                    "git_repo_repo_name": git_repo_name,
                    "ingestion_streams": {},  # Empty for bootstrap - no ingestion streams yet
                    # Git cache configuration variables
                    "pv_storage_class": self.pv_storage_class,
                    "phys_factory_table_name": self.getPhysFactoryDAGTableName(),  # Factory DAG configuration table
                    "image_pull_policy": self.image_pull_policy.value
                })
            if isinstance(self.mergeStore, HostPortSQLDatabase):
                context["merge_db_hostname"] = self.mergeStore.hostPortPair.hostName
                context["merge_db_port"] = self.mergeStore.hostPortPair.port
            else:
                raise ValueError(f"Unsupported merge store type: {type(self.mergeStore)}")
            if merge_db_query:
                context["merge_db_query"] = merge_db_query

            # Render the templates
            rendered_yaml: str = assembly.generateYaml(env, self.createTemplateContext(eco))

            # PSP level DAG to create factory dags for each dataplatform which in turn
            # create ingestion and transformer DAGs for that dataplatform.
            dag_template: Template = env.get_template('airflow281/infrastructure_dag.py.j2')
            rendered_infrastructure_dag: str = dag_template.render(context)

            #  This now really generates database records for the dataplatforms, ingestion streams and datatransformers
            # in the newly merged model.
            model_merge_template: Template = env.get_template('model_merge_job.yaml.j2')
            rendered_model_merge_job: str = model_merge_template.render(context)

            # This is a yaml file which runs in the environment to create database tables needed
            # and other things required to run the dataplatforms.
            ring1_init_template: Template = env.get_template('ring1_init_job.yaml.j2')
            rendered_ring1_init_job: str = ring1_init_template.render(context)

            # This creates all the Workspace views needed for all the dataplatforms in the model.
            reconcile_views_template: Template = env.get_template('reconcile_views_job.yaml.j2')
            rendered_reconcile_views_job: str = reconcile_views_template.render(context)

            # Return as dictionary with filename as key
            # Factory DAG files removed - now handled dynamically by infrastructure DAG
            return {
                "kubernetes-bootstrap.yaml": rendered_yaml,
                f"{self.to_python_name(self.name)}_infrastructure_dag.py": rendered_infrastructure_dag,
                f"{self.to_python_name(self.name)}_model_merge_job.yaml": rendered_model_merge_job,
                f"{self.to_python_name(self.name)}_ring1_init_job.yaml": rendered_ring1_init_job,
                f"{self.to_python_name(self.name)}_reconcile_views_job.yaml": rendered_reconcile_views_job
            }
        elif ringLevel == 1:
            # Create the airflow dsg table, datatransformer table, and factory DAG table if needed
            mergeUser, mergePassword = self.credStore.getAsUserPassword(self.mergeRW_Credential)
            mergeEngine: Engine = createEngine(self.mergeStore, mergeUser, mergePassword)
            inspector = createInspector(mergeEngine)
            with mergeEngine.begin() as mergeConnection:
                createOrUpdateTable(mergeConnection, inspector, self.getFactoryDAGTable())

            ydp: DataPlatform
            for ydp in self.dataPlatforms.values():
                assert isinstance(ydp, YellowGenericDataPlatform)
                ydp.createPlatformTables(eco, mergeEngine, inspector)
            return {}
        else:
            raise ValueError(f"Invalid ring level {ringLevel} for YellowDataPlatform")


class YellowDataPlatform(YellowGenericDataPlatform):
    """This defines the kubernetes Merge DB starter data platform. It can consume data from sources and write them to a Merge DB based merge store.
      It has the use of a Merge DB database for staging and merge tables as well as Workspace views"""
    def __init__(
            self,
            name: str,
            doc: Documentation,
            milestoneStrategy: YellowMilestoneStrategy = YellowMilestoneStrategy.SCD1,
            stagingBatchesToKeep: int = 50):
        super().__init__(name, doc, YellowPlatformExecutor())
        self.milestoneStrategy: YellowMilestoneStrategy = milestoneStrategy
        self.stagingBatchesToKeep: int = stagingBatchesToKeep

        # Create the required data containers
        # Set logging context for this platform
        set_context(platform=name)

    def createPlatformTables(self, eco: Ecosystem, mergeEngine: Engine, inspector: Inspector) -> None:
        """This creates the tables for the data platform in the merge engine during bootstrapping."""
        with mergeEngine.begin() as mergeConnection:
            createOrUpdateTable(mergeConnection, inspector, self.getAirflowDAGTable())
            createOrUpdateTable(mergeConnection, inspector, self.getDataTransformerDAGTable())

    def getEffectiveCMDForDatastore(self, eco: Ecosystem, store: Datastore) -> CaptureMetaData:
        """If there is a pip for the datastore then construct a new SQLMergeIngestion and return it. Otherwise return the current cmd."""
        pip: Optional[PrimaryIngestionPlatform] = eco.getPrimaryIngestionPlatformsForDatastore(store.name)
        if pip is None:
            assert store.cmd is not None
            return store.cmd
        else:
            primDP: Optional[YellowDataPlatform] = self.getCompatiblePIPToUseForDatastore(eco, store.name)
            # If this platform is itself the primary ingestion platform for the datastore,
            # then the effective CMD should remain unchanged (read from the original source).
            if primDP is self:
                assert store.cmd is not None
                return store.cmd
            assert store.cmd is not None
            assert store.cmd.singleOrMultiDatasetIngestion is not None
            assert primDP is not None
            cmd: SQLIngestion = SQLMergeIngestion(
                primDP.psp.mergeStore,  # The merge tables are in the primary platform's merge store
                primDP,
                self.psp.mergeRW_Credential,  # This dataplatforms credentials MUST have read access to the primary platform's merge store
                store.cmd.singleOrMultiDatasetIngestion
            )
            return cmd

    def getCompatiblePIPToUseForDatastore(self, eco: Ecosystem, storeName: str) -> Optional['YellowDataPlatform']:
        """This returns a DataPlatform which is compatible with this DataPlatform. Datastores with PIPs can have multiple of them.
        We need to pick one whose merge store we understand can be used to ingest data from the datastore. For now, these
        are YellowDataPlatforms."""
        pip: Optional[PrimaryIngestionPlatform] = eco.getPrimaryIngestionPlatformsForDatastore(storeName)
        if pip is None:
            return None
        else:
            primDP: Optional[YellowDataPlatform] = None
            dpKey: DataPlatformKey
            for dpKey in pip.dataPlatforms:
                dp: DataPlatform = eco.getDataPlatformOrThrow(dpKey.name)
                if isinstance(dp, YellowDataPlatform):
                    primDP = dp
                    break
            return primDP

    def setPSP(self, psp: YellowPlatformServiceProvider) -> None:
        super().setPSP(psp)

    def getCredentialStore(self) -> CredentialStore:
        return self.psp.credStore

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
                "pspName": self.psp.name,
                "milestoneStrategy": self.milestoneStrategy.value,
                "stagingBatchesToKeep": self.stagingBatchesToKeep
            }
        )
        return rc

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def isWorkspaceDataContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return isinstance(dc, DataPlatformManagedDataContainer)

    def lint(self, eco: Ecosystem, tree: ValidationTree) -> None:
        """This should validate the platform and its associated parts but it cannot validate the usage of the DataPlatform
        as the graph must be generated for that to happen. The lintGraph method on the KPSGraphHandler does
        that as well as generating the terraform, airflow and other artifacts."""
        super().lint(eco, tree)
        if self.stagingBatchesToKeep < -1:
            tree.addRaw(ValidationProblem(f"Staging batches to keep must be -1 or larger, got {self.stagingBatchesToKeep}", ProblemSeverity.ERROR))

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        """This is called to handle merge events on the revised graph."""
        return YellowGraphHandler(self, graph)

    def getTableForPlatform(self, tableName: str) -> str:
        """This returns the table name for the platform"""
        return f"{self.name}_{tableName}".lower()

    def getPhysDAGTableName(self) -> str:
        """This returns the name of the batch counter table"""
        return self.psp.namingMapper.fmtTVI(self.getTableForPlatform("airflow_dsg"))

    def getPhysDataTransformerTableName(self) -> str:
        """This returns the name of the DataTransformer DAG table"""
        return self.psp.namingMapper.fmtTVI(self.getTableForPlatform("airflow_datatransformer"))

    def getAirflowDAGTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch metrics table. The key is either the data store name or the
        data store name and the dataset name."""
        t: Table = Table(self.getPhysDAGTableName(), MetaData(),
                         Column("stream_key", sqlalchemy.String(length=STREAM_KEY_MAX_LENGTH), primary_key=True),
                         Column("config_json", sqlalchemy.String(length=2048)),
                         Column("status", sqlalchemy.String(length=50)),
                         Column("created_at", TIMESTAMP()),
                         Column("updated_at", TIMESTAMP()))
        return t

    def getDataTransformerDAGTable(self) -> Table:
        """This constructs the sqlalchemy table for DataTransformer DAG configurations."""
        t: Table = Table(self.getPhysDataTransformerTableName(), MetaData(),
                         Column("workspace_name", sqlalchemy.String(length=255), primary_key=True),
                         Column("config_json", sqlalchemy.String(length=4096)),
                         Column("status", sqlalchemy.String(length=50)),
                         Column("created_at", TIMESTAMP()),
                         Column("updated_at", TIMESTAMP()))
        return t

    def createSchemaProjector(self, eco: Ecosystem) -> SchemaProjector:
        # Create a database operations object for the merge database
        return YellowSchemaProjector(eco, self)

    def lintWorkspace(self, eco: Ecosystem, tree: ValidationTree, ws: 'Workspace', dsgName: str):
        # Here, we want to make sure this Platform is configured to support the intended milestone strategy.
        dsg: Optional[DatasetGroup] = ws.dsgs[dsgName]
        if dsg is None:
            tree.addRaw(UnknownObjectReference(f"Unknown dataset group {dsgName}", ProblemSeverity.ERROR))
        else:
            # Yellow only supports DataPlatformManagedDataContainer for workspaces
            if not isinstance(ws.dataContainer, DataPlatformManagedDataContainer):
                tree.addRaw(ObjectWrongType(ws.dataContainer, DataPlatformManagedDataContainer, ProblemSeverity.ERROR))
            chooser: Optional[DataPlatformChooser] = dsg.platformMD
            if chooser is not None:
                if isinstance(chooser, WorkspacePlatformConfig):
                    if self.milestoneStrategy == YellowMilestoneStrategy.SCD2:
                        if chooser.retention.milestoningStrategy != DataMilestoningStrategy.FORENSIC:
                            tree.addRaw(AttributeValueNotSupported(
                                chooser.retention.milestoningStrategy, [DataMilestoningStrategy.FORENSIC.name], ProblemSeverity.ERROR))
                    elif self.milestoneStrategy == YellowMilestoneStrategy.SCD1:
                        if chooser.retention.milestoningStrategy != DataMilestoningStrategy.LIVE_ONLY:
                            tree.addRaw(AttributeValueNotSupported(
                                chooser.retention.milestoningStrategy, [DataMilestoningStrategy.LIVE_ONLY.name], ProblemSeverity.ERROR))
                else:
                    tree.addRaw(ObjectWrongType(chooser, WorkspacePlatformConfig, ProblemSeverity.ERROR))

    def resetBatchState(self, eco: Ecosystem, storeName: str, datasetName: Optional[str] = None, committedOk: bool = False) -> str:
        """Reset batch state for a given store and optionally a specific dataset.
        This clears batch counters and metrics, forcing a fresh start for ingestion.

        Args:
            eco: The ecosystem containing the datastore
            storeName: Name of the datastore to reset
            datasetName: Optional dataset name for single-dataset reset. If None, resets entire store.
        """

        # Get credentials and create database connection
        user, password = self.psp.credStore.getAsUserPassword(self.psp.mergeRW_Credential)
        engine = createEngine(self.psp.mergeStore, user, password)

        # Validate datastore exists before proceeding
        datastore_ce: Optional[DatastoreCacheEntry] = eco.cache_getDatastore(storeName)
        if datastore_ce is None:
            return "ERROR: Could not find datastore in ecosystem"

        datastore: Datastore = datastore_ce.datastore
        if datastore.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.MULTI_DATASET and datasetName is not None:
            return "ERROR: Cannot specify a dataset name for a multi-dataset datastore"
        elif datastore.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET and datasetName is None:
            return "ERROR: Cannot reset a single-dataset datastore without a dataset name"

        # Determine the key(s) to reset
        keys_to_reset = []
        if datasetName is not None:
            # Single dataset reset
            ydu: YellowDatasetUtilities = YellowDatasetUtilities(eco, self.psp.credStore, self, datastore, datasetName)
            keys_to_reset.append(ydu.getIngestionStreamKey())
        else:
            # Multi-dataset reset - there is just one key for the store.
            ydu: YellowDatasetUtilities = YellowDatasetUtilities(eco, self.psp.credStore, self, datastore)
            keys_to_reset.append(ydu.getIngestionStreamKey())

        # Get table names
        ypu: YellowDatasetUtilities = YellowDatasetUtilities(eco, self.psp.credStore, self, datastore)
        batch_counter_table = ypu.getPhysBatchCounterTableName()
        batch_metrics_table = ypu.getPhysBatchMetricsTableName()

        logger.info("Starting batch state reset",
                    platform_name=self.name,
                    store_name=storeName,
                    dataset_name=datasetName,
                    keys_to_reset=keys_to_reset)

        # We can only reset a batch there there is an existing open batch. An open batch is one where the batch_status is not committed.
        # Resetting a batch means deleting the ingested data for the current batch from staging. Then reset the state of the batch to
        # It's initial state.

        with engine.begin() as connection:
            for key in keys_to_reset:
                # First check if there's a current batch and its status
                # Get the current batch ID
                result = connection.execute(text(f'SELECT "currentBatch" FROM {batch_counter_table} WHERE key = :key'), {"key": key})
                batch_row = result.fetchone()

                if batch_row is not None:
                    current_batch_id = batch_row[0]

                    # Check the batch status
                    result = connection.execute(text(f'''
                        SELECT "batch_status"
                        FROM {batch_metrics_table}
                        WHERE "key" = :key AND "batch_id" = :batch_id
                    '''), {"key": key, "batch_id": current_batch_id})
                    status_row = result.fetchone()

                    if status_row is not None:
                        batch_status = status_row[0]
                        if batch_status == BatchStatus.COMMITTED.value:
                            if committedOk:
                                return "SUCCESS"
                            else:
                                logger.warning(
                                                "Batch reset skipped - batch is committed and committedOk is False",
                                                key=key,
                                                batch_id=current_batch_id,
                                                platform_name=self.name,
                                                reason="committed_batches_immutable")
                                return "ERROR: Batch is COMMITTED and cannot be reset"
                        else:
                            logger.info("Batch is safe to reset",
                                        key=key,
                                        batch_id=current_batch_id,
                                        batch_status=batch_status,
                                        platform_name=self.name)

                            # Proceed with reset - Get the datastore to access datasets
                            datastore_ce: Optional[DatastoreCacheEntry] = eco.cache_getDatastore(storeName)
                            if datastore_ce is None:
                                logger.error("Could not find datastore in ecosystem",
                                             store_name=storeName,
                                             platform_name=self.name)
                                return "ERROR: Could not find datastore in ecosystem"

                            datastore: Datastore = datastore_ce.datastore
                            if datastore.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.MULTI_DATASET and datasetName is not None:
                                logger.error("Cannot specify dataset name for multi-dataset datastore",
                                             datastore_name=datastore.name,
                                             dataset_name=datasetName,
                                             platform_name=self.name)
                                return "ERROR: Cannot specify a dataset name for a multi-dataset datastore"
                            elif datastore.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET and datasetName is None:
                                logger.error("Cannot reset single-dataset datastore without dataset name",
                                             datastore_name=datastore.name,
                                             platform_name=self.name)
                                return "ERROR: Cannot reset a single-dataset datastore without a dataset name"

                            # Get the batch state to understand which datasets are involved
                            result = connection.execute(text(f'''
                                SELECT "state"
                                FROM {batch_metrics_table}
                                WHERE "key" = :key AND "batch_id" = :batch_id
                            '''), {"key": key, "batch_id": current_batch_id})
                            batch_state_row = result.fetchone()

                            if batch_state_row is not None:
                                state: BatchState = BatchState.model_validate_json(batch_state_row[0])

                                # Reset the batch state to its initial state
                                state.reset()

                                # Update the batch_metrics table with the new batch state
                                result = connection.execute(text(f'''
                                    UPDATE {batch_metrics_table}
                                    SET "batch_status" = :batch_status,
                                        "state" = :batch_state
                                    WHERE "key" = :key AND "batch_id" = :batch_id
                                '''), {
                                    "key": key,
                                    "batch_id": current_batch_id,
                                    "batch_status": BatchStatus.STARTED.value,
                                    "batch_state": state.model_dump_json()
                                })

                                # Now delete the records from the staging table for every dataset in the batch
                                datasets_cleared = 0
                                for dataset_name in state.all_datasets:
                                    # Use the proper staging table naming method
                                    dataset: Dataset = datastore.datasets[dataset_name]
                                    staging_table_name = ypu.getPhysStagingTableNameForDataset(dataset)
                                    # Delete staging records for this batch using the correct column name
                                    result = connection.execute(text(f'''
                                        DELETE FROM {staging_table_name}
                                        WHERE {YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = :batch_id
                                    '''), {"batch_id": current_batch_id})

                                    records_deleted = result.rowcount
                                    if records_deleted > 0:
                                        logger.info("Cleared staging records for dataset",
                                                    dataset_name=dataset_name,
                                                    staging_table=staging_table_name,
                                                    records_deleted=records_deleted,
                                                    batch_id=current_batch_id)
                                        datasets_cleared += 1

                                logger.info("Batch reset completed successfully",
                                            key=key,
                                            batch_id=current_batch_id,
                                            datasets_cleared=datasets_cleared,
                                            platform_name=self.name)
                            else:
                                logger.error("Could not find batch state for batch",
                                             batch_id=current_batch_id,
                                             key=key,
                                             platform_name=self.name)
                                return "ERROR: Could not find batch state for batch {current_batch_id}"

        logger.info("Batch state reset operation completed",
                    platform_name=self.name,
                    store_name=storeName,
                    dataset_name=datasetName)
        return "SUCCESS"


class IUDValues(Enum):
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"


class YellowSchemaProjector(SchemaProjector):
    """This is a schema projector for the YellowDataPlatform. It projects the dataset schema to the original schema of the dataset."""

    def __init__(self, eco: Ecosystem, dp: 'YellowDataPlatform'):
        super().__init__(eco, dp)

    def getSchemaTypes(self) -> set[str]:
        return {YellowSchemaConstants.SCHEMA_TYPE_MERGE, YellowSchemaConstants.SCHEMA_TYPE_STAGING}

    def computeSchema(self, dataset: 'Dataset', schemaType: str, db_ops: DatabaseOperations) -> 'Dataset':
        """This returns the actual Dataset in use for that Dataset in the Workspace on this DataPlatform."""
        assert isinstance(self.dp, YellowDataPlatform)
        if schemaType == YellowSchemaConstants.SCHEMA_TYPE_MERGE:
            pds: Dataset = copy.deepcopy(dataset)
            ddlSchema: DDLTable = cast(DDLTable, pds.originalSchema)
            # Only add ds_surf_batch_id for live-only mode, not for forensic mode
            if self.dp.milestoneStrategy != YellowMilestoneStrategy.SCD2:
                ddlSchema.add(DDLColumn(name=YellowSchemaConstants.BATCH_ID_COLUMN_NAME, data_type=Integer(), nullable=NullableStatus.NOT_NULLABLE))
            ddlSchema.add(
                DDLColumn(name=YellowSchemaConstants.ALL_HASH_COLUMN_NAME, nullable=NullableStatus.NOT_NULLABLE,
                          data_type=VarChar(maxSize=db_ops.get_hash_column_width())))
            ddlSchema.add(
                DDLColumn(name=YellowSchemaConstants.KEY_HASH_COLUMN_NAME, nullable=NullableStatus.NOT_NULLABLE,
                          data_type=VarChar(maxSize=db_ops.get_hash_column_width())))
            if self.dp.milestoneStrategy == YellowMilestoneStrategy.SCD2:
                ddlSchema.add(DDLColumn(name=YellowSchemaConstants.BATCH_IN_COLUMN_NAME, data_type=Integer(), nullable=NullableStatus.NOT_NULLABLE))
                ddlSchema.add(DDLColumn(name=YellowSchemaConstants.BATCH_OUT_COLUMN_NAME, data_type=Integer(), nullable=NullableStatus.NOT_NULLABLE))
                # For forensic mode, modify the primary key to include batch_in
                if ddlSchema.primaryKeyColumns:
                    # Create new primary key with original columns plus batch_in
                    new_pk_columns = list(ddlSchema.primaryKeyColumns.colNames) + [YellowSchemaConstants.BATCH_IN_COLUMN_NAME]
                    ddlSchema.primaryKeyColumns = PrimaryKeyList(new_pk_columns)
            return pds
        elif schemaType == YellowSchemaConstants.SCHEMA_TYPE_STAGING:
            pds: Dataset = copy.deepcopy(dataset)
            ddlSchema: DDLTable = cast(DDLTable, pds.originalSchema)
            ddlSchema.add(DDLColumn(name=YellowSchemaConstants.BATCH_ID_COLUMN_NAME, data_type=Integer(), nullable=NullableStatus.NOT_NULLABLE))
            ddlSchema.add(
                DDLColumn(name=YellowSchemaConstants.ALL_HASH_COLUMN_NAME, nullable=NullableStatus.NOT_NULLABLE,
                          data_type=VarChar(maxSize=db_ops.get_hash_column_width())))
            ddlSchema.add(
                DDLColumn(name=YellowSchemaConstants.KEY_HASH_COLUMN_NAME, nullable=NullableStatus.NOT_NULLABLE,
                          data_type=VarChar(maxSize=db_ops.get_hash_column_width())))
            # For staging tables, modify the primary key to include batch_id to allow same business keys across batches
            if ddlSchema.primaryKeyColumns:
                # Create new primary key with original columns plus batch_id
                new_pk_columns = list(ddlSchema.primaryKeyColumns.colNames) + [YellowSchemaConstants.BATCH_ID_COLUMN_NAME]
                ddlSchema.primaryKeyColumns = PrimaryKeyList(new_pk_columns)
            return pds
        else:
            raise ValueError(f"Invalid schema type: {schemaType}")
