"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, DataPlatformExecutor, Documentation, Ecosystem, ValidationTree, CloudVendor, DataContainer, \
    PlatformPipelineGraph, DataPlatformGraphHandler, PostgresDatabase, MySQLDatabase, OracleDatabase, SQLServerDatabase
from typing import Any, Optional
from datasurface.md import LocationKey, Credential, KafkaServer, Datastore, KafkaIngestion, SQLSnapshotIngestion, ProblemSeverity, UnsupportedIngestionType, \
    DatastoreCacheEntry, IngestionConsistencyType, DatasetConsistencyNotSupported, \
    DataTransformerNode, DataTransformer, HostPortPair, HostPortPairList, Workspace, SQLIngestion
from datasurface.md.governance import DatasetGroup, DataTransformerOutput, IngestionMetadata
from datasurface.md.lint import ObjectWrongType, ObjectMissing, UnknownObjectReference, UnexpectedExceptionProblem, \
    ObjectNotSupportedByDataPlatform, AttributeValueNotSupported, AttributeNotSet
from datasurface.md.exceptions import ObjectDoesntExistException
from jinja2 import Environment, PackageLoader, select_autoescape, Template
from datasurface.md.credential import CredentialStore, CredentialType, CredentialTypeNotSupportedProblem, CredentialNotAvailableException, \
    CredentialNotAvailableProblem
from datasurface.md import SchemaProjector, DataContainerNamingMapper, Dataset, DataPlatformChooser, WorkspacePlatformConfig, DataMilestoningStrategy
from datasurface.md import DataPlatformManagedDataContainer, PlatformServicesProvider
from datasurface.md.schema import DDLTable, DDLColumn, PrimaryKeyList
from datasurface.md.types import Integer, String
import os
import re
from datasurface.md.repo import GitHubRepository
from typing import cast
import copy
from enum import Enum
import json
import sqlalchemy
from typing import List
from sqlalchemy import Table, Column, TIMESTAMP, MetaData, Engine
from datasurface.platforms.yellow.db_utils import createEngine
from datasurface.md.sqlalchemyutils import createOrUpdateTable, datasetToSQLAlchemyTable
from datasurface.md import CronTrigger, ExternallyTriggered, StepTrigger
from pydantic import BaseModel, Field
from abc import ABC
import hashlib
from datasurface.md.codeartifact import PythonRepoCodeArtifact
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger, set_context,
    log_operation_timing
)

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
        return self.dp.psp.namingMapper.mapNoun(self.dp.getTableForPlatform("batch_counter"))

    def getPhysBatchMetricsTableName(self) -> str:
        """This returns the name of the batch metrics table"""
        return self.dp.psp.namingMapper.mapNoun(self.dp.getTableForPlatform("batch_metrics"))

    def getBatchCounterTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch counter table"""
        t: Table = Table(self.getPhysBatchCounterTableName(), MetaData(),
                         Column("key", sqlalchemy.String(length=255), primary_key=True),
                         Column("currentBatch", sqlalchemy.Integer()))
        return t

    def getBatchMetricsTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch metrics table. The key is either the data store name or the
        data store name and the dataset name."""
        t: Table = Table(self.getPhysBatchMetricsTableName(), MetaData(),
                         Column("key", sqlalchemy.String(length=255), primary_key=True),
                         Column("batch_id", sqlalchemy.Integer(), primary_key=True),
                         Column("batch_start_time", sqlalchemy.TIMESTAMP()),
                         Column("batch_end_time", sqlalchemy.TIMESTAMP(), nullable=True),
                         Column("batch_status", sqlalchemy.String(length=32)),
                         Column("records_inserted", sqlalchemy.Integer(), nullable=True),
                         Column("records_updated", sqlalchemy.Integer(), nullable=True),
                         Column("records_deleted", sqlalchemy.Integer(), nullable=True),
                         Column("total_records", sqlalchemy.Integer(), nullable=True),
                         Column("state", sqlalchemy.String(length=2048), nullable=True))  # This needs to be large enough to hold the state of the ingestion
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
        return self.dp.psp.namingMapper.mapNoun(self.dp.getTableForPlatform(f"dt_{tableName}"))


class YellowDatasetUtilities(JobUtilities):
    """This class provides utilities for working with datasets in the Yellow platform."""
    def __init__(self, eco: Ecosystem, credStore: CredentialStore, dp: 'YellowDataPlatform', store: Datastore, datasetName: Optional[str] = None) -> None:
        super().__init__(eco, credStore, dp)
        self.store: Datastore = store
        self.datasetName: Optional[str] = datasetName
        self.dataset: Optional[Dataset] = self.store.datasets[datasetName] if datasetName is not None else None

    def checkForSchemaChanges(self, state: 'BatchState') -> None:
        """Check if any dataset schemas have changed since batch start"""
        for dataset_name in state.all_datasets:
            dataset = self.store.datasets[dataset_name]
            stored_hash = state.schema_versions.get(dataset_name)
            if stored_hash and not self.validateSchemaUnchanged(dataset, stored_hash):
                raise Exception(f"Schema changed for dataset {dataset_name} during batch processing")

    def getKey(self) -> str:
        """This returns the key for the batch"""
        return f"{self.store.name}#{self.dataset.name}" if self.dataset is not None else self.store.name

    def getStagingSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the staging schema for a dataset"""
        stagingDS: Dataset = self.schemaProjector.computeSchema(dataset, self.schemaProjector.SCHEMA_TYPE_STAGING)
        t: Table = datasetToSQLAlchemyTable(stagingDS, tableName, sqlalchemy.MetaData())
        return t

    def getMergeSchemaForDataset(self, dataset: Dataset, tableName: str) -> Table:
        """This returns the merge schema for a dataset"""
        mergeDS: Dataset = self.schemaProjector.computeSchema(dataset, self.schemaProjector.SCHEMA_TYPE_MERGE)
        t: Table = datasetToSQLAlchemyTable(mergeDS, tableName, sqlalchemy.MetaData())
        return t

    def getBaseTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the base table name for a dataset"""
        return self.getRawBaseTableNameForDataset(self.store, dataset, False)

    def getPhysStagingTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the staging table name for a dataset"""
        tableName: str = self.getBaseTableNameForDataset(dataset)
        return self.dp.psp.namingMapper.mapNoun(self.dp.getTableForPlatform(tableName + "_staging"))

    def getPhysMergeTableNameForDataset(self, dataset: Dataset) -> str:
        """This returns the merge table name for a dataset"""
        return self.dp.psp.namingMapper.mapNoun(self.getRawMergeTableNameForDataset(self.store, dataset))

    def createStagingTableIndexes(self, mergeEngine: Engine, tableName: str) -> None:
        """Create performance indexes for staging tables"""
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        batchIdIndexName: str = self.dp.psp.namingMapper.mapNoun(f"idx_{tableName}_batch_id")
        keyHashIndexName: str = self.dp.psp.namingMapper.mapNoun(f"idx_{tableName}_key_hash")
        batchKeyIndexName: str = self.dp.psp.namingMapper.mapNoun(f"idx_{tableName}_batch_key")
        indexes = [
            # Primary: batch filtering (used in every query)
            (batchIdIndexName, f"CREATE INDEX IF NOT EXISTS {batchIdIndexName} ON {tableName} ({sp.BATCH_ID_COLUMN_NAME})"),

            # Secondary: join performance
            (keyHashIndexName, f"CREATE INDEX IF NOT EXISTS {keyHashIndexName} ON {tableName} ({sp.KEY_HASH_COLUMN_NAME})"),

            # Composite: optimal for most queries that filter by batch AND join by key
            (batchKeyIndexName,
             f"CREATE INDEX IF NOT EXISTS {batchKeyIndexName} ON {tableName} ({sp.BATCH_ID_COLUMN_NAME}, {sp.KEY_HASH_COLUMN_NAME})")
        ]

        with mergeEngine.begin() as connection:
            for index_name, index_sql in indexes:
                # Check if index already exists
                check_sql = """
                SELECT COUNT(*) FROM pg_indexes
                WHERE indexname = :index_name AND tablename = :table_name
                """
                result = connection.execute(sqlalchemy.text(check_sql), {"index_name": index_name, "table_name": tableName})
                index_exists = result.fetchone()[0] > 0

                if not index_exists:
                    try:
                        with log_operation_timing(logger, "create_staging_index", index_name=index_name):
                            connection.execute(sqlalchemy.text(index_sql))
                        logger.info("Created staging index successfully", index_name=index_name, table_name=tableName)
                    except Exception as e:
                        logger.error("Failed to create staging index",
                                     index_name=index_name,
                                     table_name=tableName,
                                     error=str(e))

    def createMergeTableIndexes(self, mergeEngine: Engine, tableName: str) -> None:
        """Create performance indexes for merge tables"""
        assert self.schemaProjector is not None
        sp: YellowSchemaProjector = self.schemaProjector

        keyHashIndexName: str = self.dp.psp.namingMapper.mapNoun(f"idx_{tableName}_key_hash")
        indexes = [
            # Critical: join performance (exists in all modes)
            (keyHashIndexName, f"CREATE INDEX IF NOT EXISTS {keyHashIndexName} ON {tableName} ({sp.KEY_HASH_COLUMN_NAME})")
        ]

        # Add forensic-specific indexes for batch milestoned tables
        if self.dp.milestoneStrategy == YellowMilestoneStrategy.BATCH_MILESTONED:
            batchOutIndexName: str = self.dp.psp.namingMapper.mapNoun(f"idx_{tableName}_batch_out")
            liveRecordsIndexName: str = self.dp.psp.namingMapper.mapNoun(f"idx_{tableName}_live_records")
            batchInIndexName: str = self.dp.psp.namingMapper.mapNoun(f"idx_{tableName}_batch_in")
            batchRangeIndexName: str = self.dp.psp.namingMapper.mapNoun(f"idx_{tableName}_batch_range")
            indexes.extend([
                # Critical: live record filtering (batch_out = 2147483647)
                (batchOutIndexName, f"CREATE INDEX IF NOT EXISTS {batchOutIndexName} ON {tableName} ({sp.BATCH_OUT_COLUMN_NAME})"),

                # Composite: optimal for live record joins (most common pattern)
                (liveRecordsIndexName,
                 f"CREATE INDEX IF NOT EXISTS {liveRecordsIndexName} ON {tableName} ({sp.KEY_HASH_COLUMN_NAME}, {sp.BATCH_OUT_COLUMN_NAME})"),

                # For forensic queries: finding recently closed records
                (batchInIndexName, f"CREATE INDEX IF NOT EXISTS {batchInIndexName} ON {tableName} ({sp.BATCH_IN_COLUMN_NAME})"),

                # For forensic history queries
                (batchRangeIndexName,
                 f"CREATE INDEX IF NOT EXISTS {batchRangeIndexName} ON {tableName} ({sp.BATCH_IN_COLUMN_NAME}, {sp.BATCH_OUT_COLUMN_NAME})")
            ])
        else:
            # For live-only mode, we can create an index on ds_surf_all_hash for change detection
            allHashIndexName: str = self.dp.psp.namingMapper.mapNoun(f"idx_{tableName}_all_hash")
            indexes.extend([
                # For live-only mode: change detection performance
                (allHashIndexName, f"CREATE INDEX IF NOT EXISTS {allHashIndexName} ON {tableName} ({sp.ALL_HASH_COLUMN_NAME})")
            ])

        with mergeEngine.begin() as connection:
            for index_name, index_sql in indexes:
                # Check if index already exists
                check_sql = """
                SELECT COUNT(*) FROM pg_indexes
                WHERE indexname = :index_name AND tablename = :table_name
                """
                result = connection.execute(sqlalchemy.text(check_sql), {"index_name": index_name, "table_name": tableName})
                index_exists = result.fetchone()[0] > 0

                if not index_exists:
                    try:
                        with log_operation_timing(logger, "create_merge_index", index_name=index_name):
                            connection.execute(sqlalchemy.text(index_sql))
                        logger.info("Created merge index successfully", index_name=index_name, table_name=tableName)
                    except Exception as e:
                        logger.error("Failed to create merge index",
                                     index_name=index_name,
                                     table_name=tableName,
                                     error=str(e))

    def ensureUniqueConstraintExists(self, mergeEngine: Engine, tableName: str, columnName: str) -> None:
        """Ensure that a unique constraint exists on the specified column"""
        with mergeEngine.begin() as connection:
            # Check if the constraint already exists
            check_sql = f"""
            SELECT COUNT(*) FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = '{tableName}'
                AND tc.constraint_type = 'UNIQUE'
                AND kcu.column_name = '{columnName}'
            """
            result = connection.execute(sqlalchemy.text(check_sql))
            constraint_exists = result.fetchone()[0] > 0

            if not constraint_exists:
                # Create the unique constraint
                constraint_name = f"{tableName}_{columnName}_unique"
                create_constraint_sql = f"ALTER TABLE {tableName} ADD CONSTRAINT {constraint_name} UNIQUE ({columnName})"
                logger.debug("Creating unique constraint",
                             constraint_name=constraint_name,
                             table_name=tableName,
                             column_name=columnName)
                connection.execute(sqlalchemy.text(create_constraint_sql))
                logger.info("Successfully created unique constraint",
                            constraint_name=constraint_name,
                            table_name=tableName,
                            column_name=columnName)

    def createOrUpdateForensicTable(self, mergeEngine: Engine, mergeTable: Table, tableName: str) -> None:
        """Create or update a forensic table, handling primary key changes by dropping and recreating if needed"""
        from sqlalchemy import inspect
        inspector = inspect(mergeEngine)

        if not inspector.has_table(tableName):
            # Table doesn't exist, create it
            mergeTable.create(mergeEngine)
            logger.info("Created forensic table", table_name=tableName)
            return

        # Table exists, check if primary key needs to be updated
        current_table = Table(tableName, MetaData(), autoload_with=mergeEngine)

        # Get current primary key columns
        current_pk_columns = []
        for col in current_table.columns:
            if col.primary_key:
                current_pk_columns.append(col.name)

        # Get desired primary key columns from the merge table
        desired_pk_columns = []
        for col in mergeTable.columns:
            if col.primary_key:
                desired_pk_columns.append(col.name)

        # Check if primary key needs to be changed
        if set(current_pk_columns) != set(desired_pk_columns):
            logger.info("Primary key change detected for forensic table",
                        table_name=tableName,
                        current_pk=current_pk_columns,
                        desired_pk=desired_pk_columns)

            # Drop and recreate the table with new primary key
            with mergeEngine.begin() as connection:
                # Drop the existing table
                connection.execute(sqlalchemy.text(f"DROP TABLE {tableName} CASCADE"))
                logger.info("Dropped table for primary key change", table_name=tableName)

            # Create the new table
            mergeTable.create(mergeEngine)
            logger.info("Recreated table with new primary key", table_name=tableName)
        else:
            # Primary key is the same, use normal update
            createOrUpdateTable(mergeEngine, mergeTable)

    def reconcileStagingTableSchemas(self, mergeEngine: Engine, store: Datastore) -> None:
        """This will make sure the staging table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = self.getPhysStagingTableNameForDataset(dataset)
            stagingTable: Table = self.getStagingSchemaForDataset(dataset, tableName)
            createOrUpdateTable(mergeEngine, stagingTable)
            # Create performance indexes for staging table
            self.createStagingTableIndexes(mergeEngine, tableName)

    def reconcileMergeTableSchemas(self, mergeEngine: Engine, store: Datastore) -> None:
        """This will make sure the merge table exists and has the current schema for each dataset"""
        for dataset in store.datasets.values():
            # Map the dataset name if necessary
            tableName: str = self.getPhysMergeTableNameForDataset(dataset)
            mergeTable: Table = self.getMergeSchemaForDataset(dataset, tableName)

            # For forensic mode, we need to handle primary key changes
            if self.dp.milestoneStrategy == YellowMilestoneStrategy.BATCH_MILESTONED:
                self.createOrUpdateForensicTable(mergeEngine, mergeTable, tableName)
            else:
                createOrUpdateTable(mergeEngine, mergeTable)
                # Only add unique constraint on key_hash for live-only mode
                # In forensic mode, multiple records can have the same key_hash (different versions)
                self.ensureUniqueConstraintExists(mergeEngine, tableName, "ds_surf_key_hash")

            # Create performance indexes for merge table
            self.createMergeTableIndexes(mergeEngine, tableName)


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
    def __init__(self, name: str, locs: set[LocationKey], namespace: str):
        super().__init__(name, locs)
        self.namespace: str = namespace

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
                "namespace": self.namespace
            }
        )
        return rc

    def checkCredentialIsAvailable(self, cred: Credential, tree: ValidationTree) -> None:
        """This is used to check if a Credential is supported by this store."""
        pass

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


class YellowGraphHandler(DataPlatformGraphHandler):
    """This takes the graph and then implements the data pipeline described in the graph using the technology stack
    pattern implemented by this platform. This platform supports ingesting data from Kafka confluence connectors. It
    takes the data from kafka topics and writes them to a postgres staging table. A seperate job scheduled by airflow
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
        in the templates directory. It creates a sink connector to copy from the topics to a postgres
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
        for workspace in self.graph.workspaces.values():
            if workspace.dataTransformer is not None:
                if not isinstance(workspace.dataTransformer.code, PythonRepoCodeArtifact):
                    tree.addRaw(ObjectWrongType(workspace.dataTransformer.code, PythonRepoCodeArtifact, ProblemSeverity.ERROR))

        # If no Kafka ingestion nodes, return empty terraform
        if not ingest_nodes:
            return "# No Kafka ingestion nodes found - SQL snapshot ingestion doesn't require Kafka infrastructure"

        # Prepare the global context for the template
        try:
            pg_user, pg_password = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.postgresCredential)
            # Prepare Kafka API keys if needed (using connectCredentials)
            # kafka_api_key, kafka_api_secret = self.getKafkaKeysFromCredential(platform.connectCredentials)
            # TODO: Implement getKafkaKeysFromCredential similar to getPostgresUserPasswordFromCredential
            kafka_api_key = "placeholder_kafka_api_key"  # Placeholder
            kafka_api_secret = "placeholder_kafka_api_secret"  # Placeholder

            context: dict[str, Any] = {
                "ingest_nodes": ingest_nodes,
                "database_host": self.dp.psp.mergeStore.hostPortPair.hostName,
                "database_port": self.dp.psp.mergeStore.hostPortPair.port,
                "database_name": self.dp.psp.mergeStore.databaseName,
                "database_user": pg_user,
                "database_password": pg_password,
                "kafka_api_key": kafka_api_key,  # Placeholder
                "kafka_api_secret": kafka_api_secret,  # Placeholder
                # Add default_connector_config if defined in KPSGraphHandler
            }

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

    def lintGraph(self, eco: Ecosystem, credStore: 'CredentialStore', tree: ValidationTree) -> None:
        """This should be called execute graph. This is where the graph is validated and any issues are reported. If there are
        no issues then the graph is executed. Executed here means
        1. Terraform file which creates kafka connect connectors to ingest topics to postgres tables (for Kafka ingestion).
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
            if store.cmd is None:
                storeTree.addRaw(ObjectMissing(store, "cmd", ProblemSeverity.ERROR))
                return
            elif isinstance(store.cmd, KafkaIngestion):
                self.lintKafkaIngestion(store, storeTree)
            elif isinstance(store.cmd, SQLSnapshotIngestion):
                self.lintSQLSnapshotIngestion(store, storeTree)
            elif isinstance(store.cmd, DataTransformerOutput):
                # Nothing to do for DataTransformerOutput
                pass
            else:
                storeTree.addRaw(ObjectNotSupportedByDataPlatform(store, [KafkaIngestion, SQLSnapshotIngestion], ProblemSeverity.ERROR))

        # Check all Workspace requirements are supported by the platform
        for workspace in self.graph.workspaces.values():
            wsTree: ValidationTree = tree.addSubTree(workspace)
            if workspace.dataTransformer:
                dtTree: ValidationTree = wsTree.addSubTree(workspace.dataTransformer)
                dt: DataTransformer = workspace.dataTransformer
                if dt.code is None:
                    dtTree.addRaw(AttributeNotSet("code"))
                if not isinstance(dt.code, PythonRepoCodeArtifact):
                    dtTree.addRaw(ObjectNotSupportedByDataPlatform(store, [PythonRepoCodeArtifact], ProblemSeverity.ERROR))

                # YellowDataPlatform supports None (sensor-based) or CronTrigger (scheduled) for DataTransformers
                if dt.trigger is not None:
                    if not isinstance(dt.trigger, CronTrigger):
                        dtTree.addRaw(ObjectNotSupportedByDataPlatform(dt.trigger, [CronTrigger], ProblemSeverity.ERROR))

            for dsg in workspace.dsgs.values():
                dsgTree: ValidationTree = wsTree.addSubTree(dsg)
                pc: Optional[DataPlatformChooser] = dsg.platformMD
                if pc is not None:
                    if isinstance(pc, WorkspacePlatformConfig):
                        pcTree: ValidationTree = dsgTree.addSubTree(pc)
                        if pc.retention.milestoningStrategy != DataMilestoningStrategy.LIVE_ONLY:
                            pcTree.addRaw(AttributeValueNotSupported(pc, [DataMilestoningStrategy.LIVE_ONLY.name], ProblemSeverity.ERROR))
                    else:
                        dsgTree.addRaw(ObjectWrongType(pc, WorkspacePlatformConfig, ProblemSeverity.ERROR))
                else:
                    dsgTree.addRaw(AttributeNotSet("platformMD"))

        # Check CodeArtifacts on DataTransformer nodes are compatible with the PSP on the Ecosystem
        if eco.platformServicesProvider is not None:
            for node in self.graph.nodes.values():
                if isinstance(node, DataTransformerNode):
                    if node.workspace.dataTransformer is not None:
                        dt: DataTransformer = node.workspace.dataTransformer
                        dt.code.lint(eco, tree)

    def createAirflowDAGs(self, eco: Ecosystem, issueTree: ValidationTree) -> dict[str, str]:
        """This creates individual AirFlow DAGs for each ingestion stream.
        Each DAG runs the SnapshotMergeJob and handles the return code to decide whether to
        reschedule immediately, wait for next trigger, or fail."""

        # Create Jinja2 environment
        env: Environment = Environment(
            loader=PackageLoader('datasurface.platforms.yellow.templates', 'jinja'),
            autoescape=select_autoescape(['html', 'xml'])
        )

        # Load the ingestion stream DAG template
        dag_template: Template = env.get_template('ingestion_stream_dag.py.j2')

        # Build the ingestion_streams context from the graph
        ingestion_streams: list[dict[str, Any]] = []

        for storeName in self.graph.storesToIngest:
            try:
                storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(storeName)
                store: Datastore = storeEntry.datastore

                if isinstance(store.cmd, (KafkaIngestion, SQLSnapshotIngestion)):
                    # Determine if this is single or multi dataset ingestion
                    is_single_dataset = store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET

                    if is_single_dataset:
                        # For single dataset, create a separate entry for each dataset
                        for dataset in store.datasets.values():
                            stream_key = f"{storeName}_{dataset.name}"
                            ingestion_streams.append({
                                "stream_key": stream_key,
                                "single_dataset": True,
                                "datasets": [dataset.name],
                                "store_name": storeName,
                                "dataset_name": dataset.name,
                                "ingestion_type": "kafka" if isinstance(store.cmd, KafkaIngestion) else "sql_snapshot"
                            })
                    else:
                        # For multi dataset, create one entry for the entire store
                        ingestion_streams.append({
                            "stream_key": storeName,
                            "single_dataset": False,
                            "datasets": [dataset.name for dataset in store.datasets.values()],
                            "store_name": storeName,
                            "ingestion_type": "kafka" if isinstance(store.cmd, KafkaIngestion) else "sql_snapshot"
                        })
                elif isinstance(store.cmd, DataTransformerOutput):
                    # DataTransformerOutput doesn't need Kafka infrastructure
                    continue
                else:
                    issueTree.addRaw(ObjectNotSupportedByDataPlatform(store, [KafkaIngestion, SQLSnapshotIngestion], ProblemSeverity.ERROR))
                    continue

            except ObjectDoesntExistException:
                issueTree.addRaw(UnknownObjectReference(storeName, ProblemSeverity.ERROR))
                continue

        # Generate individual DAGs for each ingestion stream
        dag_files: dict[str, str] = {}

        try:
            gitRepo: GitHubRepository = cast(GitHubRepository, eco.owningRepo)

            # Extract git repository owner and name from the full repository name
            git_repo_parts = gitRepo.repositoryName.split('/')
            if len(git_repo_parts) != 2:
                raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
            git_repo_owner, git_repo_name = git_repo_parts

            # Common context for all DAGs
            common_context: dict[str, Any] = {
                "namespace_name": self.dp.psp.namespace,
                "platform_name": self.dp.to_k8s_name(self.dp.name),
                "original_platform_name": self.dp.name,  # Original platform name for job execution
                "ecosystem_name": eco.name,  # Original ecosystem name
                "ecosystem_k8s_name": self.dp.to_k8s_name(eco.name),  # K8s-safe ecosystem name for volume claims
                "postgres_hostname": self.dp.psp.mergeStore.hostPortPair.hostName,
                "postgres_database": self.dp.psp.mergeStore.databaseName,
                "postgres_port": self.dp.psp.mergeStore.hostPortPair.port,
                "postgres_credential_secret_name": self.dp.to_k8s_name(self.dp.psp.postgresCredential.name),
                "git_credential_secret_name": self.dp.to_k8s_name(self.dp.psp.gitCredential.name),
                "datasurface_docker_image": self.dp.psp.datasurfaceDockerImage,
                "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                "git_repo_branch": gitRepo.branchName,
                "git_repo_name": gitRepo.repositoryName,
                "git_repo_owner": git_repo_owner,
                "git_repo_repo_name": git_repo_name,
                # Git cache configuration variables
                "git_cache_storage_class": self.dp.psp.git_cache_storage_class,
                "git_cache_access_mode": self.dp.psp.git_cache_access_mode,
                "git_cache_storage_size": self.dp.psp.git_cache_storage_size,
                "git_cache_max_age_minutes": self.dp.psp.git_cache_max_age_minutes,
                "git_cache_enabled": self.dp.psp.git_cache_enabled,
            }

            # Generate a DAG for each ingestion stream
            for stream in ingestion_streams:
                # Add credential information for this stream
                storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(stream["store_name"])
                store: Datastore = storeEntry.datastore

                if stream["ingestion_type"] == "kafka":
                    stream["kafka_connect_credential_secret_name"] = self.dp.to_k8s_name(self.dp.psp.connectCredentials.name)
                elif stream["ingestion_type"] == "sql_snapshot":
                    # For SQL snapshot ingestion, we need the source database credentials
                    if isinstance(store.cmd, SQLSnapshotIngestion) and store.cmd.credential is not None:
                        stream["source_credential_secret_name"] = self.dp.to_k8s_name(store.cmd.credential.name)

                # Create context for this specific stream
                stream_context = common_context.copy()
                stream_context.update(stream)  # Add stream properties directly to context

                # Render the DAG for this stream
                dag_content: str = dag_template.render(stream_context)
                dag_filename = f"{self.dp.to_k8s_name(self.dp.name)}__{stream['stream_key']}_ingestion.py"
                dag_files[dag_filename] = dag_content

            return dag_files

        except Exception as e:
            issueTree.addRaw(UnexpectedExceptionProblem(e))
            return {}

    def getScheduleStringForTrigger(self, trigger: StepTrigger) -> str:
        """This returns the schedule string for a trigger"""
        if isinstance(trigger, CronTrigger):
            return trigger.cron
        elif isinstance(trigger, ExternallyTriggered):
            return "None"
        else:
            raise ValueError(f"Unsupported trigger type: {type(trigger)}")

    def populateDAGConfigurations(self, eco: Ecosystem, issueTree: ValidationTree) -> None:
        """Populate the database with ingestion stream configurations for dynamic DAG factory"""
        from sqlalchemy import text
        from datasurface.platforms.yellow.db_utils import createEngine

        # Get database connection
        user, password = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.postgresCredential)
        engine = createEngine(self.dp.psp.mergeStore, user, password)

        # Build the ingestion_streams context from the graph (same logic as createAirflowDAGs)
        ingestion_streams: list[dict[str, Any]] = []

        for storeName in self.graph.storesToIngest:
            try:
                storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(storeName)
                store: Datastore = storeEntry.datastore

                if isinstance(store.cmd, (KafkaIngestion, SQLSnapshotIngestion)):
                    # Determine if this is single or multi dataset ingestion
                    is_single_dataset = store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET

                    assert store.cmd.stepTrigger is not None
                    if is_single_dataset:
                        # For single dataset, create a separate entry for each dataset
                        for dataset in store.datasets.values():
                            stream_key = f"{storeName}_{dataset.name}"
                            stream_config = {
                                "stream_key": stream_key,
                                "single_dataset": True,
                                "datasets": [dataset.name],
                                "store_name": storeName,
                                "dataset_name": dataset.name,
                                "ingestion_type": "kafka" if isinstance(store.cmd, KafkaIngestion) else "sql_snapshot",
                                "schedule_string": self.getScheduleStringForTrigger(store.cmd.stepTrigger)
                            }

                            # Add credential information for this stream
                            if stream_config["ingestion_type"] == "kafka":
                                stream_config["kafka_connect_credential_secret_name"] = self.dp.to_k8s_name(self.dp.psp.connectCredentials.name)
                            elif stream_config["ingestion_type"] == "sql_snapshot":
                                # For SQL snapshot ingestion, we need the source database credentials
                                if isinstance(store.cmd, SQLSnapshotIngestion) and store.cmd.credential is not None:
                                    stream_config["source_credential_secret_name"] = self.dp.to_k8s_name(store.cmd.credential.name)

                            ingestion_streams.append(stream_config)
                    else:
                        # For multi dataset, create one entry for the entire store
                        stream_config = {
                            "stream_key": storeName,
                            "single_dataset": False,
                            "datasets": [dataset.name for dataset in store.datasets.values()],
                            "store_name": storeName,
                            "ingestion_type": "kafka" if isinstance(store.cmd, KafkaIngestion) else "sql_snapshot",
                            "schedule_string": self.getScheduleStringForTrigger(store.cmd.stepTrigger)
                        }

                        # Add credential information for this stream
                        if stream_config["ingestion_type"] == "kafka":
                            stream_config["kafka_connect_credential_secret_name"] = self.dp.to_k8s_name(self.dp.psp.connectCredentials.name)
                        elif stream_config["ingestion_type"] == "sql_snapshot":
                            # For SQL snapshot ingestion, we need the source database credentials
                            if isinstance(store.cmd, SQLSnapshotIngestion) and store.cmd.credential is not None:
                                stream_config["source_credential_secret_name"] = self.dp.to_k8s_name(store.cmd.credential.name)

                        ingestion_streams.append(stream_config)
                elif isinstance(store.cmd, DataTransformerOutput):
                    # DataTransformerOutput doesn't need Kafka infrastructure
                    continue
                else:
                    issueTree.addRaw(ObjectNotSupportedByDataPlatform(store, [KafkaIngestion, SQLSnapshotIngestion], ProblemSeverity.ERROR))
                    continue

            except ObjectDoesntExistException:
                issueTree.addRaw(UnknownObjectReference(storeName, ProblemSeverity.ERROR))
                continue

        try:
            gitRepo: GitHubRepository = cast(GitHubRepository, eco.owningRepo)

            # Extract git repository owner and name from the full repository name
            git_repo_parts = gitRepo.repositoryName.split('/')
            if len(git_repo_parts) != 2:
                raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
            git_repo_owner, git_repo_name = git_repo_parts

            # Common context for all streams (platform-level configuration)
            common_context: dict[str, Any] = {
                "namespace_name": self.dp.psp.namespace,
                "platform_name": self.dp.to_k8s_name(self.dp.name),
                "original_platform_name": self.dp.name,  # Original platform name for job execution
                "ecosystem_name": eco.name,  # Original ecosystem name
                "ecosystem_k8s_name": self.dp.to_k8s_name(eco.name),  # K8s-safe ecosystem name for volume claims
                "postgres_hostname": self.dp.psp.mergeStore.hostPortPair.hostName,
                "postgres_database": self.dp.psp.mergeStore.databaseName,
                "postgres_port": self.dp.psp.mergeStore.hostPortPair.port,
                "postgres_credential_secret_name": self.dp.to_k8s_name(self.dp.psp.postgresCredential.name),
                "git_credential_secret_name": self.dp.to_k8s_name(self.dp.psp.gitCredential.name),
                "git_credential_name": self.dp.psp.gitCredential.name,  # Original credential name for job parameter
                "datasurface_docker_image": self.dp.psp.datasurfaceDockerImage,
                "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                "git_repo_branch": gitRepo.branchName,
                "git_repo_name": gitRepo.repositoryName,
                "git_repo_owner": git_repo_owner,
                "git_repo_repo_name": git_repo_name,
                # Git cache configuration variables
                "git_cache_storage_class": self.dp.psp.git_cache_storage_class,
                "git_cache_access_mode": self.dp.psp.git_cache_access_mode,
                "git_cache_storage_size": self.dp.psp.git_cache_storage_size,
                "git_cache_max_age_minutes": self.dp.psp.git_cache_max_age_minutes,
                "git_cache_enabled": self.dp.psp.git_cache_enabled,
            }

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
        user, password = self.dp.psp.credStore.getAsUserPassword(self.dp.psp.postgresCredential)
        engine = createEngine(self.dp.psp.mergeStore, user, password)

        # Build the DataTransformer configurations from the graph
        datatransformer_configs: list[dict[str, Any]] = []

        for workspaceName, workspace in self.graph.workspaces.items():
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
                                    stream_key = f"{sink.storeName}_{sink.datasetName}"
                                else:
                                    stream_key = sink.storeName

                                # Regular ingestion DAG naming pattern
                                dag_id = f"{self.dp.to_k8s_name(self.dp.name)}__{stream_key}_ingestion"

                            elif isinstance(store.cmd, DataTransformerOutput):
                                # DataTransformer output store - use dt_ingestion naming pattern
                                dag_id = f"{self.dp.to_k8s_name(self.dp.name)}__{sink.storeName}_dt_ingestion"

                            else:
                                # Unsupported store type, skip
                                continue

                            if dag_id not in input_dag_ids:
                                input_dag_ids.append(dag_id)

                            input_dataset_list.append(f"{sink.storeName}#{sink.datasetName}")

                    # Get output dataset list
                    output_dataset_list: list[str] = [dataset.name for dataset in outputDatastore.datasets.values()]

                    # Create the configuration
                    dt_config = {
                        "workspace_name": workspaceName,
                        "output_datastore_name": outputDatastore.name,
                        "input_dag_ids": input_dag_ids,
                        "input_dataset_list": input_dataset_list,
                        "output_dataset_list": output_dataset_list
                    }

                    assert isinstance(workspace.dataTransformer.code, PythonRepoCodeArtifact)
                    if workspace.dataTransformer.code.repo.credential is not None:
                        dt_config["git_credential_secret_name"] = self.dp.to_k8s_name(workspace.dataTransformer.code.repo.credential.name)

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
            gitRepo: GitHubRepository = cast(GitHubRepository, eco.owningRepo)

            # Extract git repository owner and name from the full repository name
            git_repo_parts = gitRepo.repositoryName.split('/')
            if len(git_repo_parts) != 2:
                raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
            git_repo_owner, git_repo_name = git_repo_parts

            # Common context for all DataTransformers (platform-level configuration)
            common_context: dict[str, Any] = {
                "namespace_name": self.dp.psp.namespace,
                "platform_name": self.dp.to_k8s_name(self.dp.name),
                "original_platform_name": self.dp.name,  # Original platform name for job execution
                "ecosystem_name": eco.name,  # Original ecosystem name
                "ecosystem_k8s_name": self.dp.to_k8s_name(eco.name),  # K8s-safe ecosystem name for volume claims
                "postgres_hostname": self.dp.psp.mergeStore.hostPortPair.hostName,
                "postgres_database": self.dp.psp.mergeStore.databaseName,
                "postgres_port": self.dp.psp.mergeStore.hostPortPair.port,
                "postgres_credential_secret_name": self.dp.to_k8s_name(self.dp.psp.postgresCredential.name),
                "git_credential_secret_name": self.dp.to_k8s_name(self.dp.psp.gitCredential.name),
                "git_credential_name": self.dp.psp.gitCredential.name,  # Original credential name for job parameter
                "datasurface_docker_image": self.dp.psp.datasurfaceDockerImage,
                "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                "git_repo_branch": gitRepo.branchName,
                "git_repo_name": gitRepo.repositoryName,
                "git_repo_owner": git_repo_owner,
                "git_repo_repo_name": git_repo_name,
                # Git cache configuration variables
                "git_cache_storage_class": self.dp.psp.git_cache_storage_class,
                "git_cache_access_mode": self.dp.psp.git_cache_access_mode,
                "git_cache_storage_size": self.dp.psp.git_cache_storage_size,
                "git_cache_max_age_minutes": self.dp.psp.git_cache_max_age_minutes,
                "git_cache_enabled": self.dp.psp.git_cache_enabled,
            }

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
        kafka ingestion capture meta data to postgres staging tables. It also populates the database
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
                'credential': self.dp.psp.postgresCredential,
                'purpose': 'PostgreSQL database access for merge store',
                'k8s_name': self.dp.to_k8s_name(self.dp.psp.postgresCredential.name),
                'category': 'Platform Core'
            },
            {
                'credential': self.dp.psp.gitCredential,
                'purpose': 'Git repository access for ecosystem model',
                'k8s_name': self.dp.to_k8s_name(self.dp.psp.gitCredential.name),
                'category': 'Platform Core'
            },
            {
                'credential': self.dp.psp.connectCredentials,
                'purpose': 'Kafka Connect cluster API access',
                'k8s_name': self.dp.to_k8s_name(self.dp.psp.connectCredentials.name),
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
                    k8s_name = self.dp.to_k8s_name(store.cmd.credential.name)
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
        for workspaceName, workspace in self.graph.workspaces.items():
            if workspace.dataTransformer is not None:
                assert isinstance(workspace.dataTransformer.code, PythonRepoCodeArtifact)
                if workspace.dataTransformer.code.repo.credential is not None:
                    k8s_name = self.dp.to_k8s_name(workspace.dataTransformer.code.repo.credential.name)
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
        gitRepo: GitHubRepository = cast(GitHubRepository, eco.owningRepo)

        markdown_lines = [
            f"# Kubernetes Secrets for {self.dp.name}",
            "",
            f"This document lists all Kubernetes secrets required for the `{self.dp.name}` YellowDataPlatform deployment.",
            "",
            f"**Platform:** {self.dp.name}",
            f"**Namespace:** {self.dp.psp.namespace}",
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
                        f"  --namespace {self.dp.psp.namespace}",
                        "```",
                        ""
                    ])
                elif credential.credentialType == CredentialType.API_TOKEN:
                    markdown_lines.extend([
                        "**Create Command:**",
                        "```bash",
                        f"kubectl create secret generic {k8s_name} \\",
                        "  --from-literal=token='your-api-token' \\",
                        f"  --namespace {self.dp.psp.namespace}",
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
                        f"  --namespace {self.dp.psp.namespace}",
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
            f"kubectl get secrets -n {self.dp.psp.namespace}",
            "",
            "# Verify specific secrets exist",
        ])

        for k8s_name in sorted(secrets_info.keys()):
            markdown_lines.append(f"kubectl get secret {k8s_name} -n {self.dp.psp.namespace}")

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
            f"# Namespace: {self.dp.psp.namespace}",
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
            f"  name: {self.dp.to_k8s_name(self.dp.name)}-operational-database-access",
            f"  namespace: {self.dp.psp.namespace}",
            "  labels:",
            f"    app: {self.dp.to_k8s_name(self.dp.name)}",
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
    LIVE_ONLY = "live_only"
    BATCH_MILESTONED = "batch_milestoned"


class YellowPlatformServiceProvider(PlatformServicesProvider):
    """This provides basic kubernetes services to the YellowDataPlatforms it manages. It provides
    a kubernetes namespace, an airflow instance and a merge store."""

    def __init__(self, name: str, locs: set[LocationKey], doc: Documentation, namespace: str,
                 postgresCredential: Credential, gitCredential: Credential,
                 connectCredentials: Credential,
                 merge_datacontainer: PostgresDatabase,
                 kafkaConnectName: str = "kafka-connect",
                 kafkaClusterName: str = "kafka",
                 airflowName: str = "airflow",
                 datasurfaceDockerImage: str = "datasurface/datasurface:latest", git_cache_storage_class: str = "standard",
                 git_cache_access_mode: str = "ReadWriteOnce", git_cache_storage_size: str = "5Gi", git_cache_max_age_minutes: int = 5,
                 git_cache_enabled: bool = True):
        super().__init__(
            name,
            locs,
            KubernetesEnvVarsCredentialStore(
                name=f"{name}-cred-store", locs=locs, namespace=namespace
                ))
        self.namespace: str = namespace
        self.postgresCredential: Credential = postgresCredential
        self.gitCredential: Credential = gitCredential
        self.connectCredentials: Credential = connectCredentials
        self.kafkaConnectName: str = kafkaConnectName
        self.kafkaClusterName: str = kafkaClusterName
        self.merge_datacontainer: PostgresDatabase = merge_datacontainer
        self.airflowName: str = airflowName
        self.datasurfaceDockerImage: str = datasurfaceDockerImage
        self.git_cache_storage_class: str = git_cache_storage_class
        self.git_cache_access_mode: str = git_cache_access_mode
        self.git_cache_storage_size: str = git_cache_storage_size
        self.git_cache_max_age_minutes: int = git_cache_max_age_minutes
        self.git_cache_enabled: bool = git_cache_enabled
        self.mergeStore: PostgresDatabase = merge_datacontainer
        self.namingMapper: DataContainerNamingMapper = self.mergeStore.getNamingAdapter()

        self.kafkaConnectCluster = KafkaConnectCluster(
            name=kafkaConnectName,
            locs=self.locs,
            restAPIUrlString=f"http://{kafkaConnectName}-service.{namespace}.svc.cluster.local:8083",
            kafkaServer=KafkaServer(
                name=kafkaClusterName,
                locs=self.locs,
                bootstrapServers=HostPortPairList([HostPortPair(f"{kafkaClusterName}-service.{namespace}.svc.cluster.local", 9092)])
            )
        )

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
                "namespace": self.namespace,
                "postgresCredential": self.postgresCredential.to_json(),
                "gitCredential": self.gitCredential.to_json(),
                "connectCredentials": self.connectCredentials.to_json(),
                "kafkaConnectName": self.kafkaConnectName,
                "kafkaClusterName": self.kafkaClusterName,
                "merge_datacontainer": self.merge_datacontainer.to_json(),
                "airflowName": self.airflowName,
                "datasurfaceDockerImage": self.datasurfaceDockerImage,
                "git_cache_storage_class": self.git_cache_storage_class,
                "git_cache_access_mode": self.git_cache_access_mode,
                "git_cache_storage_size": self.git_cache_storage_size,
                "git_cache_max_age_minutes": self.git_cache_max_age_minutes,
                "git_cache_enabled": self.git_cache_enabled,
            }
        )
        return rc

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        """This should validate the platform and its associated parts but it cannot validate the usage of the DataPlatform
        as the graph must be generated for that to happen. The lintGraph method on the KPSGraphHandler does
        that as well as generating the terraform, airflow and other artifacts."""
        super().lint(eco, tree)
        if not YellowPlatformServiceProvider.isLegalKubernetesNamespaceName(self.namespace):
            tree.addProblem(
                f"Kubernetes namespace '{self.namespace}' is not a valid RFC 1123 label (must match ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$)",
                ProblemSeverity.ERROR
            )
        if self.postgresCredential.credentialType != CredentialType.USER_PASSWORD:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.postgresCredential, [CredentialType.USER_PASSWORD]))
        if self.connectCredentials.credentialType != CredentialType.API_TOKEN:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.connectCredentials, [CredentialType.API_TOKEN]))
        if self.gitCredential.credentialType != CredentialType.API_TOKEN:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.gitCredential, [CredentialType.API_TOKEN]))

        # check the ecosystem repository is a GitHub repository, we're only supporting GitHub for now
        if not isinstance(eco.owningRepo, GitHubRepository):
            tree.addRaw(ObjectNotSupportedByDataPlatform(eco.owningRepo, [GitHubRepository], ProblemSeverity.ERROR))

        self.kafkaConnectCluster.lint(eco, tree.addSubTree(self.kafkaConnectCluster))
        self.mergeStore.lint(eco, tree.addSubTree(self.mergeStore))
        for loc in self.locs:
            loc.lint(tree.addSubTree(loc))
        eco.checkAllRepositoriesInEcosystem(tree, [GitHubRepository])

    @staticmethod
    def isLegalKubernetesNamespaceName(name: str) -> bool:
        """Check if the name is a valid Kubernetes namespace (RFC 1123 label)."""
        return re.match(r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$', name) is not None

    def _getKafkaBootstrapServers(self) -> str:
        """Calculate the Kafka bootstrap servers from the created Kafka cluster."""
        return f"{self.kafkaClusterName}-service.{self.namespace}.svc.cluster.local:9092"

    def mergeHandler(self, eco: 'Ecosystem'):
        """This is the merge handler implementation."""
        raise NotImplementedError("This is an abstract method")


class YellowDataPlatform(DataPlatform):
    """This defines the kubernetes postgres starter data platform. It can consume data from sources and write them to a postgres based merge store.
      It has the use of a postgres database for staging and merge tables as well as Workspace views"""
    def __init__(
            self,
            name: str,
            doc: Documentation,
            platformServiceProvider: YellowPlatformServiceProvider,
            milestoneStrategy: YellowMilestoneStrategy = YellowMilestoneStrategy.LIVE_ONLY,
            kafkaConnectName: str = "kafka-connect",
            kafkaClusterName: str = "kafka"):
        super().__init__(name, doc, YellowPlatformExecutor())
        self.psp: YellowPlatformServiceProvider = platformServiceProvider
        self.milestoneStrategy: YellowMilestoneStrategy = milestoneStrategy

        # Create the required data containers
        # Set logging context for this platform
        set_context(platform=name)

    def getCredentialStore(self) -> CredentialStore:
        return self.psp.credStore

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
                "pspName": self.psp.name,
                "milestoneStrategy": self.milestoneStrategy.value
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

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        """This is called to handle merge events on the revised graph."""
        return YellowGraphHandler(self, graph)

    @staticmethod
    def to_k8s_name(name: str) -> str:
        """Convert a name to a valid Kubernetes resource name (RFC 1123)."""
        name = name.lower().replace('_', '-').replace(' ', '-')
        name = re.sub(r'[^a-z0-9-]', '', name)
        name = re.sub(r'-+', '-', name)
        name = name.strip('-')
        return name

    def getTableForPlatform(self, tableName: str) -> str:
        """This returns the table name for the platform"""
        return f"{self.name}_{tableName}".lower()

    def getPhysDAGTableName(self) -> str:
        """This returns the name of the batch counter table"""
        return self.psp.namingMapper.mapNoun(self.getTableForPlatform("airflow_dsg"))

    def getPhysDataTransformerTableName(self) -> str:
        """This returns the name of the DataTransformer DAG table"""
        return self.psp.namingMapper.mapNoun(self.getTableForPlatform("airflow_datatransformer"))

    def getAirflowDAGTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch metrics table. The key is either the data store name or the
        data store name and the dataset name."""
        t: Table = Table(self.getPhysDAGTableName(), MetaData(),
                         Column("stream_key", sqlalchemy.String(length=255), primary_key=True),
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
            kubernetes_template: Template = env.get_template('kubernetes_services.yaml.j2')

            # Load the infrastructure DAG template
            dag_template: Template = env.get_template('infrastructure_dag.py.j2')

            # Load the factory DAG template
            factory_template: Template = env.get_template('yellow_platform_factory_dag.py.j2')

            # Load the DataTransformer factory DAG template
            datatransformer_factory_template: Template = env.get_template('datatransformer_factory_dag.py.j2')

            # Load the model merge job template
            model_merge_template: Template = env.get_template('model_merge_job.yaml.j2')

            # Load the ring1 initialization job template
            ring1_init_template: Template = env.get_template('ring1_init_job.yaml.j2')

            # Load the reconcile views job template
            reconcile_views_template: Template = env.get_template('reconcile_views_job.yaml.j2')

            gitRepo: GitHubRepository = cast(GitHubRepository, eco.owningRepo)

            # Extract git repository owner and name from the full repository name
            git_repo_parts = gitRepo.repositoryName.split('/')
            if len(git_repo_parts) != 2:
                raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
            git_repo_owner, git_repo_name = git_repo_parts

            # Prepare template context with all required variables
            context: dict[str, Any] = {
                "namespace_name": self.psp.namespace,
                "platform_name": self.to_k8s_name(self.name),
                "original_platform_name": self.name,  # Original platform name for job execution
                "ecosystem_name": eco.name,  # Original ecosystem name
                "ecosystem_k8s_name": self.to_k8s_name(eco.name),  # K8s-safe ecosystem name for volume claims
                "postgres_hostname": self.psp.mergeStore.hostPortPair.hostName,
                "postgres_database": self.psp.mergeStore.databaseName,
                "postgres_port": self.psp.mergeStore.hostPortPair.port,
                "postgres_credential_secret_name": self.to_k8s_name(self.psp.postgresCredential.name),
                "airflow_name": self.to_k8s_name(self.psp.airflowName),
                "airflow_credential_secret_name": self.to_k8s_name(self.psp.postgresCredential.name),  # Airflow uses postgres creds
                "kafka_cluster_name": self.to_k8s_name(self.psp.kafkaClusterName),
                "kafka_connect_name": self.to_k8s_name(self.psp.kafkaConnectName),
                "kafka_connect_credential_secret_name": self.to_k8s_name(self.psp.connectCredentials.name),
                "kafka_bootstrap_servers": self.psp._getKafkaBootstrapServers(),
                "datasurface_docker_image": self.psp.datasurfaceDockerImage,
                "git_credential_secret_name": self.to_k8s_name(self.psp.gitCredential.name),
                "git_credential_name": self.psp.gitCredential.name,  # Original credential name for job parameter
                "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                "git_repo_branch": gitRepo.branchName,
                "git_repo_name": gitRepo.repositoryName,
                "git_repo_owner": git_repo_owner,
                "git_repo_repo_name": git_repo_name,
                "ingestion_streams": {},  # Empty for bootstrap - no ingestion streams yet
                # Physical table names for factory DAGs to query
                "phys_dag_table_name": self.getPhysDAGTableName(),
                "phys_datatransformer_table_name": self.getPhysDataTransformerTableName(),
                # Git cache configuration variables
                "git_cache_storage_class": self.psp.git_cache_storage_class,
                "git_cache_access_mode": self.psp.git_cache_access_mode,
                "git_cache_storage_size": self.psp.git_cache_storage_size,
                "git_cache_max_age_minutes": self.psp.git_cache_max_age_minutes,
                "git_cache_enabled": self.psp.git_cache_enabled,
            }

            # Render the templates
            rendered_yaml: str = kubernetes_template.render(context)
            rendered_infrastructure_dag: str = dag_template.render(context)
            rendered_factory_dag: str = factory_template.render(context)
            rendered_datatransformer_factory_dag: str = datatransformer_factory_template.render(context)
            rendered_model_merge_job: str = model_merge_template.render(context)
            rendered_ring1_init_job: str = ring1_init_template.render(context)
            rendered_reconcile_views_job: str = reconcile_views_template.render(context)

            # Return as dictionary with filename as key
            return {
                "kubernetes-bootstrap.yaml": rendered_yaml,
                f"{self.to_k8s_name(self.name)}_infrastructure_dag.py": rendered_infrastructure_dag,
                f"{self.to_k8s_name(self.name)}_factory_dag.py": rendered_factory_dag,
                f"{self.to_k8s_name(self.name)}_datatransformer_factory_dag.py": rendered_datatransformer_factory_dag,
                f"{self.to_k8s_name(self.name)}_model_merge_job.yaml": rendered_model_merge_job,
                f"{self.to_k8s_name(self.name)}_ring1_init_job.yaml": rendered_ring1_init_job,
                f"{self.to_k8s_name(self.name)}_reconcile_views_job.yaml": rendered_reconcile_views_job
            }
        elif ringLevel == 1:
            # Create the airflow dsg table and datatransformer table if needed
            mergeUser, mergePassword = self.psp.credStore.getAsUserPassword(self.psp.postgresCredential)
            mergeEngine: Engine = createEngine(self.psp.mergeStore, mergeUser, mergePassword)
            createOrUpdateTable(mergeEngine, self.getAirflowDAGTable())
            createOrUpdateTable(mergeEngine, self.getDataTransformerDAGTable())
            return {}
        else:
            raise ValueError(f"Invalid ring level {ringLevel} for YellowDataPlatform")

    def createSchemaProjector(self, eco: Ecosystem) -> SchemaProjector:
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
                    if self.milestoneStrategy == YellowMilestoneStrategy.BATCH_MILESTONED:
                        if chooser.retention.milestoningStrategy != DataMilestoningStrategy.FORENSIC:
                            tree.addRaw(AttributeValueNotSupported(
                                chooser.retention.milestoningStrategy, [DataMilestoningStrategy.FORENSIC.name], ProblemSeverity.ERROR))
                    elif self.milestoneStrategy == YellowMilestoneStrategy.LIVE_ONLY:
                        if chooser.retention.milestoningStrategy != DataMilestoningStrategy.LIVE_ONLY:
                            tree.addRaw(AttributeValueNotSupported(
                                chooser.retention.milestoningStrategy, [DataMilestoningStrategy.LIVE_ONLY.name], ProblemSeverity.ERROR))
                else:
                    tree.addRaw(ObjectWrongType(chooser, WorkspacePlatformConfig, ProblemSeverity.ERROR))

    def resetBatchState(self, eco: Ecosystem, storeName: str, datasetName: Optional[str] = None) -> str:
        """Reset batch state for a given store and optionally a specific dataset.
        This clears batch counters and metrics, forcing a fresh start for ingestion.

        Args:
            eco: The ecosystem containing the datastore
            storeName: Name of the datastore to reset
            datasetName: Optional dataset name for single-dataset reset. If None, resets entire store.
        """
        from sqlalchemy import text
        from datasurface.platforms.yellow.db_utils import createEngine

        # Get credentials and create database connection
        user, password = self.psp.credStore.getAsUserPassword(self.psp.postgresCredential)
        engine = createEngine(self.psp.mergeStore, user, password)

        # Create schema projector to access column name constants
        schema_projector_base = self.createSchemaProjector(eco)
        schema_projector = cast(YellowSchemaProjector, schema_projector_base)

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
            keys_to_reset.append(f"{storeName}#{datasetName}")
        else:
            # Multi-dataset reset - there is just one key for the store.
            keys_to_reset.append(storeName)

        # Get table names
        platform_prefix = self.name.lower()
        batch_counter_table = platform_prefix + "_batch_counter"
        batch_metrics_table = platform_prefix + "_batch_metrics"

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
                            logger.warning("Batch reset skipped - batch is committed",
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
                                    base_table_name = f"{storeName}_{dataset_name}"
                                    staging_table_name = f"{platform_prefix}_{base_table_name}_staging"

                                    # Delete staging records for this batch using the correct column name
                                    result = connection.execute(text(f'''
                                        DELETE FROM {staging_table_name}
                                        WHERE {schema_projector.BATCH_ID_COLUMN_NAME} = :batch_id
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

    def __init__(self, eco: Ecosystem, dp: 'YellowDataPlatform'):
        super().__init__(eco, dp)

    def getSchemaTypes(self) -> set[str]:
        return {self.SCHEMA_TYPE_MERGE, self.SCHEMA_TYPE_STAGING}

    def computeSchema(self, dataset: 'Dataset', schemaType: str) -> 'Dataset':
        """This returns the actual Dataset in use for that Dataset in the Workspace on this DataPlatform."""
        assert isinstance(self.dp, YellowDataPlatform)
        if schemaType == self.SCHEMA_TYPE_MERGE:
            pds: Dataset = copy.deepcopy(dataset)
            ddlSchema: DDLTable = cast(DDLTable, pds.originalSchema)
            ddlSchema.add(DDLColumn(name=self.BATCH_ID_COLUMN_NAME, data_type=Integer()))
            ddlSchema.add(DDLColumn(name=self.ALL_HASH_COLUMN_NAME, data_type=String(maxSize=32)))
            ddlSchema.add(DDLColumn(name=self.KEY_HASH_COLUMN_NAME, data_type=String(maxSize=32)))
            if self.dp.milestoneStrategy == YellowMilestoneStrategy.BATCH_MILESTONED:
                ddlSchema.add(DDLColumn(name=self.BATCH_IN_COLUMN_NAME, data_type=Integer()))
                ddlSchema.add(DDLColumn(name=self.BATCH_OUT_COLUMN_NAME, data_type=Integer()))
                # For forensic mode, modify the primary key to include batch_in
                if ddlSchema.primaryKeyColumns:
                    # Create new primary key with original columns plus batch_in
                    new_pk_columns = list(ddlSchema.primaryKeyColumns.colNames) + [self.BATCH_IN_COLUMN_NAME]
                    ddlSchema.primaryKeyColumns = PrimaryKeyList(new_pk_columns)
            return pds
        elif schemaType == self.SCHEMA_TYPE_STAGING:
            pds: Dataset = copy.deepcopy(dataset)
            ddlSchema: DDLTable = cast(DDLTable, pds.originalSchema)
            ddlSchema.add(DDLColumn(name=self.BATCH_ID_COLUMN_NAME, data_type=Integer()))
            ddlSchema.add(DDLColumn(name=self.ALL_HASH_COLUMN_NAME, data_type=String(maxSize=32)))
            ddlSchema.add(DDLColumn(name=self.KEY_HASH_COLUMN_NAME, data_type=String(maxSize=32)))
            return pds
        else:
            raise ValueError(f"Invalid schema type: {schemaType}")
