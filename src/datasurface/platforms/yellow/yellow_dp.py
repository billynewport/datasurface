"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, DataPlatformExecutor, Documentation, Ecosystem, ValidationTree, CloudVendor, DataContainer, \
    PlatformPipelineGraph, DataPlatformGraphHandler, PostgresDatabase, MySQLDatabase, OracleDatabase, SQLServerDatabase
from typing import Any, Optional
from datasurface.md import LocationKey, Credential, KafkaServer, Datastore, KafkaIngestion, SQLSnapshotIngestion, ProblemSeverity, UnsupportedIngestionType, \
    DatastoreCacheEntry, IngestionConsistencyType, DatasetConsistencyNotSupported, \
    DataTransformerNode, DataTransformer, HostPortPair, HostPortPairList, Workspace
from datasurface.md.governance import DatasetGroup, DataTransformerOutput
from datasurface.md.lint import ObjectWrongType, ObjectMissing, UnknownObjectReference, UnexpectedExceptionProblem, \
    ObjectNotSupportedByDataPlatform, AttributeValueNotSupported, AttributeNotSet
from datasurface.md.exceptions import ObjectDoesntExistException
from jinja2 import Environment, PackageLoader, select_autoescape, Template
from datasurface.md.credential import CredentialStore, CredentialType, CredentialTypeNotSupportedProblem, CredentialNotAvailableException, \
    CredentialNotAvailableProblem
from datasurface.md import SchemaProjector, DataContainerNamingMapper, Dataset, DataPlatformChooser, WorkspacePlatformConfig, DataMilestoningStrategy
from datasurface.md import DataPlatformManagedDataContainer
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
from datasurface.md.sqlalchemyutils import createOrUpdateTable
from datasurface.md import CronTrigger, ExternallyTriggered, StepTrigger
from pydantic import BaseModel, Field
from datasurface.md.codeartifact import PythonRepoCodeArtifact


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
        token: Optional[str] = os.getenv(f"{cred.name}_TOKEN")
        if token is None:
            raise CredentialNotAvailableException(cred, "token is None")
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
        return {self.dp.kafkaConnectCluster, self.dp.mergeStore}

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
            print("Error: Platform associated with the graph is not a YellowGraphHandler.")
            tree.addRaw(ObjectWrongType(self.graph.platform, YellowDataPlatform, ProblemSeverity.ERROR))
            return ""

        platform: YellowDataPlatform = self.graph.platform

        ingest_nodes: list[dict[str, Any]] = []
        for storeName in self.graph.storesToIngest:
            try:
                storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(storeName)
                store: Datastore = storeEntry.datastore

                if isinstance(store.cmd, KafkaIngestion):
                    # For each dataset in the store
                    for dataset in store.datasets.values():
                        datasetName = dataset.name
                        # Example: Create a unique connector name based on store and dataset
                        connector_name = f"{storeName}_{datasetName}".replace("-", "_")

                        # Determine other node-specific attributes
                        kafka_topic = f"{storeName}.{datasetName}"  # Example topic naming convention
                        target_table_name = f"staging_{storeName}_{datasetName}".replace("-", "_")
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
            pg_user, pg_password = self.dp.credStore.getAsUserPassword(platform.postgresCredential)
            # Prepare Kafka API keys if needed (using connectCredentials)
            # kafka_api_key, kafka_api_secret = self.getKafkaKeysFromCredential(platform.connectCredentials)
            # TODO: Implement getKafkaKeysFromCredential similar to getPostgresUserPasswordFromCredential
            kafka_api_key = "placeholder_kafka_api_key"  # Placeholder
            kafka_api_secret = "placeholder_kafka_api_secret"  # Placeholder

            context: dict[str, Any] = {
                "ingest_nodes": ingest_nodes,
                "database_host": platform.mergeStore.hostPortPair.hostName,
                "database_port": platform.mergeStore.hostPortPair.port,
                "database_name": platform.mergeStore.databaseName,
                "database_user": pg_user,
                "database_password": pg_password,
                "kafka_api_key": kafka_api_key,  # Placeholder
                "kafka_api_secret": kafka_api_secret,  # Placeholder
                # Add default_connector_config if defined in KPSGraphHandler
            }

            # Render the template once with the full context
            code: str = template.render(context)

            print(f"Generated Terraform code:\n{code}")
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
                "namespace_name": self.dp.namespace,
                "platform_name": self.dp.to_k8s_name(self.dp.name),
                "original_platform_name": self.dp.name,  # Original platform name for job execution
                "postgres_hostname": self.dp.mergeStore.hostPortPair.hostName,
                "postgres_database": self.dp.mergeStore.databaseName,
                "postgres_port": self.dp.mergeStore.hostPortPair.port,
                "postgres_credential_secret_name": self.dp.to_k8s_name(self.dp.postgresCredential.name),
                "git_credential_secret_name": self.dp.to_k8s_name(self.dp.gitCredential.name),
                "slack_credential_secret_name": self.dp.to_k8s_name(self.dp.slackCredential.name),
                "slack_channel_name": self.dp.slackChannel,
                "datasurface_docker_image": self.dp.datasurfaceImage,
                "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                "git_repo_branch": gitRepo.branchName,
                "git_repo_name": gitRepo.repositoryName,
                "git_repo_owner": git_repo_owner,
                "git_repo_repo_name": git_repo_name,
            }

            # Generate a DAG for each ingestion stream
            for stream in ingestion_streams:
                # Add credential information for this stream
                storeEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(stream["store_name"])
                store: Datastore = storeEntry.datastore

                if stream["ingestion_type"] == "kafka":
                    stream["kafka_connect_credential_secret_name"] = self.dp.to_k8s_name(self.dp.connectCredentials.name)
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
        user, password = self.dp.credStore.getAsUserPassword(self.dp.postgresCredential)
        engine = createEngine(self.dp.mergeStore, user, password)

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
                                stream_config["kafka_connect_credential_secret_name"] = self.dp.to_k8s_name(self.dp.connectCredentials.name)
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
                            stream_config["kafka_connect_credential_secret_name"] = self.dp.to_k8s_name(self.dp.connectCredentials.name)
                        elif stream_config["ingestion_type"] == "sql_snapshot":
                            # For SQL snapshot ingestion, we need the source database credentials
                            if isinstance(store.cmd, SQLSnapshotIngestion) and store.cmd.credential is not None:
                                stream_config["source_credential_secret_name"] = self.dp.to_k8s_name(store.cmd.credential.name)

                        ingestion_streams.append(stream_config)
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
                "namespace_name": self.dp.namespace,
                "platform_name": self.dp.to_k8s_name(self.dp.name),
                "original_platform_name": self.dp.name,  # Original platform name for job execution
                "postgres_hostname": self.dp.mergeStore.hostPortPair.hostName,
                "postgres_database": self.dp.mergeStore.databaseName,
                "postgres_port": self.dp.mergeStore.hostPortPair.port,
                "postgres_credential_secret_name": self.dp.to_k8s_name(self.dp.postgresCredential.name),
                "git_credential_secret_name": self.dp.to_k8s_name(self.dp.gitCredential.name),
                "slack_credential_secret_name": self.dp.to_k8s_name(self.dp.slackCredential.name),
                "slack_channel_name": self.dp.slackChannel,
                "datasurface_docker_image": self.dp.datasurfaceImage,
                "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                "git_repo_branch": gitRepo.branchName,
                "git_repo_name": gitRepo.repositoryName,
                "git_repo_owner": git_repo_owner,
                "git_repo_repo_name": git_repo_name,
            }

            # Get table name
            table_name = self.dp.getDAGTableName()

            # Store configurations in database
            with engine.begin() as connection:
                # First, delete all existing records for this platform to ensure clean state
                connection.execute(text(f"DELETE FROM {table_name}"))
                print(f"Cleared existing configurations from {table_name}")

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

            print(f"Populated {len(ingestion_streams)} ingestion stream configurations in {table_name}")

        except Exception as e:
            issueTree.addRaw(UnexpectedExceptionProblem(e))

    def populateDataTransformerConfigurations(self, eco: Ecosystem, issueTree: ValidationTree) -> None:
        """Populate the database with DataTransformer configurations for dynamic DAG factory"""
        from sqlalchemy import text
        from datasurface.platforms.yellow.db_utils import createEngine

        # Get database connection
        user, password = self.dp.credStore.getAsUserPassword(self.dp.postgresCredential)
        engine = createEngine(self.dp.mergeStore, user, password)

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
                "namespace_name": self.dp.namespace,
                "platform_name": self.dp.to_k8s_name(self.dp.name),
                "original_platform_name": self.dp.name,  # Original platform name for job execution
                "postgres_hostname": self.dp.mergeStore.hostPortPair.hostName,
                "postgres_database": self.dp.mergeStore.databaseName,
                "postgres_port": self.dp.mergeStore.hostPortPair.port,
                "postgres_credential_secret_name": self.dp.to_k8s_name(self.dp.postgresCredential.name),
                "git_credential_secret_name": self.dp.to_k8s_name(self.dp.gitCredential.name),
                "slack_credential_secret_name": self.dp.to_k8s_name(self.dp.slackCredential.name),
                "slack_channel_name": self.dp.slackChannel,
                "datasurface_docker_image": self.dp.datasurfaceImage,
                "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                "git_repo_branch": gitRepo.branchName,
                "git_repo_name": gitRepo.repositoryName,
                "git_repo_owner": git_repo_owner,
                "git_repo_repo_name": git_repo_name,
            }

            # Get table name
            table_name = self.dp.getDataTransformerTableName()

            # Store configurations in database
            with engine.begin() as connection:
                # First, delete all existing records for this platform to ensure clean state
                connection.execute(text(f"DELETE FROM {table_name}"))
                print(f"Cleared existing configurations from {table_name}")

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

            print(f"Populated {len(datatransformer_configs)} DataTransformer configurations in {table_name}")

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

        # Return only terraform - factory DAG is created during bootstrap
        result: dict[str, str] = {
            "terraform_code": terraform_code
        }
        return result


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


class YellowDataPlatform(DataPlatform):
    """This defines the kubernetes postgres starter data platform. It can consume data from sources and write them to a postgres based merge store.
      It has the use of a postgres database for staging and merge tables as well as Workspace views"""
    def __init__(
            self,
            name: str,
            locs: set[LocationKey],
            doc: Documentation,
            namespace: str,
            connectCredentials: Credential,
            postgresCredential: Credential,
            gitCredential: Credential,
            slackCredential: Credential,
            merge_datacontainer: PostgresDatabase,
            airflowName: str = "airflow",
            kafkaConnectName: str = "kafka-connect",
            kafkaClusterName: str = "kafka-cluster",
            slackChannel: str = "datasurface-events",
            milestoneStrategy: YellowMilestoneStrategy = YellowMilestoneStrategy.LIVE_ONLY,
            datasurfaceImage: str = "datasurface/datasurface:latest"
            ):
        super().__init__(name, doc, YellowPlatformExecutor())
        self.locs: set[LocationKey] = locs
        self.milestoneStrategy: YellowMilestoneStrategy = milestoneStrategy
        self.namespace: str = namespace
        self.connectCredentials: Credential = connectCredentials
        self.postgresCredential: Credential = postgresCredential
        self.airflowName: str = airflowName
        self.mergeStore: PostgresDatabase = merge_datacontainer
        self.kafkaConnectName: str = kafkaConnectName
        self.kafkaClusterName: str = kafkaClusterName
        self.slackCredential: Credential = slackCredential
        self.slackChannel: str = slackChannel
        self.gitCredential: Credential = gitCredential
        self.datasurfaceImage: str = datasurfaceImage

        # Create the required data containers
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

        self.credStore = KubernetesEnvVarsCredentialStore(
            name=f"{name}-cred-store",
            locs=self.locs,
            namespace=namespace
        )

    def getCredentialStore(self) -> CredentialStore:
        return self.credStore

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
                "namespace": self.namespace,
                "airflowName": self.airflowName,
                "kafkaConnectName": self.kafkaConnectName,
                "kafkaClusterName": self.kafkaClusterName,
                "connectCredentials": self.connectCredentials.to_json(),
                "postgresCredential": self.postgresCredential.to_json(),
                "slackCredential": self.slackCredential.to_json(),
                "slackChannel": self.slackChannel,
                "gitCredential": self.gitCredential.to_json(),
                "datasurfaceImage": self.datasurfaceImage,
                "kafkaConnectCluster": self.kafkaConnectCluster.to_json(),
                "mergeStore": self.mergeStore.to_json(),
                "milestoneStrategy": self.milestoneStrategy.value,
                "locs": [loc.to_json() for loc in self.locs]
            }
        )
        return rc

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def isWorkspaceDataContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return isinstance(dc, DataPlatformManagedDataContainer)

    def isLegalKubernetesNamespaceName(self, name: str) -> bool:
        """Check if the name is a valid Kubernetes namespace (RFC 1123 label)."""
        return re.match(r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$', name) is not None

    def lint(self, eco: Ecosystem, tree: ValidationTree) -> None:
        """This should validate the platform and its associated parts but it cannot validate the usage of the DataPlatform
        as the graph must be generated for that to happen. The lintGraph method on the KPSGraphHandler does
        that as well as generating the terraform, airflow and other artifacts."""
        super().lint(eco, tree)
        if not self.isLegalKubernetesNamespaceName(self.namespace):
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
        if self.slackCredential.credentialType != CredentialType.API_TOKEN:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.slackCredential, [CredentialType.API_TOKEN]))

        # check the ecosystem repository is a GitHub repository, we're only supporting GitHub for now
        if not isinstance(eco.owningRepo, GitHubRepository):
            tree.addRaw(ObjectNotSupportedByDataPlatform(eco.owningRepo, [GitHubRepository], ProblemSeverity.ERROR))

        self.kafkaConnectCluster.lint(eco, tree.addSubTree(self.kafkaConnectCluster))
        self.mergeStore.lint(eco, tree.addSubTree(self.mergeStore))
        for loc in self.locs:
            loc.lint(tree.addSubTree(loc))
        eco.checkAllRepositoriesInEcosystem(tree, [GitHubRepository])

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        """This is called to handle merge events on the revised graph."""
        return YellowGraphHandler(self, graph)

    def _getKafkaBootstrapServers(self) -> str:
        """Calculate the Kafka bootstrap servers from the created Kafka cluster."""
        return f"{self.kafkaClusterName}-service.{self.namespace}.svc.cluster.local:9092"

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

    def getDAGTableName(self) -> str:
        """This returns the name of the batch counter table"""
        return self.getTableForPlatform("airflow_dsg")

    def getDataTransformerTableName(self) -> str:
        """This returns the name of the DataTransformer DAG table"""
        return self.getTableForPlatform("airflow_datatransformer")

    def getAirflowDAGTable(self) -> Table:
        """This constructs the sqlalchemy table for the batch metrics table. The key is either the data store name or the
        data store name and the dataset name."""
        t: Table = Table(self.getDAGTableName(), MetaData(),
                         Column("stream_key", sqlalchemy.String(length=255), primary_key=True),
                         Column("config_json", sqlalchemy.String(length=2048)),
                         Column("status", sqlalchemy.String(length=50)),
                         Column("created_at", TIMESTAMP()),
                         Column("updated_at", TIMESTAMP()))
        return t

    def getDataTransformerDAGTable(self) -> Table:
        """This constructs the sqlalchemy table for DataTransformer DAG configurations."""
        t: Table = Table(self.getDataTransformerTableName(), MetaData(),
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

        print(f"Generating bootstrap artifacts for {self.name} at ring level '{ringLevel}'")
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

            gitRepo: GitHubRepository = cast(GitHubRepository, eco.owningRepo)

            # Extract git repository owner and name from the full repository name
            git_repo_parts = gitRepo.repositoryName.split('/')
            if len(git_repo_parts) != 2:
                raise ValueError(f"Invalid repository name format: {gitRepo.repositoryName}. Expected 'owner/repo'")
            git_repo_owner, git_repo_name = git_repo_parts

            # Prepare template context with all required variables
            context: dict[str, Any] = {
                "namespace_name": self.namespace,
                "platform_name": self.to_k8s_name(self.name),
                "original_platform_name": self.name,  # Original platform name for job execution
                "postgres_hostname": self.mergeStore.hostPortPair.hostName,
                "postgres_database": self.mergeStore.databaseName,
                "postgres_port": self.mergeStore.hostPortPair.port,
                "postgres_credential_secret_name": self.to_k8s_name(self.postgresCredential.name),
                "airflow_name": self.to_k8s_name(self.airflowName),
                "airflow_credential_secret_name": self.to_k8s_name(self.postgresCredential.name),  # Airflow uses postgres creds
                "kafka_cluster_name": self.to_k8s_name(self.kafkaClusterName),
                "kafka_connect_name": self.to_k8s_name(self.kafkaConnectName),
                "kafka_connect_credential_secret_name": self.to_k8s_name(self.connectCredentials.name),
                "kafka_bootstrap_servers": self._getKafkaBootstrapServers(),
                "datasurface_docker_image": self.datasurfaceImage,
                "git_credential_secret_name": self.to_k8s_name(self.gitCredential.name),
                "slack_credential_secret_name": self.to_k8s_name(self.slackCredential.name),
                "slack_channel_name": self.slackChannel,
                "git_repo_url": f"https://github.com/{gitRepo.repositoryName}",
                "git_repo_branch": gitRepo.branchName,
                "git_repo_name": gitRepo.repositoryName,
                "git_repo_owner": git_repo_owner,
                "git_repo_repo_name": git_repo_name,
                "ingestion_streams": {}  # Empty for bootstrap - no ingestion streams yet
            }

            # Render the templates
            rendered_yaml: str = kubernetes_template.render(context)
            rendered_infrastructure_dag: str = dag_template.render(context)
            rendered_factory_dag: str = factory_template.render(context)
            rendered_datatransformer_factory_dag: str = datatransformer_factory_template.render(context)
            rendered_model_merge_job: str = model_merge_template.render(context)
            rendered_ring1_init_job: str = ring1_init_template.render(context)

            # Return as dictionary with filename as key
            return {
                "kubernetes-bootstrap.yaml": rendered_yaml,
                f"{self.to_k8s_name(self.name)}_infrastructure_dag.py": rendered_infrastructure_dag,
                f"{self.to_k8s_name(self.name)}_factory_dag.py": rendered_factory_dag,
                f"{self.to_k8s_name(self.name)}_datatransformer_factory_dag.py": rendered_datatransformer_factory_dag,
                f"{self.to_k8s_name(self.name)}_model_merge_job.yaml": rendered_model_merge_job,
                f"{self.to_k8s_name(self.name)}_ring1_init_job.yaml": rendered_ring1_init_job
            }
        elif ringLevel == 1:
            # Create the airflow dsg table and datatransformer table if needed
            mergeUser, mergePassword = self.credStore.getAsUserPassword(self.postgresCredential)
            mergeEngine: Engine = createEngine(self.mergeStore, mergeUser, mergePassword)
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
        user, password = self.credStore.getAsUserPassword(self.postgresCredential)
        engine = createEngine(self.mergeStore, user, password)

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

        print(f"Resetting batch state for platform {self.name}, store {storeName}" +
              (f", dataset {datasetName}" if datasetName else ""))
        print(f"Keys to reset: {keys_to_reset}")

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
                            print(f"  Key '{key}': SKIPPED - Batch {current_batch_id} is COMMITTED and cannot be reset")
                            print("    Committed batches are immutable to preserve data integrity")
                            print("    If you need to reprocess data, start a new batch instead")
                            return "ERROR: Batch is COMMITTED and cannot be reset"
                        else:
                            print(f"  Key '{key}': Batch {current_batch_id} status is '{batch_status}' - safe to reset")

                            # Proceed with reset - Get the datastore to access datasets
                            datastore_ce: Optional[DatastoreCacheEntry] = eco.cache_getDatastore(storeName)
                            if datastore_ce is None:
                                print(f"    ERROR: Could not find datastore '{storeName}' in ecosystem")
                                return "ERROR: Could not find datastore in ecosystem"

                            datastore: Datastore = datastore_ce.datastore
                            if datastore.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.MULTI_DATASET and datasetName is not None:
                                print("Cannot specify a dataset name for a multi-dataset datastore {datastore.name}")
                                return "ERROR: Cannot specify a dataset name for a multi-dataset datastore"
                            elif datastore.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET and datasetName is None:
                                print(f"Cannot reset a single-dataset datastore {datastore.name} without a dataset name")
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
                                        print(f"    Cleared {records_deleted} records from {staging_table_name}")
                                        datasets_cleared += 1

                                print(f"  Key '{key}': reset batch {current_batch_id} successfully")
                                print(f"    Reset state and cleared staging data for {datasets_cleared} datasets")
                            else:
                                print(f"    ERROR: Could not find batch state for batch {current_batch_id}")
                                return "ERROR: Could not find batch state for batch {current_batch_id}"

        print("Batch state reset complete")
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
