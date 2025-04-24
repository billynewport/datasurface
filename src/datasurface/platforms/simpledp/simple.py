"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, DataPlatformExecutor, Documentation, Ecosystem, ValidationTree, CloudVendor, DataContainer, \
    PlatformPipelineGraph, DataPlatformGraphHandler, PostgresDatabase
from typing import Any, Optional
from datasurface.md import LocationKey, Credential, JSONable, KafkaServer, Datastore, KafkaIngestion, ProblemSeverity, UnsupportedIngestionType, \
    DatastoreCacheEntry, FileSecretCredential, IngestionConsistencyType, DatasetConsistencyNotSupported, Schema, PrimaryKeyList
from datasurface.md.lint import ObjectWrongType, ObjectMissing
from datasurface.md.exceptions import ObjectDoesntExistException
from jinja2 import Environment, PackageLoader, select_autoescape, Template
from datasurface.md import CredentialStore


class SimplePlatformExecutor(DataPlatformExecutor):
    def __init__(self):
        super().__init__()

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass


class SimpleDataPlatformHandler(DataPlatformGraphHandler):
    """This takes the graph and then implements the data pipeline described in the graph using the technology stack
    pattern implemented by this platform. This platform supports ingesting data from Kafka confluence connectors. It
    takes the data from kafka topics and writes them to a postgres staging table. A seperate job scheduled by airflow
    then runs periodically and merges the staging data in to a MERGE table as a batch. Any workspaces can also query the
    data in the MERGE tables through Workspace specific views."""
    def __init__(self, graph: PlatformPipelineGraph) -> None:
        super().__init__(graph)
        self.env: Environment = Environment(
            loader=PackageLoader('datasurface.platforms.simpledp.templates', 'jinja'),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def getInternalDataContainers(self) -> set[DataContainer]:
        """This returns any DataContainers used by the platform."""
        return set()

    def createTemplate(self, name: str) -> Template:
        template: Template = self.env.get_template(name, None)
        return template

    def getPostgresUserPasswordFromCredential(self, credential: Credential) -> tuple[str, str]:
        if not isinstance(credential, FileSecretCredential):
            # Consider how to handle other credential types or raise a more specific error
            raise ValueError("Credential is not a FileSecretCredential")
        try:
            with open(credential.secretFilePath, 'r') as file:
                lines = file.readlines()
                if len(lines) >= 2:
                    return lines[0].strip(), lines[1].strip()
                else:
                    # Handle file format error (e.g., less than 2 lines)
                    raise ValueError(f"Credential file {credential.secretFilePath} has incorrect format.")
        except FileNotFoundError:
            raise ValueError(f"Credential file {credential.secretFilePath} not found.")
        except Exception as e:
            # Catch other potential file reading errors
            raise ValueError(f"Error reading credential file {credential.secretFilePath}: {e}")

    def createTerraformForAllIngestedNodes(self, eco: Ecosystem, tree: ValidationTree) -> None:
        """This creates a terraform file for all the ingested nodes in the graph using jinja templates which are found
        in the templates directory. It creates a sink connector to copy from the topics to a postgres
        staging file and a cluster link resource if required to recreate the topics on this cluster.
        Any errors during generation will just be added as ValidationProblems to the tree using the appropriate
        subtree so the user can see which object caused the errors. The caller method will check if the tree
        has errors and stop the generation process."""
        template: Template = self.createTemplate('kafka_topic_to_staging.jinja2')

        # Ensure the platform is the correct type early on
        if not isinstance(self.graph.platform, SimpleDataPlatform):
            print("Error: Platform associated with the graph is not a SimpleDataPlatform.")
            return

        platform: SimpleDataPlatform = self.graph.platform

        ingest_nodes: list[dict[str, Any]] = []  # List to hold node data for the template

        # Iterate through the names of datastores to be ingested by this platform
        for storeName in self.graph.storesToIngest:
            try:
                datastoreEntry: DatastoreCacheEntry = eco.cache_getDatastoreOrThrow(storeName)
                datastore: Datastore = datastoreEntry.datastore

                if datastore.cmd and isinstance(datastore.cmd, KafkaIngestion):
                    ingestionNode: KafkaIngestion = datastore.cmd

                    topicNames: dict[str, str] = ingestionNode.topicForDataset

                    # We need to generate a resource for each datastore/dataset pair.
                    # This is because the connector can only support a single topic.
                    # So we need to create a new connector for each dataset.
                    # We will use the same connector for all datasets on the same datastore.
                    # We will use the same connector for all datasets on the same datastore.

                    for dataset in datastore.datasets.values():
                        # Prepare data for a single node for the template list
                        # Use dataset name as the topic name unless a mapping is provided.
                        topicName: str = dataset.name
                        if topicNames[dataset.name]:
                            topicName = topicNames[dataset.name]
                        if dataset.originalSchema is None:
                            tree.addSubTree(dataset).addRaw(ObjectMissing(dataset, Schema, ProblemSeverity.ERROR))
                        else:
                            if dataset.originalSchema.primaryKeyColumns is None:
                                tree.addSubTree(dataset.originalSchema).addRaw(ObjectMissing(dataset.originalSchema, PrimaryKeyList, ProblemSeverity.ERROR))
                            else:
                                node_data: dict[str, Any] = {
                                    "connector_name": f"jdbc_sink_{datastore.name}_{dataset.name}",
                                    "kafka_topic": topicName,  # TEMPLATE uses singular topic
                                    "target_table_name": f"staging_{datastore.name}_{topicName}",  # TEMPLATE needs this
                                    "tasks_max": 1,  # Default or get from ingestionNode/datastore if available
                                    "input_data_format": "AVRO",  # Placeholder - get from ingestionNode/datastore schema if available
                                    "primary_key_fields": dataset.originalSchema.primaryKeyColumns.colNames
                                    # Add node.connector_config_overrides if needed/available
                                }
                                ingest_nodes.append(node_data)

                else:
                    print(f"Warning: Datastore '{storeName}' does not have KafkaIngestion metadata or cmd is None. Skipping.")
                    continue

            except ObjectDoesntExistException as e:
                print(f"Error: Datastore '{storeName}' mentioned in graph not found in ecosystem cache: {e}")
                continue
            except ValueError as e:
                print(f"Error processing credentials for datastore '{storeName}': {e}")
                continue  # Skip node if credential processing fails

        # Prepare the global context for the template
        try:
            pg_user, pg_password = self.getPostgresUserPasswordFromCredential(platform.postgresCredential)
            # Prepare Kafka API keys if needed (using connectCredentials)
            # kafka_api_key, kafka_api_secret = self.getKafkaKeysFromCredential(platform.connectCredentials)
            # TODO: Implement getKafkaKeysFromCredential similar to getPostgresUserPasswordFromCredential
            kafka_api_key = "placeholder_kafka_api_key"  # Placeholder
            kafka_api_secret = "placeholder_kafka_api_secret"  # Placeholder

            context: dict[str, Any] = {
                "ingest_nodes": ingest_nodes,
                "database_host": platform.mergeStore.connection.hostName,
                "database_port": platform.mergeStore.connection.port,
                "database_name": platform.mergeStore.databaseName,
                "database_user": pg_user,
                "database_password": pg_password,
                "kafka_api_key": kafka_api_key,  # Placeholder
                "kafka_api_secret": kafka_api_secret,  # Placeholder
                # Add default_connector_config if defined in SimpleDataPlatform
            }

            # Render the template once with the full context
            code: str = template.render(context)

            # TODO: Decide what to do with the generated 'code' string (write to file, etc.)
            print(f"Generated Terraform code:\n{code}")

        except ValueError as e:
            print(f"Error processing platform credentials: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during template rendering: {e}")

    def lintKafkaIngestion(self, store: Datastore, storeTree: ValidationTree):
        """Kafka ingestions can only be single dataset. Each dataset is published on a different topic."""
        if not isinstance(store.cmd, KafkaIngestion):
            storeTree.addRaw(UnsupportedIngestionType(store, self.graph.platform, ProblemSeverity.ERROR))
        else:
            cmdTree: ValidationTree = storeTree.addSubTree(store.cmd)
            if store.cmd.singleOrMultiDatasetIngestion is None:
                cmdTree.addRaw(ObjectMissing(store, IngestionConsistencyType, ProblemSeverity.ERROR))
            elif store.cmd.singleOrMultiDatasetIngestion != IngestionConsistencyType.SINGLE_DATASET:
                cmdTree.addRaw(DatasetConsistencyNotSupported(store, store.cmd.singleOrMultiDatasetIngestion, self.graph.platform, ProblemSeverity.ERROR))

    def lintGraph(self, eco: Ecosystem, credStore: 'CredentialStore', tree: ValidationTree):
        """This should be called execute graph. This is where the graph is validated and any issues are reported. If there are
        no issues then the graph is executed. Executed here means
        1. Terraform file which creates kafka connect connectors to ingest topics to postgres tables.
        2. Airflow DAG which has all the needed MERGE jobs. The Jobs in Airflow should be parameterized and the parameters
        would have enough information such as DataStore name, Datasets names. The Ecosystem git clone should be on the local file
        system and the path provided to the DAG. It can then get everything it needs from that.

        The MERGE job are responsible for node type reconciliation such as creating tables/views for staging, merge tables
        and Workspace views. It's also responsible to keep them up to date. This happens before the merge job is run. This seems
        like something that will be done by a DataSurface service as it's pretty standard.

        As validation/linting errors are found then they are added as ValidationProblems to the tree."""

        if not isinstance(self.graph.platform, SimpleDataPlatform):
            tree.addRaw(ObjectWrongType(self.graph.platform, SimpleDataPlatform, ProblemSeverity.ERROR))

        # Lets make sure only kafa ingestions are used.
        for storeName in self.graph.storesToIngest:
            store: Datastore = eco.cache_getDatastoreOrThrow(storeName).datastore
            storeTree: ValidationTree = tree.addSubTree(store)
            if store.cmd is None:
                storeTree.addRaw(ObjectMissing(store, KafkaIngestion, ProblemSeverity.ERROR))
            elif not isinstance(store.cmd, KafkaIngestion):
                storeTree.addRaw(UnsupportedIngestionType(store, self.graph.platform, ProblemSeverity.ERROR))
            else:
                self.lintKafkaIngestion(store, storeTree)

    def renderGraph(self, credStore: 'CredentialStore', issueTree: ValidationTree):
        """This is called by the RenderEngine to instruct a DataPlatform to render the
        intention graph that it manages."""
        pass


class KafkaConnectCluster(DataContainer, JSONable):
    def __init__(self, name: str, locs: set[LocationKey], restAPIUrlString: str, kafkaServer: KafkaServer, caCert: Optional[Credential] = None) -> None:
        super().__init__(name, locs)
        self.restAPIUrlString: str = restAPIUrlString
        self.kafkaServer: KafkaServer = kafkaServer
        self.caCertificate: Optional[Credential] = caCert

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
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


class SimpleDataPlatform(DataPlatform):
    """This defines the simple data platform. It can consume data from sources and write them to a postgres based merge store.
      It has the use of a postgres database for staging and merge tables as well as Workspace views"""
    def __init__(
            self,
            name: str,
            doc: Documentation,
            kafkaConnectCluster: KafkaConnectCluster,
            connectCredentials: Credential,
            mergeStore: PostgresDatabase,
            postgresCredential: Credential
            ):
        super().__init__(name, doc, SimplePlatformExecutor())
        self.kafkaConnectCluster: KafkaConnectCluster = kafkaConnectCluster
        self.connectCredentials: Credential = connectCredentials
        self.mergeStore: PostgresDatabase = mergeStore
        self.postgresCredential: Credential = postgresCredential

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "mergeStore": self.mergeStore.to_json(),
                "connectCredentials": self.connectCredentials.to_json(),
                "postgresCredential": self.postgresCredential.to_json(),
                "kafkaConnectCluster": self.kafkaConnectCluster.to_json()
            }
        )
        return rc

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return False

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        """This should validate the platform and its associated parts but it cannot validate the usage of the DataPlatform
        as the graph must be generated for that to happen. The lintGraph method on the SimpleDataPlatformHandler does
        that as well as generating the terraform, airflow and other artifacts."""
        super().lint(eco, tree)
        if not isinstance(self.postgresCredential, FileSecretCredential):
            tree.addRaw(ObjectWrongType(self.postgresCredential, FileSecretCredential, ProblemSeverity.ERROR))
        self.kafkaConnectCluster.lint(eco, tree)
        self.mergeStore.lint(eco, tree)
        self.connectCredentials.lint(eco, tree)

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        return SimpleDataPlatformHandler(graph)
