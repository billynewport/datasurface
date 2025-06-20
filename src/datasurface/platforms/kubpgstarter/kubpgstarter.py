"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, DataPlatformExecutor, Documentation, Ecosystem, ValidationTree, CloudVendor, DataContainer, \
    PlatformPipelineGraph, DataPlatformGraphHandler, PostgresDatabase
from typing import Any, Optional
from datasurface.md import LocationKey, Credential, KafkaServer, Datastore, KafkaIngestion, ProblemSeverity, UnsupportedIngestionType, \
    DatastoreCacheEntry, IngestionConsistencyType, DatasetConsistencyNotSupported, \
    DataTransformerNode, DataTransformer, HostPortPair, HostPortPairList
from datasurface.md.lint import ObjectWrongType, ObjectMissing, UnknownObjectReference, UnexpectedExceptionProblem
from datasurface.md.exceptions import ObjectDoesntExistException
from jinja2 import Environment, PackageLoader, select_autoescape, Template
from datasurface.md.credential import CredentialStore, CredentialType, CredentialTypeNotSupportedProblem, CredentialNotAvailableException, \
    CredentialNotAvailableProblem
from datasurface.md import SchemaProjector, DataContainerNamingMapper, Dataset
import os
import re


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


class KubernetesPGStarterPlatformExecutor(DataPlatformExecutor):
    def __init__(self):
        super().__init__()

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass


class KPSGraphHandler(DataPlatformGraphHandler):
    """This takes the graph and then implements the data pipeline described in the graph using the technology stack
    pattern implemented by this platform. This platform supports ingesting data from Kafka confluence connectors. It
    takes the data from kafka topics and writes them to a postgres staging table. A seperate job scheduled by airflow
    then runs periodically and merges the staging data in to a MERGE table as a batch. Any workspaces can also query the
    data in the MERGE tables through Workspace specific views."""
    def __init__(self, dp: 'KubernetesPGStarterDataPlatform', graph: PlatformPipelineGraph) -> None:
        super().__init__(graph)
        self.dp: KubernetesPGStarterDataPlatform = dp
        self.env: Environment = Environment(
            loader=PackageLoader('datasurface.platforms.kubpgstarter.templates', 'jinja'),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def getInternalDataContainers(self) -> set[DataContainer]:
        """This returns any DataContainers used by the platform."""
        return {self.dp.kafkaConnectCluster, self.dp.mergeStore}

    def createJinjaTemplate(self, name: str) -> Template:
        template: Template = self.env.get_template(name, None)
        return template

    def createTerraformForAllIngestedNodes(self, eco: Ecosystem, tree: ValidationTree) -> str:
        """This creates a terraform file for all the ingested nodes in the graph using jinja templates which are found
        in the templates directory. It creates a sink connector to copy from the topics to a postgres
        staging file and a cluster link resource if required to recreate the topics on this cluster.
        Any errors during generation will just be added as ValidationProblems to the tree using the appropriate
        subtree so the user can see which object caused the errors. The caller method will check if the tree
        has errors and stop the generation process."""
        template: Template = self.createJinjaTemplate('kafka_topic_to_staging.jinja2')

        # Ensure the platform is the correct type early on
        if not isinstance(self.graph.platform, KubernetesPGStarterDataPlatform):
            print("Error: Platform associated with the graph is not a KPSGraphHandler.")
            tree.addRaw(ObjectWrongType(self.graph.platform, KubernetesPGStarterDataPlatform, ProblemSeverity.ERROR))
            return ""

        platform: KubernetesPGStarterDataPlatform = self.graph.platform

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

                else:
                    tree.addRaw(ObjectMissing(store, "cmd is none or is not KafkaIngestion", ProblemSeverity.ERROR))
                    continue

            except ObjectDoesntExistException:
                tree.addRaw(UnknownObjectReference(storeName, ProblemSeverity.ERROR))
                return ""

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
                "database_host": platform.mergeStore.connection.hostName,
                "database_port": platform.mergeStore.connection.port,
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
                cmdTree.addRaw(DatasetConsistencyNotSupported(store, store.cmd.singleOrMultiDatasetIngestion, self.graph.platform, ProblemSeverity.ERROR))

    def lintGraph(self, eco: Ecosystem, credStore: 'CredentialStore', tree: ValidationTree) -> None:
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

        if not isinstance(self.graph.platform, KubernetesPGStarterDataPlatform):
            tree.addRaw(ObjectWrongType(self.graph.platform, KubernetesPGStarterDataPlatform, ProblemSeverity.ERROR))
            return

        # Lets make sure only kafa ingestions are used.
        for storeName in self.graph.storesToIngest:
            store: Datastore = eco.cache_getDatastoreOrThrow(storeName).datastore
            storeTree: ValidationTree = tree.addSubTree(store)
            if store.cmd is None:
                storeTree.addRaw(ObjectMissing(store, KafkaIngestion, ProblemSeverity.ERROR))
                return
            elif not isinstance(store.cmd, KafkaIngestion):
                storeTree.addRaw(UnsupportedIngestionType(store, self.graph.platform, ProblemSeverity.ERROR))
            else:
                self.lintKafkaIngestion(store, storeTree)

        # Check CodeArtifacts on DataTransformer nodes are compatible with the PSP on the Ecosystem
        if eco.platformServicesProvider is not None:
            for node in self.graph.nodes.values():
                if isinstance(node, DataTransformerNode):
                    if node.workspace.dataTransformer is not None:
                        dt: DataTransformer = node.workspace.dataTransformer
                        dt.code.lint(eco, tree)

    def createAirflowDAG(self, eco: Ecosystem, issueTree: ValidationTree) -> str:
        """This creates an AirFlow DAG containing the MERGE task for every ingestion (store or dataset depending on whether
        the model specifies single or multidataset)"""
        raise NotImplementedError("Airflow DAG generation is not implemented")

    def renderGraph(self, credStore: 'CredentialStore', issueTree: ValidationTree) -> dict[str, str]:
        """This is called by the RenderEngine to instruct a DataPlatform to render the
        intention graph that it manages. For this platform it returns a dictionary containing a terraform
        file which configures all the kafka connect sink connectors to copy datastores using
        kafka ingestion capture meta data to postgres staging tables. It also needs to create
        an AirFlow DAG containing the MERGE task for every ingestion (store or dataset depending on whether
        the model specifies single or multidataset)"""
        # First create the terraform file
        terraform_code: str = self.createTerraformForAllIngestedNodes(self.graph.eco, issueTree)
        # Then create the AirFlow DAG
        airflow_dag: str = self.createAirflowDAG(self.graph.eco, issueTree)
        return {
            "terraform_code": terraform_code,
            "airflow_dag": airflow_dag
        }


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

    def projectDatasetSchema(self, dataset: 'Dataset') -> Optional['SchemaProjector']:
        """Returns None as this is handled by the underlying Kafka server."""
        return None

    def getNamingAdapter(self) -> Optional['DataContainerNamingMapper']:
        """Returns None as naming is handled by the platform."""
        return None


class KubernetesPGStarterDataPlatform(DataPlatform):
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
            airflowName: str = "airflow",
            postgresName: str = "pg-data",
            kafkaConnectName: str = "kafka-connect",
            kafkaClusterName: str = "kafka-cluster",
            slackChannel: str = "datasurface-events",
            datasurfaceImage: str = "datasurface/datasurface:latest"
            ):
        super().__init__(name, doc, KubernetesPGStarterPlatformExecutor())
        self.locs: set[LocationKey] = locs
        self.namespace: str = namespace
        self.connectCredentials: Credential = connectCredentials
        self.postgresCredential: Credential = postgresCredential
        self.airflowName: str = airflowName
        self.postgresName: str = postgresName
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

        self.mergeStore = PostgresDatabase(
            name=f"{postgresName}-db",
            connection=HostPortPair(f"{postgresName}.{namespace}.svc.cluster.local", 5432),
            locations=self.locs,
            databaseName="datasurface_merge"
        )
        self.credStore = KubernetesEnvVarsCredentialStore(
            name=f"{name}-cred-store",
            locs=self.locs,
            namespace=namespace
        )

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
                "namespace": self.namespace,
                "airflowName": self.airflowName,
                "postgresName": self.postgresName,
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
                "locs": [loc.to_json() for loc in self.locs]
            }
        )
        return rc

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return False

    def lint(self, eco: Ecosystem, tree: ValidationTree) -> None:
        """This should validate the platform and its associated parts but it cannot validate the usage of the DataPlatform
        as the graph must be generated for that to happen. The lintGraph method on the KPSGraphHandler does
        that as well as generating the terraform, airflow and other artifacts."""
        super().lint(eco, tree)
        if self.postgresCredential.credentialType != CredentialType.USER_PASSWORD:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.postgresCredential, [CredentialType.USER_PASSWORD]))
        if self.connectCredentials.credentialType != CredentialType.API_TOKEN:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.connectCredentials, [CredentialType.API_TOKEN]))
        if self.gitCredential.credentialType != CredentialType.API_TOKEN:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.gitCredential, [CredentialType.API_TOKEN]))
        if self.slackCredential.credentialType != CredentialType.API_TOKEN:
            tree.addRaw(CredentialTypeNotSupportedProblem(self.slackCredential, [CredentialType.API_TOKEN]))
        self.kafkaConnectCluster.lint(eco, tree.addSubTree(self.kafkaConnectCluster))
        self.mergeStore.lint(eco, tree.addSubTree(self.mergeStore))
        for loc in self.locs:
            loc.lint(tree.addSubTree(loc))

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        """This is called to handle merge events on the revised graph."""
        return KPSGraphHandler(self, graph)

    def _getKafkaBootstrapServers(self) -> str:
        """Calculate the Kafka bootstrap servers from the created Kafka cluster."""
        return f"{self.kafkaClusterName}-service.{self.namespace}.svc.cluster.local:9092"

    def generateBootstrapArtifacts(self) -> dict[str, str]:
        """This generates a kubernetes yaml file for the data platform using a jinja2 template.
        This doesn't need an intention graph, it's just for boot-strapping.
        Our bootstrap file would be a postgres instance, a kafka cluster, a kafka connect cluster and an airflow instance. It also
        needs to create the DAG for the infrastructure."""

        # Create Jinja2 environment
        env: Environment = Environment(
            loader=PackageLoader('datasurface.platforms.kubpgstarter.templates', 'jinja'),
            autoescape=select_autoescape(['html', 'xml'])
        )

        # Load the bootstrap template
        kubernetes_template: Template = env.get_template('kubernetes_services.j2')

        # Load the infrastructure DAG template
        dag_template: Template = env.get_template('infrastructure_dag.py.j2')

        # Prepare template context with all required variables
        context: dict[str, Any] = {
            "namespace_name": self.namespace,
            "platform_name": self.name,
            "postgres_hostname": self.postgresName,
            "postgres_credential_secret_name": self.postgresCredential.name,
            "airflow_name": self.airflowName,
            "airflow_credential_secret_name": self.postgresCredential.name,  # Airflow uses postgres creds
            "kafka_cluster_name": self.kafkaClusterName,
            "kafka_connect_name": self.kafkaConnectName,
            "kafka_connect_credential_secret_name": self.connectCredentials.name,
            "kafka_bootstrap_servers": self._getKafkaBootstrapServers(),
            "datasurface_docker_image": self.datasurfaceImage,
            "git_credential_secret_name": self.gitCredential.name,
            "slack_credential_secret_name": self.slackCredential.name,
            "slack_channel_name": self.slackChannel
        }

        # Render the templates
        rendered_yaml: str = kubernetes_template.render(context)
        rendered_dag: str = dag_template.render(context)

        # Return as dictionary with filename as key
        return {
            "kubernetes-bootstrap.yaml": rendered_yaml,
            f"{self.name}_infrastructure_dag.py": rendered_dag
        }
