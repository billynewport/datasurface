"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import Any, Generic, Optional, TypeVar
from jinja2 import Environment, Template
from datasurface.md import StorageRequirement
import re
import urllib.parse
from abc import ABC, abstractmethod
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger
)
from datasurface.md import UserDSLObject, ValidationTree, ProblemSeverity
from datasurface.md.credential import Credential
from datasurface.md import HostPortSQLDatabase, PostgresDatabase
from datasurface.md import HostPortPair
from datasurface.md import Ecosystem
from datasurface.platforms.yellow.db_utils import getDriverNameAndQueryForDataContainer


# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


class K8sUtils:
    @staticmethod
    def to_k8s_name(name: str) -> str:
        """Convert a name to a valid Kubernetes resource name (RFC 1123)."""
        name = name.lower().replace('_', '-').replace(' ', '-')
        name = re.sub(r'[^a-z0-9-]', '', name)
        name = re.sub(r'-+', '-', name)
        name = name.strip('-')
        return name

    @staticmethod
    def isLegalKubernetesNamespaceName(name: str) -> bool:
        """Check if the name is a valid Kubernetes namespace (RFC 1123 label)."""
        return re.match(r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$', name) is not None


# Model for Kubernetes resource limits
# Allows the requested and limit for CPU and memory to be specified for a pod.
class K8sResourceLimits(UserDSLObject):
    def __init__(self, requested_memory: StorageRequirement, limits_memory: StorageRequirement, requested_cpu: float, limits_cpu: float) -> None:
        super().__init__()
        self.requested_memory: StorageRequirement = requested_memory
        self.limits_memory: StorageRequirement = limits_memory
        self.requested_cpu: float = requested_cpu
        self.limits_cpu: float = limits_cpu

    def to_json(self) -> dict[str, Any]:
        """Convert K8sResourceLimits to JSON-serializable dictionary."""
        return {
            "requested_memory": self.requested_memory.to_json(),
            "limits_memory": self.limits_memory.to_json(),
            "requested_cpu": self.requested_cpu,
            "limits_cpu": self.limits_cpu
        }

    def to_k8s_json(self) -> dict[str, Any]:
        """Convert K8sResourceLimits to Kubernetes JSON format."""
        return {
            "requested_memory": Component.storageToKubernetesFormat(self.requested_memory),
            "limits_memory": Component.storageToKubernetesFormat(self.limits_memory),
            "requested_cpu": Component.cpuToKubernetesFormat(self.requested_cpu),
            "limits_cpu": Component.cpuToKubernetesFormat(self.limits_cpu)
            }

    def lint(self, tree: ValidationTree):
        """This validates the K8sResourceLimits object."""
        self.requested_memory.lint(tree.addSubTree(self.requested_memory))
        self.limits_memory.lint(tree.addSubTree(self.limits_memory))
        if self.requested_memory > self.limits_memory:
            tree.addProblem(f"Requested memory must be less than or equal to limits memory, got {self.requested_memory} and {self.limits_memory}")
        if self.requested_cpu < 0:
            tree.addProblem(f"Requested CPU must be greater than 0, got {self.requested_cpu}")
        if self.limits_cpu < 0:
            tree.addProblem(f"Limits CPU must be greater than 0, got {self.limits_cpu}")
        if self.requested_cpu > self.limits_cpu:
            tree.addProblem(f"Requested CPU must be less than or equal to limits CPU, got {self.requested_cpu} and {self.limits_cpu}")


class GitCacheConfig(UserDSLObject):
    def __init__(self,
                 enabled: bool = True, pvc_name: str = "git-cache-pvc", pv_name: str = "git-cache-pv",
                 storage_size: StorageRequirement = StorageRequirement("5G"), storageClass: str = "standard",
                 access_mode: str = "ReadWriteMany", git_cache_max_age_minutes: int = 5, cacheLocalPath: str = "/cache/git-cache") -> None:
        super().__init__()
        self.enabled: bool = enabled
        self.pvc_name: str = pvc_name
        self.pv_name: str = pv_name
        self.storage_size: StorageRequirement = storage_size
        self.storageClass: str = storageClass
        self.access_mode: str = access_mode
        self.git_cache_max_age_minutes: int = git_cache_max_age_minutes
        self.cacheLocalPath: str = cacheLocalPath

    def to_json(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "pvc_name": self.pvc_name,
            "pv_name": self.pv_name,
            "storage_size": self.storage_size.to_json(),
            "storageClass": self.storageClass,
            "access_mode": self.access_mode,
            "git_cache_max_age_minutes": self.git_cache_max_age_minutes,
            "cacheLocalPath": self.cacheLocalPath
        }

    def lint(self, tree: ValidationTree) -> None:
        self.storage_size.lint(tree.addSubTree(self.storage_size))
        if self.access_mode not in ["ReadWriteOnce", "ReadWriteMany"]:
            tree.addProblem(f"Invalid access mode: {self.access_mode}")

    def mergeToYAMLContext(self, eco: Ecosystem) -> dict[str, Any]:
        rc: dict[str, Any] = dict()
        rc.update(
            {
                "git_cache_access_mode": self.access_mode,
                "git_clone_cache_pvc": self.pvc_name,
                "git_clone_cache_pv": self.pv_name,
                "git_cache_storage_size": Component.storageToKubernetesFormat(self.storage_size),
                "git_cache_max_age_minutes": self.git_cache_max_age_minutes,
                "git_cache_enabled": self.enabled,
                "git_cache_local_path": self.cacheLocalPath
            }
        )
        return rc


class Component(ABC):
    """A component can a hostname, a credential and"""
    def __init__(self, name: str, namespace: str) -> None:
        super().__init__()
        self.name: str = name
        self.namespace: str = namespace

    @abstractmethod
    def render(self, env: Environment, templateContext: dict[str, Any]) -> str:
        """This renders the component to a string."""
        raise NotImplementedError("This is an abstract method")

    @staticmethod
    def storageToKubernetesFormat(s: StorageRequirement) -> str:
        """Convert storage spec to Kubernetes format by ensuring binary units have 'i' suffix"""
        if s.spec[-1].upper() in ['G', 'T', 'P', 'E', 'Z', 'Y']:
            # Binary units in Kubernetes should have 'i' suffix
            return s.spec + 'i'
        # For other units like M, K, B, return as-is
        return s.spec

    @staticmethod
    def cpuToKubernetesFormat(cpu: float) -> str:
        """Convert CPU spec to Kubernetes format by ensuring binary units have 'm' suffix"""
        return f"{int(cpu * 1000)}m"


class NamespaceComponent(Component):
    def __init__(self, name: str, namespace: str) -> None:
        super().__init__(name, namespace)

    def render(self, env: Environment, templateContext: dict[str, Any]) -> str:
        yaml: str = ""
        namespace_template: Template = env.get_template('psp_namespace.yaml.j2')
        ctxt: dict[str, Any] = templateContext.copy()
        ctxt.update(
            {
                "namespace_name": K8sUtils.to_k8s_name(self.namespace)
            }
        )
        namespace_rendered: str = namespace_template.render(ctxt)
        yaml += namespace_rendered
        return yaml


class LoggingComponent(Component):
    def __init__(self, psp_name: str, namespace: str) -> None:
        super().__init__(psp_name, namespace)

    def render(self, env: Environment, templateContext: dict[str, Any]) -> str:
        yaml: str = ""
        logging_template: Template = env.get_template('psp_logging.yaml.j2')
        ctxt: dict[str, Any] = templateContext.copy()
        ctxt.update(
            {
                "namespace_name": K8sUtils.to_k8s_name(self.namespace),
                "psp_k8s_name": K8sUtils.to_k8s_name(self.name),
                "psp_name": self.name,
            }
        )
        logging_rendered: str = logging_template.render(ctxt)
        yaml += logging_rendered
        return yaml


class NetworkPolicyComponent(Component):
    def __init__(self, psp_name: str, namespace: str) -> None:
        super().__init__(psp_name, namespace)

    def render(self, env: Environment, templateContext: dict[str, Any]) -> str:
        yaml: str = ""
        network_policy_template: Template = env.get_template('psp_networkpolicy.yaml.j2')
        ctxt: dict[str, Any] = templateContext.copy()
        ctxt.update(
            {
                "namespace_name": K8sUtils.to_k8s_name(self.namespace),
                "psp_k8s_name": K8sUtils.to_k8s_name(self.name),
                "psp_name": self.name,
            }
        )
        network_policy_rendered: str = network_policy_template.render(ctxt)
        yaml += network_policy_rendered
        return yaml


class Airflow281Component(Component):
    def __init__(self, name: str, namespace: str, dbCred: Credential, db: HostPortSQLDatabase,
                 dagCreds: list[Credential], webserverResourceLimits: Optional[K8sResourceLimits] = None,
                 schedulerResourceLimits: Optional[K8sResourceLimits] = None, airflow_image: str = "apache/airflow:2.11.0") -> None:
        super().__init__(name, namespace)
        self.dbCred: Credential = dbCred
        self.db: HostPortSQLDatabase = db
        self.dagCreds: list[Credential] = dagCreds
        self.webserverResourceLimits: K8sResourceLimits = webserverResourceLimits or K8sResourceLimits(
            requested_memory=StorageRequirement("1G"),
            limits_memory=StorageRequirement("2G"),
            requested_cpu=0.5,
            limits_cpu=1.0
            )
        self.schedulerResourceLimits: K8sResourceLimits = schedulerResourceLimits or K8sResourceLimits(
            requested_memory=StorageRequirement("2G"),
            limits_memory=StorageRequirement("4G"),
            requested_cpu=0.5,
            limits_cpu=1.0
        )
        self.airflow_image: str = airflow_image

    def render(self, env: Environment, templateContext: dict[str, Any]) -> str:
        yaml: str = ""
        airflow_template: Template = env.get_template('airflow281/psp_airflow.yaml.j2')
        ctxt: dict[str, Any] = templateContext.copy()
        driverName, query = getDriverNameAndQueryForDataContainer(self.db)

        # URL-encode the driver query parameter to handle spaces and special characters
        encoded_query = urllib.parse.quote_plus(query) if query else None

        ctxt.update(
            {
                "airflow_k8s_name": K8sUtils.to_k8s_name(self.name),
                "airflow_db_hostname": self.db.hostPortPair.hostName,
                "airflow_db_port": self.db.hostPortPair.port,
                "airflow_db_driver": driverName,
                "airflow_db_query": encoded_query,
                "airflow_db_credential_secret_name": K8sUtils.to_k8s_name(self.dbCred.name),  # Airflow uses merge db creds
                "extra_credentials": [K8sUtils.to_k8s_name(cred.name) for cred in self.dagCreds],
                "webserver_requested_memory": Component.storageToKubernetesFormat(self.webserverResourceLimits.requested_memory),
                "webserver_limits_memory": Component.storageToKubernetesFormat(self.webserverResourceLimits.limits_memory),
                "webserver_requested_cpu": Component.cpuToKubernetesFormat(self.webserverResourceLimits.requested_cpu),
                "webserver_limits_cpu": Component.cpuToKubernetesFormat(self.webserverResourceLimits.limits_cpu),
                "scheduler_requested_memory": Component.storageToKubernetesFormat(self.schedulerResourceLimits.requested_memory),
                "scheduler_limits_memory": Component.storageToKubernetesFormat(self.schedulerResourceLimits.limits_memory),
                "scheduler_requested_cpu": Component.cpuToKubernetesFormat(self.schedulerResourceLimits.requested_cpu),
                "scheduler_limits_cpu": Component.cpuToKubernetesFormat(self.schedulerResourceLimits.limits_cpu),
                "airflow_image": self.airflow_image
            }
        )
        airflow_rendered: str = airflow_template.render(ctxt)
        yaml += airflow_rendered
        return yaml


class PostgresComponent(Component):
    def __init__(self, name: str, namespace: str, dbCred: Credential, db: PostgresDatabase,
                 storageNeeds: StorageRequirement, resourceLimits: Optional[K8sResourceLimits] = None, postgres_image: str = "postgres:15") -> None:
        super().__init__(name, namespace)
        self.dbCred: Credential = dbCred
        self.db: PostgresDatabase = db
        self.resourceLimits: K8sResourceLimits = resourceLimits or K8sResourceLimits(
            requested_memory=StorageRequirement("1G"),
            limits_memory=StorageRequirement("2G"),
            requested_cpu=0.5,
            limits_cpu=1.0
        )
        self.storageNeeds: StorageRequirement = storageNeeds
        self.postgres_image: str = postgres_image

    def render(self, env: Environment, templateContext: dict[str, Any]) -> str:
        yaml: str = ""
        postgres_template: Template = env.get_template('psp_postgres.yaml.j2')

        # Add the postgres instance variables to a private template context
        ctxt: dict[str, Any] = templateContext.copy()
        ctxt.update(
            {
                "instance_name": K8sUtils.to_k8s_name(self.name),
                "postgres_hostname": self.db.hostPortPair.hostName,
                "postgres_port": self.db.hostPortPair.port,
                "postgres_credential_secret_name": K8sUtils.to_k8s_name(self.dbCred.name),
                "postgres_storage": Component.storageToKubernetesFormat(self.storageNeeds),
                "postgres_requested_memory": Component.storageToKubernetesFormat(self.resourceLimits.requested_memory),
                "postgres_limits_memory": Component.storageToKubernetesFormat(self.resourceLimits.limits_memory),
                "postgres_requested_cpu": Component.cpuToKubernetesFormat(self.resourceLimits.requested_cpu),
                "postgres_limits_cpu": Component.cpuToKubernetesFormat(self.resourceLimits.limits_cpu),
                "postgres_image": self.postgres_image
            }
        )
        postgres_rendered: str = postgres_template.render(ctxt)
        yaml += postgres_rendered
        return yaml


class PVCComponent(Component):
    def __init__(self, name: str, namespace: str, capacity: StorageRequirement, storageClass: str, accessMode: str) -> None:
        super().__init__(name, namespace)
        self.capacity: StorageRequirement = capacity
        self.storageClass: str = storageClass
        self.accessMode: str = accessMode

    def render(self, env: Environment, templateContext: dict[str, Any]) -> str:
        yaml: str = ""
        pvc_template: Template = env.get_template('psp_pvc.yaml.j2')
        ctxt: dict[str, Any] = templateContext.copy()
        ctxt.update(
            {
                "pvc_name": K8sUtils.to_k8s_name(self.name),
                "pvc_storage_class": self.storageClass,
                "pvc_access_mode": self.accessMode,
                "pvc_storage_size": Component.storageToKubernetesFormat(self.capacity)
            }
        )
        pvc_rendered: str = pvc_template.render(ctxt)
        yaml += pvc_rendered
        return yaml


class NFSComponent(Component):
    def __init__(self, name: str, namespace: str, nfs_server_node: str,
                 nfs_server_pvc_name: str, nfs_server_pv_name: str,
                 nfs_client_pvc_name: str, nfs_client_pv_name: str,
                 nfs_server_storage: StorageRequirement = StorageRequirement("20G"),
                 nfs_client_storage: StorageRequirement = StorageRequirement("10G"), nfs_server_host_path: Optional[str] = None,
                 nfs_server_image: str = "erichough/nfs-server:2.2.1") -> None:
        super().__init__(name, namespace)
        self.nfs_server_node: str = nfs_server_node
        self.nfs_server_storage: StorageRequirement = nfs_server_storage
        self.nfs_client_storage: StorageRequirement = nfs_client_storage
        self.nfs_server_host_path: str = nfs_server_host_path or f"/opt/{name}-nfs-storage"
        self.nfs_server_image: str = nfs_server_image
        name_k8s: str = K8sUtils.to_k8s_name(name)
        self.nfs_server_pvc_name: str = nfs_server_pvc_name or f"{name_k8s}-nfs-server-pvc"
        self.nfs_server_pv_name: str = nfs_server_pv_name or f"{name_k8s}-nfs-server-pv"
        self.nfs_client_pvc_name: str = nfs_client_pvc_name or f"{name_k8s}-nfs-client-pvc"
        self.nfs_client_pv_name: str = nfs_client_pv_name or f"{name_k8s}-nfs-client-pv"

    def render(self, env: Environment, templateContext: dict[str, Any]) -> str:
        yaml: str = ""
        nfs_template: Template = env.get_template('psp_nfs.yaml.j2')

        # Add the NFS instance variables to a private template context
        ctxt: dict[str, Any] = templateContext.copy()
        name_k8s: str = K8sUtils.to_k8s_name(self.name)
        ctxt.update(
            {
                "instance_name": name_k8s,
                "nfs_server_image": self.nfs_server_image,
                "nfs_server_node": self.nfs_server_node,
                "nfs_server_host_path": self.nfs_server_host_path,
                "nfs_server_storage": Component.storageToKubernetesFormat(self.nfs_server_storage),
                "nfs_service_name": f"{name_k8s}-nfs-service",
                "nfs_shared_path": "/shared",
                "nfs_client_storage": Component.storageToKubernetesFormat(self.nfs_client_storage),
                "nfs_server_pvc_name": self.nfs_server_pvc_name,
                "nfs_server_pv_name": self.nfs_server_pv_name,
                "nfs_client_pvc_name": self.nfs_client_pvc_name,
                "nfs_client_pv_name": self.nfs_client_pv_name,
                "nfs_server_memory_request": "1Gi",
                "nfs_server_memory_limit": "2Gi",
                "nfs_server_cpu_request": "500m",
                "nfs_server_cpu_limit": "1000m"
            }
        )
        nfs_rendered: str = nfs_template.render(ctxt)
        yaml += nfs_rendered
        return yaml


class Assembly:
    def __init__(self, name: str, namespace: str, components: list[Component]) -> None:
        super().__init__()
        self.name: str = name
        self.components: list[Component] = components
        self.namespace: str = namespace

    def generateYaml(self, env: Environment, templateContext: dict[str, Any]) -> str:
        """This generates the yaml for the assembly."""
        yaml_parts: list[str] = []
        for component in self.components:
            rendered = component.render(env, templateContext)
            if rendered.strip():  # Only add non-empty templates
                yaml_parts.append(rendered.rstrip())  # Remove trailing whitespace

        # Join with proper YAML document separator
        return '\n'.join(yaml_parts) + '\n'


DB = TypeVar('DB', bound=HostPortSQLDatabase)


class K8sAssemblyFactory(UserDSLObject, Generic[DB]):
    def __init__(self, name: str, namespace: str, git_cache_config: GitCacheConfig) -> None:
        super().__init__()
        self.name: str = name
        self.namespace: str = namespace
        self.git_cache_config: GitCacheConfig = git_cache_config

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "namespace": self.namespace,
            "git_cache": self.git_cache_config.to_json(),
            "_type": self.__class__.__name__
        }

    @abstractmethod
    def createAssembly(self, mergeRW_Credential: Credential, mergeDB: DB) -> Assembly:
        pass

    @abstractmethod
    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass

    @abstractmethod
    def createYAMLContext(self, eco: Ecosystem) -> dict[str, Any]:
        rc: dict[str, Any] = dict()
        rc.update(
            {
                "namespace_name": self.namespace,
                "psp_k8s_name": K8sUtils.to_k8s_name(self.name),
                "psp_name": self.name,
            }
        )
        rc.update(self.git_cache_config.mergeToYAMLContext(eco))
        return rc


class YellowSinglePostgresDatabaseAssembly(K8sAssemblyFactory[PostgresDatabase]):
    """
    This is an assembly where a kubernetes name space is configured. It has a PVC for a git clone cache
    and for a postgres database. The postgres database is created in a pod along with a pod for airflow
    which shared the postgres database.
    This is a relatively low performance setup, postgres running on top of longhorn won't be fast
    but it is functional. The details on the postgres to create are taken from the mergecontainer provided
    from the psp when create is called."""
    def __init__(
            self,
            name: str,
            namespace: str,
            git_cache_config: GitCacheConfig,
            nfs_server_node: str,
            afHostPortPair: HostPortPair,
            dbStorageNeeds: StorageRequirement,
            dbResourceLimits: Optional[K8sResourceLimits] = None,
            afWebserverResourceLimits: Optional[K8sResourceLimits] = None,
            afSchedulerResourceLimits: Optional[K8sResourceLimits] = None) -> None:
        super().__init__(name, namespace, git_cache_config)
        self.nfs_server_node: str = nfs_server_node
        self.afHostPortPair: HostPortPair = afHostPortPair
        self.dbStorageNeeds: StorageRequirement = dbStorageNeeds
        self.dbResourceLimits: Optional[K8sResourceLimits] = dbResourceLimits
        self.afWebserverResourceLimits: Optional[K8sResourceLimits] = afWebserverResourceLimits
        self.afSchedulerResourceLimits: Optional[K8sResourceLimits] = afSchedulerResourceLimits

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        self.git_cache_config.lint(tree.addSubTree(self.git_cache_config))
        self.dbStorageNeeds.lint(tree.addSubTree(self.dbStorageNeeds))
        if self.dbResourceLimits:
            self.dbResourceLimits.lint(tree.addSubTree(self.dbResourceLimits))
        if self.afWebserverResourceLimits:
            self.afWebserverResourceLimits.lint(tree.addSubTree(self.afWebserverResourceLimits))
        if self.afSchedulerResourceLimits:
            self.afSchedulerResourceLimits.lint(tree.addSubTree(self.afSchedulerResourceLimits))
        if not K8sUtils.isLegalKubernetesNamespaceName(self.namespace):
            tree.addProblem(
                f"Kubernetes namespace '{self.namespace}' is not a valid RFC 1123 label (must match ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$)",
                ProblemSeverity.ERROR
            )

    def createYAMLContext(self, eco: Ecosystem) -> dict[str, Any]:
        rc: dict[str, Any] = super().createYAMLContext(eco)
        rc.update(self.git_cache_config.mergeToYAMLContext(eco))
        return rc

    def createYellowAssemblySinglePostgresDatabase(
            self,
            name: str,
            nfs_server_node: str,
            dbRWCred: Credential,
            db: PostgresDatabase,
            afHostPortPair: HostPortPair,
            dbStorageNeeds: StorageRequirement,
            dbResourceLimits: Optional[K8sResourceLimits] = None,
            afWebserverResourceLimits: Optional[K8sResourceLimits] = None,
            afSchedulerResourceLimits: Optional[K8sResourceLimits] = None) -> Assembly:
        """This create the kubernetes yaml to build a single database YellowDataPlatform where a single postgres
        database is used for both the merge store and the airflow database."""

        assembly: Assembly = Assembly(
            name, self.namespace,
            components=list()
        )
        assembly.components.append(NamespaceComponent("ns", self.namespace))
        assembly.components.append(
            PVCComponent(
                self.git_cache_config.pvc_name, self.namespace,
                self.git_cache_config.storage_size, self.git_cache_config.storageClass,
                self.git_cache_config.access_mode))
        assembly.components.append(LoggingComponent(self.name, self.namespace))
        assembly.components.append(NetworkPolicyComponent(self.name, self.namespace))
        assembly.components.append(PostgresComponent("pg", self.namespace, dbRWCred, db, dbStorageNeeds, dbResourceLimits))
        # Airflow uses the same database as the merge store
        assembly.components.append(
            Airflow281Component("airflow", self.namespace, dbRWCred, db, [],
                                webserverResourceLimits=afWebserverResourceLimits, schedulerResourceLimits=afSchedulerResourceLimits))
        return assembly

    def createAssembly(self, mergeRW_Credential: Credential, mergeDB: PostgresDatabase) -> Assembly:
        """This creates the assembly for the kubernetes yaml file."""
        return self.createYellowAssemblySinglePostgresDatabase(
            self.name,
            self.nfs_server_node,
            mergeRW_Credential, mergeDB,
            self.afHostPortPair,
            self.dbStorageNeeds,
            self.dbResourceLimits,
            self.afWebserverResourceLimits,
            self.afSchedulerResourceLimits)


class YellowExternalSingleDatabaseAssembly(K8sAssemblyFactory[HostPortSQLDatabase]):
    """
    This is an assembly where a kubernetes name space is configured. It has a PVC for a git clone cache.
    The SQL database is external to the system and available using info from the psp merge container
    and the credential should have SA access.
    The airflow shares the SQL database.
    The SQL database will be the merge database and the airflow database.
    This is a potentially a high performance setup, postgres running externally is as fast as it
    is configured to be. The details on the SQL are taken from the mergecontainer provided
    from the psp when create is called."""
    def __init__(
            self,
            name: str,
            namespace: str,
            git_cache_config: GitCacheConfig,
            afHostPortPair: HostPortPair,
            afWebserverResourceLimits: Optional[K8sResourceLimits] = None,
            afSchedulerResourceLimits: Optional[K8sResourceLimits] = None) -> None:
        super().__init__(name, namespace, git_cache_config)
        self.afHostPortPair: HostPortPair = afHostPortPair
        self.afWebserverResourceLimits: Optional[K8sResourceLimits] = afWebserverResourceLimits
        self.afSchedulerResourceLimits: Optional[K8sResourceLimits] = afSchedulerResourceLimits

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        self.git_cache_config.lint(tree.addSubTree(self.git_cache_config))
        if self.afWebserverResourceLimits:
            self.afWebserverResourceLimits.lint(tree.addSubTree(self.afWebserverResourceLimits))
        if self.afSchedulerResourceLimits:
            self.afSchedulerResourceLimits.lint(tree.addSubTree(self.afSchedulerResourceLimits))
        if not K8sUtils.isLegalKubernetesNamespaceName(self.namespace):
            tree.addProblem(
                f"Kubernetes namespace '{self.namespace}' is not a valid RFC 1123 label (must match ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$)",
                ProblemSeverity.ERROR
            )

    def createYAMLContext(self, eco: Ecosystem) -> dict[str, Any]:
        rc: dict[str, Any] = super().createYAMLContext(eco)
        rc.update(self.git_cache_config.mergeToYAMLContext(eco))
        return rc

    def createYellowAssemblySingleDatabase(
            self,
            name: str,
            dbRWCred: Credential,
            db: HostPortSQLDatabase,
            afHostPortPair: HostPortPair,
            afWebserverResourceLimits: Optional[K8sResourceLimits] = None,
            afSchedulerResourceLimits: Optional[K8sResourceLimits] = None) -> Assembly:
        """This create the kubernetes yaml to build a single database YellowDataPlatform where a single SQL
        database is used for both the merge store and the airflow database."""

        assembly: Assembly = Assembly(
            name, self.namespace,
            components=list()
        )
        assembly.components.append(NamespaceComponent("ns", self.namespace))
        assembly.components.append(
            PVCComponent(
                self.git_cache_config.pvc_name, self.namespace,
                self.git_cache_config.storage_size, self.git_cache_config.storageClass,
                self.git_cache_config.access_mode))
        assembly.components.append(LoggingComponent(self.name, self.namespace))
        assembly.components.append(NetworkPolicyComponent(self.name, self.namespace))
        # Airflow uses the same database as the merge store
        assembly.components.append(
            Airflow281Component("airflow", self.namespace, dbRWCred, db, [],
                                webserverResourceLimits=afWebserverResourceLimits, schedulerResourceLimits=afSchedulerResourceLimits))
        return assembly

    def createAssembly(self, mergeRW_Credential: Credential, mergeDB: HostPortSQLDatabase) -> Assembly:
        """This creates the assembly for the kubernetes yaml file."""
        return self.createYellowAssemblySingleDatabase(
            self.name,
            mergeRW_Credential, mergeDB,
            self.afHostPortPair,
            self.afWebserverResourceLimits,
            self.afSchedulerResourceLimits)


class YellowTwinPostgresDatabaseAssembly(K8sAssemblyFactory[PostgresDatabase]):
    def __init__(self, name: str, namespace: str, git_cache_config: GitCacheConfig,
                 nfs_server_node: str,
                 mergeDBStorageNeeds: StorageRequirement,
                 afDBcred: Credential,
                 afDB: PostgresDatabase,
                 afDBStorageNeeds: StorageRequirement,
                 afHostPortPair: HostPortPair) -> None:
        super().__init__(name, namespace, git_cache_config)
        self.nfs_server_node: str = nfs_server_node
        self.mergeDBStorageNeeds: StorageRequirement = mergeDBStorageNeeds
        self.afDBcred: Credential = afDBcred
        self.afDB: PostgresDatabase = afDB
        self.afDBStorageNeeds: StorageRequirement = afDBStorageNeeds
        self.afHostPortPair: HostPortPair = afHostPortPair

    def createYellowAssemblyTwinDatabase(
            self,
            name: str,
            nfs_server_node: str,
            pgCred: Credential,
            mergeDBD: PostgresDatabase,
            mergeDBStorageNeeds: StorageRequirement,
            afDBcred: Credential,
            afDB: PostgresDatabase,
            afDBStorageNeeds: StorageRequirement,
            afHostPortPair: HostPortPair) -> Assembly:
        """This creates an assembly for a YellowDataPlatform where the merge engine and the airflow use seperate databases for performance
        reasons."""
        assembly: Assembly = Assembly(
            name, self.namespace,
            components=[
                NamespaceComponent("ns", self.namespace),
                LoggingComponent(self.name, self.namespace),
                NetworkPolicyComponent(self.name, self.namespace),
                PVCComponent(
                    self.git_cache_config.pvc_name, self.namespace,
                    self.git_cache_config.storage_size, self.git_cache_config.storageClass,
                    self.git_cache_config.access_mode),
                PostgresComponent("pg-merge", self.namespace, pgCred, mergeDBD, mergeDBStorageNeeds),
                PostgresComponent("pg-airflow", self.namespace, afDBcred, afDB, afDBStorageNeeds),
                Airflow281Component("airflow", self.namespace, afDBcred, afDB, [pgCred])  # Airflow needs the merge store database credentials for DAGs
            ]
        )
        return assembly

    def createAssembly(self, mergeRW_Credential: Credential, mergeDB: PostgresDatabase) -> Assembly:
        """This creates the assembly for the kubernetes yaml file."""
        return self.createYellowAssemblyTwinDatabase(
            self.name,
            self.nfs_server_node,
            mergeRW_Credential,
            mergeDB,
            self.mergeDBStorageNeeds,
            self.afDBcred,
            self.afDB,
            self.afDBStorageNeeds,
            self.afHostPortPair)


class YellowTwinDatabaseAssembly(K8sAssemblyFactory[HostPortSQLDatabase]):
    """
    This creates a Datasurface assembly with a postgres based Airflow and a supported merge database. Airflow only
    supports postgres databases.
    """
    def __init__(self, name: str, namespace: str, git_cache_config: GitCacheConfig,
                 afDBcred: Credential,
                 afDB: PostgresDatabase,
                 afDBStorageNeeds: StorageRequirement,
                 afHostPortPair: HostPortPair) -> None:
        super().__init__(name, namespace, git_cache_config)
        self.afDBcred: Credential = afDBcred
        self.afDB: PostgresDatabase = afDB
        self.afDBStorageNeeds: StorageRequirement = afDBStorageNeeds
        self.afHostPortPair: HostPortPair = afHostPortPair

    def createYellowAssemblyTwinDatabase(
            self,
            name: str,
            mergeRWCred: Credential,
            afDBcred: Credential,
            afDB: PostgresDatabase,
            afDBStorageNeeds: StorageRequirement,
            afHostPortPair: HostPortPair) -> Assembly:
        """This creates an assembly for a YellowDataPlatform where the merge engine and the airflow use seperate databases for performance
        reasons."""
        assembly: Assembly = Assembly(
            name, self.namespace,
            components=[
                NamespaceComponent("ns", self.namespace),
                LoggingComponent(self.name, self.namespace),
                NetworkPolicyComponent(self.name, self.namespace),
                PVCComponent(
                    self.git_cache_config.pvc_name, self.namespace,
                    self.git_cache_config.storage_size, self.git_cache_config.storageClass,
                    self.git_cache_config.access_mode),
                PostgresComponent("pg-airflow", self.namespace, afDBcred, afDB, afDBStorageNeeds),
                Airflow281Component("airflow", self.namespace, afDBcred, afDB, [mergeRWCred])  # Airflow needs the merge store database credentials for DAGs
            ]
        )
        return assembly

    def createAssembly(self, mergeRW_Credential: Credential, mergeDB: HostPortSQLDatabase) -> Assembly:
        """This creates the assembly for the kubernetes yaml file."""
        return self.createYellowAssemblyTwinDatabase(
            self.name,
            mergeRW_Credential,
            self.afDBcred,
            self.afDB,
            self.afDBStorageNeeds,
            self.afHostPortPair)
