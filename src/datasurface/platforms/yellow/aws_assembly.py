"""
AWS-specific assembly classes for DataSurface Yellow Platform on EKS
Based on the existing assembly patterns but adapted for AWS services
"""

from typing import Any, Optional
from datasurface.md import StorageRequirement, Ecosystem, ValidationTree, ProblemSeverity
from datasurface.md.credential import Credential
from datasurface.md import HostPortSQLDatabase, PostgresDatabase
from datasurface.platforms.yellow.assembly import (
    K8sAssemblyFactory, Assembly, K8sResourceLimits, GitCacheConfig,
    NamespaceComponent, PVCComponent, LoggingComponent, NetworkPolicyComponent,
    Component, K8sUtils
)
from jinja2 import Template
from datasurface.platforms.yellow.db_utils import getDriverNameAndQueryForDataContainer
import urllib.parse


class AirflowAWSComponent(Component):
    """
    AWS-specific Airflow component for EKS deployment.
    This generates Kubernetes manifests optimized for EKS with AWS integrations like IRSA.
    """
    def __init__(self, name: str, namespace: str, dbCred: Credential, db: HostPortSQLDatabase,
                 airflow_iam_role_arn: str,
                 dagCreds: list[Credential], webserverResourceLimits: Optional[K8sResourceLimits] = None,
                 schedulerResourceLimits: Optional[K8sResourceLimits] = None,
                 airflow_image: str = "datasurface/airflow:2.11.0-aws",
                 aws_account_id: str = "") -> None:
        super().__init__(name, namespace)
        self.dbCred: Credential = dbCred
        self.db: HostPortSQLDatabase = db
        self.dagCreds: list[Credential] = dagCreds
        self.airflow_iam_role_arn: str = airflow_iam_role_arn
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
        self.aws_account_id: str = aws_account_id

    def render(self, env, templateContext: dict[str, Any]) -> str:
        """Render using the AirflowAWS template with AWS-specific context"""
        from jinja2 import Template

        airflow_template: Template = env.get_template('airflowAWS/psp_airflow.yaml.j2')
        ctxt: dict[str, Any] = templateContext.copy()
        driverName, query = getDriverNameAndQueryForDataContainer(self.db)
        # URL-encode the driver query parameter to handle spaces and special characters
        encoded_query = urllib.parse.quote_plus(query) if query else None

        # Add AWS-specific context
        ctxt.update({
            "airflow_k8s_name": K8sUtils.to_k8s_name(self.name),
            "airflow_db_hostname": self.db.hostPortPair.hostName,
            "airflow_db_port": self.db.hostPortPair.port,
            "airflow_db_driver": driverName,
            "airflow_db_query": encoded_query,
            "airflow_db_credential_secret_name": K8sUtils.to_k8s_name(self.dbCred.name),
            "extra_credentials": [K8sUtils.to_k8s_name(cred.name) for cred in self.dagCreds],
            "airflow_image": self.airflow_image,
            "aws_account_id": self.aws_account_id,
            "airflow_iam_role_arn": self.airflow_iam_role_arn,
            # Resource limits
            "webserver_requested_memory": self.webserverResourceLimits.to_k8s_json()["requested_memory"],
            "webserver_limits_memory": self.webserverResourceLimits.to_k8s_json()["limits_memory"],
            "webserver_requested_cpu": self.webserverResourceLimits.to_k8s_json()["requested_cpu"],
            "webserver_limits_cpu": self.webserverResourceLimits.to_k8s_json()["limits_cpu"],
            "scheduler_requested_memory": self.schedulerResourceLimits.to_k8s_json()["requested_memory"],
            "scheduler_limits_memory": self.schedulerResourceLimits.to_k8s_json()["limits_memory"],
            "scheduler_requested_cpu": self.schedulerResourceLimits.to_k8s_json()["requested_cpu"],
            "scheduler_limits_cpu": self.schedulerResourceLimits.to_k8s_json()["limits_cpu"],
        })

        return airflow_template.render(ctxt)

    def generateBootstrapArtifacts(self, eco: Ecosystem, context: dict[str, Any], jinjaEnv, ringLevel: int) -> dict[str, str]:
        """Generate AWS-specific infrastructure DAG"""
        dag_template = jinjaEnv.get_template('airflowAWS/infrastructure_dag.py.j2')

        comp_context: dict[str, Any] = context.copy()
        comp_context.update({
            "airflow_k8s_name": K8sUtils.to_k8s_name(self.name),
            "airflow_iam_role_arn": self.airflow_iam_role_arn,
        })
        rc: dict[str, str] = dict()
        rendered_infrastructure_dag: str = dag_template.render(comp_context)
        rc["infrastructure_dag.py"] = rendered_infrastructure_dag

        model_merge_template: Template = jinjaEnv.get_template('airflowAWS/model_merge_job.yaml.j2')
        rendered_model_merge_job: str = model_merge_template.render(comp_context)

        # This is a yaml file which runs in the environment to create database tables needed
        # and other things required to run the dataplatforms.
        ring1_init_template: Template = jinjaEnv.get_template('airflowAWS/ring1_init_job.yaml.j2')
        rendered_ring1_init_job: str = ring1_init_template.render(comp_context)

        # This creates all the Workspace views needed for all the dataplatforms in the model.
        reconcile_views_template: Template = jinjaEnv.get_template('airflowAWS/reconcile_views_job.yaml.j2')
        rendered_reconcile_views_job: str = reconcile_views_template.render(comp_context)

        rc["model_merge_job.yaml"] = rendered_model_merge_job
        rc["ring1_init_job.yaml"] = rendered_ring1_init_job
        rc["reconcile_views_job.yaml"] = rendered_reconcile_views_job
        return rc


class YellowAWSTwinDatabaseAssembly(K8sAssemblyFactory[HostPortSQLDatabase]):
    """
    AWS EKS assembly with external RDS databases for both Airflow and merge store.

    Key differences from YellowTwinDatabaseAssembly:
    - Uses AirflowAWSComponent instead of Airflow2XComponent
    - Removes NetworkPolicyComponent (AWS VPC handles network security)
    - Adds AWS-specific configuration (account ID, IAM roles)
    - Uses AWS-optimized Airflow image with AWS providers
    """

    def __init__(self, name: str, namespace: str, git_cache_config: GitCacheConfig,
                 afDBcred: Credential,
                 afDB: PostgresDatabase,
                 afWebserverResourceLimits: K8sResourceLimits,
                 afSchedulerResourceLimits: K8sResourceLimits,
                 aws_account_id: str,
                 airflow_iam_role_arn: str,
                 airflow_image: str = "datasurface/airflow:2.11.0-aws") -> None:
        super().__init__(name, namespace, git_cache_config)
        self.afDBcred: Credential = afDBcred
        self.afDB: PostgresDatabase = afDB
        self.afWebserverResourceLimits: K8sResourceLimits = afWebserverResourceLimits
        self.afSchedulerResourceLimits: K8sResourceLimits = afSchedulerResourceLimits
        self.aws_account_id: str = aws_account_id
        self.airflow_iam_role_arn: str = airflow_iam_role_arn
        self.airflow_image: str = airflow_image

    def createYellowAWSAssemblyTwinDatabase(
            self,
            name: str,
            mergeRWCred: Credential) -> Assembly:
        """
        Create AWS EKS assembly with external RDS databases.

        Components included:
        - NamespaceComponent: Creates Kubernetes namespace
        - LoggingComponent: Kubernetes logging configuration (works with CloudWatch)
        - NetworkPolicyComponent: Kubernetes network policies (complements AWS VPC security groups)
        - PVCComponent: Git cache storage (uses EBS via gp3 storage class)
        - AirflowAWSComponent: AWS-optimized Airflow with IRSA, RBAC, and Secrets Manager integration

        Note: AirflowAWSComponent includes its own RBAC (ServiceAccount, Role, RoleBinding)
        """
        assembly: Assembly = Assembly(
            name, self.namespace,
            components=[
                NamespaceComponent("ns", self.namespace),
                LoggingComponent(self.name, self.namespace),
                NetworkPolicyComponent(self.name, self.namespace),
                PVCComponent(
                    self.git_cache_config.pvc_name, self.namespace,
                    self.git_cache_config.storage_size,
                    self.git_cache_config.storageClass,  # AWS EBS gp3 storage class for better performance
                    self.git_cache_config.access_mode),
                AirflowAWSComponent(
                    "airflow", self.namespace, self.afDBcred, self.afDB, self.airflow_iam_role_arn, [mergeRWCred],
                    webserverResourceLimits=self.afWebserverResourceLimits,
                    schedulerResourceLimits=self.afSchedulerResourceLimits,
                    airflow_image=self.airflow_image,
                    aws_account_id=self.aws_account_id
                )  # Airflow needs the merge store database credentials for DAGs
            ]
        )
        return assembly

    def createYAMLContext(self, eco: Ecosystem) -> dict[str, Any]:
        """Add AWS-specific context variables"""
        rc: dict[str, Any] = super().createYAMLContext(eco)
        rc.update({
            "aws_account_id": self.aws_account_id,
            "storage_class": "gp3",  # AWS EBS gp3 for better performance/cost
            "use_aws_logging": True,
            "use_aws_networking": True,
        })
        return rc

    def createAssembly(self, mergeRW_Credential: Credential, mergeDB: HostPortSQLDatabase) -> Assembly:
        """This creates the assembly for the AWS EKS deployment."""
        return self.createYellowAWSAssemblyTwinDatabase(
            self.name,
            mergeRW_Credential
        )

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        """Validate AWS-specific configuration"""
        super().lint(eco, tree)
        self.afDB.lint(eco, tree.addSubTree(self.afDB))
        self.afWebserverResourceLimits.lint(tree.addSubTree(self.afWebserverResourceLimits))
        self.afSchedulerResourceLimits.lint(tree.addSubTree(self.afSchedulerResourceLimits))

        # AWS-specific validations
        if not self.aws_account_id:
            tree.addProblem("AWS Account ID is required for EKS deployment", ProblemSeverity.ERROR)
        elif not self.aws_account_id.isdigit() or len(self.aws_account_id) != 12:
            tree.addProblem(f"AWS Account ID must be a 12-digit number, got: {self.aws_account_id}", ProblemSeverity.ERROR)


class YellowAWSExternalDatabaseAssembly(K8sAssemblyFactory[HostPortSQLDatabase]):
    """
    AWS EKS assembly with a single external RDS database shared by Airflow and merge store.

    This is the most cost-effective option for AWS deployment where both Airflow and
    the merge store use the same RDS PostgreSQL instance.
    """

    def __init__(self, name: str, namespace: str, git_cache_config: GitCacheConfig,
                 airflow_iam_role_arn: str,
                 afWebserverResourceLimits: Optional[K8sResourceLimits] = None,
                 afSchedulerResourceLimits: Optional[K8sResourceLimits] = None,
                 aws_account_id: str = "",
                 airflow_image: str = "datasurface/airflow:2.11.0-aws") -> None:
        super().__init__(name, namespace, git_cache_config)
        self.afWebserverResourceLimits: Optional[K8sResourceLimits] = afWebserverResourceLimits
        self.afSchedulerResourceLimits: Optional[K8sResourceLimits] = afSchedulerResourceLimits
        self.aws_account_id: str = aws_account_id
        self.airflow_iam_role_arn: str = airflow_iam_role_arn
        self.airflow_image: str = airflow_image

    def createYellowAWSAssemblySingleDatabase(
            self,
            name: str,
            dbRWCred: Credential,
            db: HostPortSQLDatabase,
            afWebserverResourceLimits: Optional[K8sResourceLimits] = None,
            afSchedulerResourceLimits: Optional[K8sResourceLimits] = None) -> Assembly:
        """Create AWS EKS assembly with single external RDS database."""

        assembly: Assembly = Assembly(
            name, self.namespace,
            components=[
                NamespaceComponent("ns", self.namespace),
                LoggingComponent(self.name, self.namespace),
                NetworkPolicyComponent(self.name, self.namespace),
                PVCComponent(
                    self.git_cache_config.pvc_name, self.namespace,
                    self.git_cache_config.storage_size,
                    "gp3",  # AWS EBS gp3 storage class
                    self.git_cache_config.access_mode),
                AirflowAWSComponent(
                    "airflow", self.namespace, dbRWCred, db, self.airflow_iam_role_arn, [],
                    webserverResourceLimits=afWebserverResourceLimits,
                    schedulerResourceLimits=afSchedulerResourceLimits,
                    airflow_image=self.airflow_image,
                    aws_account_id=self.aws_account_id)
            ]
        )
        return assembly

    def createYAMLContext(self, eco: Ecosystem) -> dict[str, Any]:
        """Add AWS-specific context variables"""
        rc: dict[str, Any] = super().createYAMLContext(eco)
        rc.update({
            "aws_account_id": self.aws_account_id,
            "storage_class": "gp3",
            "use_aws_logging": True,
            "use_aws_networking": True,
        })
        return rc

    def createAssembly(self, mergeRW_Credential: Credential, mergeDB: HostPortSQLDatabase) -> Assembly:
        """This creates the assembly for the AWS EKS deployment with shared database."""
        return self.createYellowAWSAssemblySingleDatabase(
            self.name,
            mergeRW_Credential, mergeDB,
            self.afWebserverResourceLimits,
            self.afSchedulerResourceLimits)

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        """Validate AWS-specific configuration"""
        super().lint(eco, tree)
        if self.afWebserverResourceLimits:
            self.afWebserverResourceLimits.lint(tree.addSubTree(self.afWebserverResourceLimits))
        if self.afSchedulerResourceLimits:
            self.afSchedulerResourceLimits.lint(tree.addSubTree(self.afSchedulerResourceLimits))

        # AWS-specific validations
        if not self.aws_account_id:
            tree.addProblem("AWS Account ID is required for EKS deployment", ProblemSeverity.ERROR)
        elif not self.aws_account_id.isdigit() or len(self.aws_account_id) != 12:
            tree.addProblem(f"AWS Account ID must be a 12-digit number, got: {self.aws_account_id}", ProblemSeverity.ERROR)
