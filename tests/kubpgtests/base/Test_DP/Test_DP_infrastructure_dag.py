"""
Infrastructure DAG for Test_DP Data Platform
Generated automatically by DataSurface KubernetesPGStarter Platform

This DAG contains the core infrastructure tasks:
- MERGE task: Generates infrastructure terraform files  
- Metrics collector task: Collects and processes metrics
- Apply security task: Applies security configurations
- Table removal task: Cleans up unused tables
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'datasurface',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'Test_DP_infrastructure',
    default_args=default_args,
    description='Infrastructure DAG for Test_DP Data Platform',
    schedule_interval='@daily',  # Run daily, can be adjusted based on needs
    catchup=False,
    tags=['datasurface', 'infrastructure', 'Test_DP']
)

# Start task
start_task = DummyOperator(
    task_id='start_infrastructure_tasks',
    dag=dag
)

# Environment variables for all tasks
common_env_vars = {
    # Postgres credentials
    'postgres_USER': {
        'secret_name': 'postgres',
        'secret_key': 'username'
    },
    'postgres_PASSWORD': {
        'secret_name': 'postgres',
        'secret_key': 'password'
    },
    # Kafka Connect credentials
    'connect_TOKEN': {
        'secret_name': 'connect',
        'secret_key': 'token'
    },
    # Git credentials
    'git_TOKEN': {
        'secret_name': 'git',
        'secret_key': 'token'
    },
    # Slack credentials
    'slack_TOKEN': {
        'secret_name': 'slack',
        'secret_key': 'token'
    },
    # Platform configuration
    'DATASURFACE_PLATFORM_NAME': 'Test_DP',
    'DATASURFACE_NAMESPACE': 'ns_kub_pg_test',
    'DATASURFACE_SLACK_CHANNEL': 'datasurface-events'
}

# MERGE Task - Generates infrastructure terraform files
merge_task = KubernetesPodOperator(
    task_id='infrastructure_merge_task',
    name='Test_DP-infra-merge',
    namespace='ns_kub_pg_test',
    image='datasurface/datasurface:latest',
    cmds=['python', '-m', 'datasurface.platforms.kubpgstarter.tasks.merge'],
    arguments=[
        '--platform-name', 'Test_DP',
        '--operation', 'infrastructure-merge',
        '--git-repo-path', '/workspace/model'
    ],
    env_vars=common_env_vars,
    volumes=[
        {
            'name': 'git-workspace',
            'configMap': {'name': 'Test_DP-git-config'}
        }
    ],
    volume_mounts=[
        {
            'name': 'git-workspace',
            'mountPath': '/workspace/model',
            'readOnly': True
        }
    ],
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)

# Metrics Collector Task - Collects and processes platform metrics
metrics_collector_task = KubernetesPodOperator(
    task_id='metrics_collector_task',
    name='Test_DP-metrics-collector',
    namespace='ns_kub_pg_test',
    image='datasurface/datasurface:latest',
    cmds=['python', '-m', 'datasurface.platforms.kubpgstarter.tasks.metrics'],
    arguments=[
        '--platform-name', 'Test_DP',
        '--operation', 'collect-metrics',
        '--postgres-host', 'pg-data.ns_kub_pg_test.svc.cluster.local',
        '--postgres-port', '5432',
        '--postgres-database', 'datasurface_merge'
    ],
    env_vars=common_env_vars,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)

# Apply Security Task - Applies security configurations
apply_security_task = KubernetesPodOperator(
    task_id='apply_security_task',
    name='Test_DP-apply-security',
    namespace='ns_kub_pg_test',
    image='datasurface/datasurface:latest',
    cmds=['python', '-m', 'datasurface.platforms.kubpgstarter.tasks.security'],
    arguments=[
        '--platform-name', 'Test_DP',
        '--operation', 'apply-security',
        '--postgres-host', 'pg-data.ns_kub_pg_test.svc.cluster.local',
        '--postgres-port', '5432',
        '--postgres-database', 'datasurface_merge'
    ],
    env_vars=common_env_vars,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)

# Table Removal Task - Cleans up unused tables
table_removal_task = KubernetesPodOperator(
    task_id='table_removal_task',
    name='Test_DP-table-removal',
    namespace='ns_kub_pg_test',
    image='datasurface/datasurface:latest',
    cmds=['python', '-m', 'datasurface.platforms.kubpgstarter.tasks.cleanup'],
    arguments=[
        '--platform-name', 'Test_DP',
        '--operation', 'remove-unused-tables',
        '--postgres-host', 'pg-data.ns_kub_pg_test.svc.cluster.local',
        '--postgres-port', '5432',
        '--postgres-database', 'datasurface_merge',
        '--git-repo-path', '/workspace/model'
    ],
    env_vars=common_env_vars,
    volumes=[
        {
            'name': 'git-workspace',
            'configMap': {'name': 'Test_DP-git-config'}
        }
    ],
    volume_mounts=[
        {
            'name': 'git-workspace',
            'mountPath': '/workspace/model',
            'readOnly': True
        }
    ],
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)

# End task
end_task = DummyOperator(
    task_id='end_infrastructure_tasks',
    dag=dag
)

# Define task dependencies
# MERGE task runs first to generate infrastructure
start_task >> merge_task

# Security and metrics can run in parallel after merge
merge_task >> [apply_security_task, metrics_collector_task]

# Table removal runs after security is applied
apply_security_task >> table_removal_task

# All tasks complete before end
[metrics_collector_task, table_removal_task] >> end_task 