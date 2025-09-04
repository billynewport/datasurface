# PostgreSQL Security Module for DataSurface Transformers

## Overview

This document outlines the security architecture for DataSurface transformer access to PostgreSQL databases, implementing principle of least privilege through workspace-scoped access control and view-based data governance.

## Security Architecture

### Layered Security Model

The security model implements three distinct layers:

1. **Raw Tables Layer** - Restricted access, platform-managed
2. **Workspace Views Layer** - Controlled data interface with business logic
3. **Transformer Access Layer** - Limited view-only permissions per workspace

### Access Control Flow

```
Raw Tables (Platform Only)
    ↓ (DEFINER privileges)
Workspace Views (Business Logic Applied)
    ↓ (SELECT permissions)
Transformer Users (Workspace-Scoped Access)
```

## Database User Architecture

### User Types

#### Platform Admin User
- **Purpose**: Creates and manages views, handles schema operations
- **Access**: Full read/write access to raw tables and all schemas
- **Credential**: Stored in `datasurface/merge/credentials` in AWS Secrets Manager

#### Transformer Users
- **Naming Convention**: `dt_{transformer_name}_user`
- **Purpose**: Execute data transformation operations
- **Access**: 
  - SELECT on assigned workspace views only
  - ALL privileges on transformer-specific output schema
- **Credential**: Stored in `datasurface/transformers/{transformer_name}/credentials`

### Permission Model

#### Per-Transformer Permissions
```sql
-- Schema access
GRANT USAGE ON SCHEMA workspace_{transformer_name} TO dt_{transformer_name}_user;

-- Read access to workspace views
GRANT SELECT ON ALL TABLES IN SCHEMA workspace_{transformer_name} TO dt_{transformer_name}_user;

-- Write access to output schema
GRANT ALL ON SCHEMA dt_{transformer_name}_output TO dt_{transformer_name}_user;

-- Default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA workspace_{transformer_name} 
GRANT SELECT TO dt_{transformer_name}_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA dt_{transformer_name}_output 
GRANT ALL TO dt_{transformer_name}_user;
```

## View-Based Data Governance

### View DEFINER Rights in PostgreSQL

PostgreSQL views execute with the privileges of the user who created them (DEFINER), not the user who queries them. This enables secure data access patterns.

#### View Creation (Platform Admin)
```sql
-- Platform admin creates view with access to raw tables
SET ROLE datasurface_platform_admin;

CREATE VIEW workspace_consumer1.customers AS 
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    created_at
FROM raw_schema.customers_internal 
WHERE 
    status = 'active' 
    AND deleted_at IS NULL
    AND privacy_consent = true;
```

#### View Access (Transformer User)
```sql
-- Transformer user queries view (works with DEFINER privileges)
SET ROLE dt_maskedstore_user;

SELECT * FROM workspace_consumer1.customers;  -- ✅ Success
SELECT * FROM raw_schema.customers_internal;  -- ❌ Access denied
```

### Security Benefits

1. **Data Governance**: Views enforce business rules and data quality constraints
2. **Access Control**: Transformers cannot bypass view logic to access raw data
3. **Audit Trail**: All data access goes through controlled view interfaces
4. **Data Masking**: Views can apply field-level masking and filtering
5. **Schema Evolution**: Raw table changes don't break transformer code

## Credential Management

### Password Generation Strategy

#### Initial Password Creation
```python
import secrets
import string

def generate_transformer_password() -> str:
    """Generate cryptographically secure password for new transformer user"""
    alphabet = string.ascii_letters + string.digits + '!@#$%^&*'
    return ''.join(secrets.choice(alphabet) for _ in range(32))
```

#### AWS Secrets Manager Integration
```python
def create_transformer_credentials(transformer_name: str) -> tuple[str, str]:
    """Create new transformer user credentials in AWS Secrets Manager"""
    
    username = f"dt_{transformer_name}_user"
    password = generate_transformer_password()
    
    secrets_client = boto3.client('secretsmanager')
    secret_name = f"datasurface/transformers/{transformer_name}/credentials"
    
    # Create secret with structured format
    secret_value = {
        "username": username,
        "password": password,
        "engine": "postgres",
        "host": "aws-postgres-1.ceziu2wcs0eo.us-east-1.rds.amazonaws.com",
        "port": 5432,
        "dbname": "datasurface_merge",
        "created_at": datetime.utcnow().isoformat(),
        "transformer_name": transformer_name
    }
    
    secrets_client.create_secret(
        Name=secret_name,
        Description=f"Database credentials for {transformer_name} transformer",
        SecretString=json.dumps(secret_value)
    )
    
    return username, password
```

### Password Rotation

#### Automatic Rotation via AWS Lambda
```python
def lambda_rotation_handler(event, context):
    """AWS Secrets Manager rotation handler for transformer credentials"""
    
    secret_arn = event['SecretArn']
    token = event['ClientRequestToken']
    step = event['Step']
    
    secrets_client = boto3.client('secretsmanager')
    
    if step == "createSecret":
        # Generate new password
        new_password = generate_transformer_password()
        
        # Get current secret to preserve metadata
        current_secret = secrets_client.get_secret_value(SecretId=secret_arn)
        current_data = json.loads(current_secret['SecretString'])
        
        # Update password in new version
        current_data['password'] = new_password
        current_data['rotated_at'] = datetime.utcnow().isoformat()
        
        # Store as AWSPENDING version
        secrets_client.put_secret_value(
            SecretId=secret_arn,
            VersionStage="AWSPENDING",
            SecretString=json.dumps(current_data),
            ClientRequestToken=token
        )
        
    elif step == "setSecret":
        # Update PostgreSQL user password
        pending_secret = secrets_client.get_secret_value(
            SecretId=secret_arn,
            VersionStage="AWSPENDING"
        )
        creds = json.loads(pending_secret['SecretString'])
        
        # Connect as admin user to change password
        admin_secret = secrets_client.get_secret_value(
            SecretId="datasurface/merge/credentials"
        )
        admin_creds = json.loads(admin_secret['SecretString'])
        
        # Update password in PostgreSQL
        conn = psycopg2.connect(
            host=creds['host'],
            user=admin_creds['username'],
            password=admin_creds['password'],
            database=creds['dbname']
        )
        
        with conn.cursor() as cur:
            cur.execute(
                "ALTER USER %s PASSWORD %s",
                (AsIs(creds['username']), creds['password'])
            )
        conn.commit()
        conn.close()
        
    elif step == "testSecret":
        # Test new credentials work
        pending_secret = secrets_client.get_secret_value(
            SecretId=secret_arn,
            VersionStage="AWSPENDING"
        )
        creds = json.loads(pending_secret['SecretString'])
        
        # Test connection
        test_conn = psycopg2.connect(
            host=creds['host'],
            user=creds['username'],
            password=creds['password'],
            database=creds['dbname']
        )
        test_conn.close()
        
    elif step == "finishSecret":
        # Promote AWSPENDING to AWSCURRENT
        secrets_client.update_secret_version_stage(
            SecretId=secret_arn,
            VersionStage="AWSCURRENT",
            MoveToVersionId=token,
            RemoveFromVersionId=event['SecretArn'].split(':')[-1]
        )
```

## Security Synchronization

### Security Sync Job Process

```python
def sync_transformer_security():
    """Main security synchronization job"""
    
    try:
        # 1. Discover required transformers from ecosystem model
        required_transformers = discover_transformers_from_model()
        
        # 2. Get existing database users
        existing_users = get_existing_transformer_users()
        
        # 3. Get existing AWS secrets
        existing_secrets = get_existing_transformer_secrets()
        
        # 4. Calculate changes needed
        users_to_create = set(required_transformers) - set(existing_users.keys())
        users_to_remove = set(existing_users.keys()) - set(required_transformers)
        
        # 5. Create missing users and secrets
        for transformer_name in users_to_create:
            create_transformer_user_and_secret(transformer_name)
            
        # 6. Remove obsolete users and secrets
        for transformer_name in users_to_remove:
            remove_transformer_user_and_secret(transformer_name)
            
        # 7. Sync permissions for all active transformers
        for transformer_name in required_transformers:
            sync_transformer_permissions(transformer_name)
            
        return f"Security sync complete: +{len(users_to_create)} -{len(users_to_remove)} users"
        
    except Exception as e:
        raise Exception(f"Security sync failed: {e}")

def create_transformer_user_and_secret(transformer_name: str):
    """Create new transformer user and associated AWS secret"""
    
    # Generate credentials
    username, password = create_transformer_credentials(transformer_name)
    
    # Create PostgreSQL user
    create_postgres_user(username, password)
    
    # Grant workspace permissions
    grant_workspace_permissions(username, transformer_name)
    
    # Log creation
    log_security_event(f"Created transformer user: {username}")

def create_postgres_user(username: str, password: str):
    """Create PostgreSQL user with generated password"""
    
    # Connect as admin
    admin_conn = get_admin_connection()
    
    with admin_conn.cursor() as cur:
        # Create user
        cur.execute(f"CREATE USER {username} PASSWORD %s", (password,))
        
        # Set connection limits
        cur.execute(f"ALTER USER {username} CONNECTION LIMIT 10")
        
    admin_conn.commit()
    admin_conn.close()

def grant_workspace_permissions(username: str, transformer_name: str):
    """Grant workspace-specific permissions to transformer user"""
    
    admin_conn = get_admin_connection()
    workspace_schema = f"workspace_{transformer_name}"
    output_schema = f"dt_{transformer_name}_output"
    
    with admin_conn.cursor() as cur:
        # Workspace read access
        cur.execute(f"GRANT USAGE ON SCHEMA {workspace_schema} TO {username}")
        cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {workspace_schema} TO {username}")
        
        # Output write access
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")
        cur.execute(f"GRANT ALL ON SCHEMA {output_schema} TO {username}")
        cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {output_schema} TO {username}")
        
        # Default privileges for future objects
        cur.execute(f"""
            ALTER DEFAULT PRIVILEGES IN SCHEMA {workspace_schema} 
            GRANT SELECT TO {username}
        """)
        cur.execute(f"""
            ALTER DEFAULT PRIVILEGES IN SCHEMA {output_schema} 
            GRANT ALL TO {username}
        """)
        
    admin_conn.commit()
    admin_conn.close()
```

## Integration with Airflow

### Credential Fetching in DAGs

```python
def fetch_transformer_secrets(transformer_name: str) -> dict:
    """Fetch transformer-specific credentials from AWS Secrets Manager"""
    
    secrets_client = boto3.client('secretsmanager')
    secret_name = f"datasurface/transformers/{transformer_name}/credentials"
    
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        raise Exception(f"Failed to fetch credentials for {transformer_name}: {e}")

# In infrastructure DAG template
transformer_creds = fetch_transformer_secrets(transformer_name)
env_vars.extend([
    k8s.V1EnvVar(name=f'DT_{transformer_name.upper()}_USER', value=transformer_creds['username']),
    k8s.V1EnvVar(name=f'DT_{transformer_name.upper()}_PASSWORD', value=transformer_creds['password']),
])
```

## Deployment Integration

### Security Sync as Infrastructure Task

The security synchronization should be integrated into the infrastructure DAG:

```python
# Add to infrastructure_dag.py.j2
security_sync_task = KubernetesPodOperator(
    task_id='sync_transformer_security',
    name='security-sync-job',
    namespace='{{ namespace_name }}',
    image='{{ datasurface_docker_image }}',
    cmds=['python', '-m', 'datasurface.security.sync'],
    arguments=['--operation', 'sync-transformer-users'],
    env_vars=admin_env_vars,
    is_delete_operator_pod=True,
    dag=dag
)

# Run security sync before other infrastructure tasks
start_task >> security_sync_task >> merge_task
```

## Scale Considerations

### Performance Optimization

With hundreds of transformers, the security sync must be efficient:

#### Batch Operations
```sql
-- Create multiple users in single transaction
BEGIN;
CREATE USER dt_transformer1_user PASSWORD 'pass1';
CREATE USER dt_transformer2_user PASSWORD 'pass2';
-- ... batch create
COMMIT;
```

#### Permission Caching
```python
def get_current_permissions_cached():
    """Cache current database permissions to avoid expensive queries"""
    
    # Query once, cache results
    permissions = query_all_transformer_permissions()
    cache_permissions(permissions, ttl=3600)  # 1 hour cache
    
    return permissions
```

#### Incremental Sync
```python
def incremental_security_sync():
    """Only sync changed transformers, not all transformers"""
    
    # Track last sync timestamp
    last_sync = get_last_sync_timestamp()
    
    # Only process transformers modified since last sync
    changed_transformers = get_transformers_modified_since(last_sync)
    
    for transformer_name in changed_transformers:
        sync_single_transformer_security(transformer_name)
```

## AWS RDS PostgreSQL Considerations

### RDS Compatibility

AWS RDS PostgreSQL is **real PostgreSQL**, not emulation:

- ✅ **Full SQL compatibility** with standard PostgreSQL
- ✅ **Same view DEFINER behavior** - works identically
- ✅ **Same role/permission system** - `CREATE USER`, `GRANT`, etc.
- ✅ **View-based security** works exactly as documented

### RDS Limitations

- ❌ **No superuser access** - some admin functions restricted
- ❌ **No file system access** - cannot access underlying OS
- ❌ **Extension limitations** - only pre-approved extensions available
- ✅ **User management** - full control over database users and permissions

### AWS Secrets Manager Integration

PostgreSQL does **not** natively integrate with AWS Secrets Manager. Rotation requires orchestration:

1. **AWS Secrets Manager** - Stores and triggers rotation
2. **Custom Lambda/Airflow** - Updates PostgreSQL user passwords
3. **PostgreSQL** - Password updated via SQL commands

## Implementation Checklist

### Initial Setup

- [ ] Create platform admin user with full database access
- [ ] Design workspace schema naming convention
- [ ] Create view templates for common data patterns
- [ ] Set up AWS Secrets Manager secret templates

### Per-Transformer Setup

- [ ] Generate unique, secure password
- [ ] Create PostgreSQL user with generated password
- [ ] Create AWS Secrets Manager secret
- [ ] Grant workspace-specific permissions
- [ ] Create transformer output schema
- [ ] Test credential access and permissions

### Ongoing Operations

- [ ] Implement security sync job in infrastructure DAG
- [ ] Set up password rotation (manual or automatic)
- [ ] Monitor permission drift and unauthorized access
- [ ] Audit transformer credential usage
- [ ] Clean up obsolete users and secrets

## Security Best Practices

### Password Management

1. **Length**: Minimum 32 characters
2. **Complexity**: Mix of letters, numbers, symbols
3. **Uniqueness**: Each transformer gets unique password
4. **Rotation**: Regular rotation (30-90 days)
5. **Storage**: Only in AWS Secrets Manager, never in code

### Permission Management

1. **Least Privilege**: Only grant minimum required permissions
2. **Workspace Isolation**: Strict schema-level separation
3. **Regular Audits**: Verify permissions match requirements
4. **Change Tracking**: Log all permission modifications
5. **Cleanup**: Remove obsolete users and permissions

### Monitoring and Auditing

1. **Connection Monitoring**: Track transformer database connections
2. **Permission Changes**: Log all GRANT/REVOKE operations
3. **Failed Access**: Monitor and alert on access denied events
4. **Credential Rotation**: Track rotation success/failure
5. **Compliance Reporting**: Regular security posture reports

## Troubleshooting

### Common Issues

#### Transformer Cannot Access Views
- **Check**: User exists in PostgreSQL
- **Check**: USAGE permission on workspace schema
- **Check**: SELECT permission on specific views
- **Check**: Credentials in AWS Secrets Manager are current

#### View Access Denied
- **Check**: View DEFINER has access to underlying tables
- **Check**: View definition is syntactically correct
- **Check**: Underlying tables exist and are accessible

#### Password Rotation Failures
- **Check**: Lambda function has PostgreSQL connectivity
- **Check**: Admin credentials are valid and current
- **Check**: New password meets PostgreSQL requirements
- **Check**: AWS Secrets Manager permissions are correct

### Diagnostic Queries

```sql
-- Check user permissions
SELECT 
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM information_schema.table_privileges 
WHERE grantee = 'dt_maskedstore_user';

-- Check schema permissions
SELECT 
    schema_name,
    schema_owner,
    default_character_set_name
FROM information_schema.schemata 
WHERE schema_name LIKE 'workspace_%' OR schema_name LIKE 'dt_%';

-- Check view definitions
SELECT 
    table_schema,
    table_name,
    view_definition
FROM information_schema.views 
WHERE table_schema = 'workspace_consumer1';
```

## Future Enhancements

### Planned Improvements

1. **Dynamic Permission Calculation**: Real-time permission updates based on model changes
2. **Fine-Grained Column Permissions**: Column-level access control within views
3. **Temporal Access**: Time-based permission grants for temporary access
4. **Cross-Workspace Views**: Controlled data sharing between workspaces
5. **Compliance Integration**: Automated compliance reporting and validation

### Integration Opportunities

1. **DataSurface Governance**: Integration with DataSurface governance policies
2. **External Identity Providers**: LDAP/Active Directory integration
3. **Audit Systems**: Integration with enterprise audit and SIEM systems
4. **Policy as Code**: Version-controlled permission definitions
5. **Self-Service**: Developer portal for transformer credential requests

