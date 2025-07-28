# MVP for DataTransformers in YellowDataPlatform

DataTransformers are now fully implemented in YellowDataPlatform. A DataTransformer is code which executes against a Workspace and produces output which is then ingested into a new Datastore. For example, we can make a DataTransformer which takes a customer table with PII and removes the PII producing a new Datastore containing the customer data without PII through masking.

Common DataTransformer use cases:

* Changing the schema of input data
* Joining data with other data, materializing a view
* Masking data
* Aggregating data
* Data quality validation and enrichment

## Implementation Overview

DataTransformers are triggered when any of their input datasets change. The code runs in a job pod and produces output to staging tables (`dt_` prefix) which are then ingested by a separate output ingestion DAG into the final output datastore.

The DataTransformer code is packaged by its owner in its own GitHub repository with a normal test cycle. The Ecosystem model references a specific version of the code. The output Datastore and associated datasets/schemas are versioned together with the DataTransformer code in the model.

## Architecture Components

### 1. DataTransformer Factory DAG

A dedicated factory DAG (`{platform}_datatransformer_factory_dag.py`) manages all DataTransformer-related DAGs:

- **Reads from**: `{platform}_airflow_datatransformer` database table
- **Creates**: DataTransformer execution DAGs and output ingestion DAGs
- **Schedule**: Every 5 minutes to sync configurations
- **Manages**: DAGs ending with `_datatransformer` and `_dt_ingestion`

### 2. Database Tables

**DataTransformer Configuration Table**: `{platform}_airflow_datatransformer`
```sql
CREATE TABLE platform_airflow_datatransformer (
    workspace_name VARCHAR(255) PRIMARY KEY,
    config_json VARCHAR(4096) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

The `config_json` contains complete DAG configuration including:
- Input DAG IDs for external task sensors
- Output datastore information
- Platform configuration (credentials, git repo, etc.)

### 3. DAG Naming Conventions

To prevent conflicts between factory DAGs, distinct naming patterns are used:

- **Regular ingestion**: `{platform}__{stream_key}_ingestion`
- **DataTransformer execution**: `{platform}__{workspace}_datatransformer`
- **DataTransformer output**: `{platform}__{output_store}_dt_ingestion`

### 4. Dynamic DAG Creation

For each DataTransformer workspace, the factory creates two DAGs:

**A. DataTransformer Execution DAG**
- **External Task Sensors**: Wait for completion of all input ingestion DAGs
- **DataTransformer Job**: Runs `run-datatransformer` operation in Kubernetes pod
- **Trigger Output Ingestion**: Automatically triggers the output ingestion DAG
- **Log-based Result Checking**: Uses Airflow logs to determine next action

**B. DataTransformer Output Ingestion DAG**  
- **Output Ingestion Job**: Runs `ingest-datatransformer-output` operation
- **Triggered Execution**: Only runs when triggered by the execution DAG
- **Data Movement**: From `dt_` staging tables to final output datastore tables

### 5. Smart Sensor Dependencies

The model merge handler automatically generates correct DAG IDs based on store type:

```python
if isinstance(store.cmd, (KafkaIngestion, SQLSnapshotIngestion)):
    # Regular store → wait for regular ingestion DAG
    dag_id = f"{platform}__{stream_key}_ingestion"
    
elif isinstance(store.cmd, DataTransformerOutput):
    # DataTransformer output → wait for DataTransformer output DAG
    dag_id = f"{platform}__{store}_dt_ingestion"
```

This enables DataTransformers to chain together seamlessly - one DataTransformer can use the output of another as input.

## Model Merge Handler Integration

The model merge handler (`YellowGraphHandler.renderGraph()`) now:

1. **Creates DataTransformer table** during bootstrap (ring level 1)
2. **Populates configurations** for all workspaces with DataTransformers
3. **Generates bootstrap artifacts** including the DataTransformer factory DAG
4. **Manages dependencies** by analyzing workspace dataset usage

The handler scans all workspaces assigned to the platform and:
- Identifies workspaces with DataTransformers
- Maps input datasets to their ingestion DAG IDs (regular or DataTransformer output)
- Creates complete configuration context for the factory DAG
- Stores configurations in the database for dynamic DAG generation

## DataTransformer Job Implementation

The `DataTransformerJob` class handles:

1. **Git clone** of the DataTransformer code repository
2. **Table creation** for `dt_` output staging tables
3. **Transaction management** with truncate and rollback capability
4. **Code execution** calling the `executeTransformer` function
5. **Dataset mapping** providing table names to the transformer code

The DataTransformer code receives:
- Database connection for writing output
- Dictionary mapping `store#dataset` names to table names
- Input tables: standard merge table names
- Output tables: `dt_` prefixed staging table names

## Factory DAG Conflict Prevention

Each factory DAG has clear ownership boundaries:

**Regular Factory DAG**:
- Manages: `*_ingestion` (excludes `*_dt_ingestion` and `*_datatransformer`)
- Removes: Only DAGs matching regular ingestion patterns

**DataTransformer Factory DAG**:
- Manages: `*_datatransformer` AND `*_dt_ingestion`
- Removes: Only DAGs matching DataTransformer patterns

This prevents "zombie DAGs" and ensures safe parallel operation of both factory DAGs.

## DataTransformer Versioning and Rollback

The DataTransformer code is versioned in GitHub repositories. The Ecosystem model references specific versions. Output schema changes must be backwards compatible or require creating a new DataTransformer with consumer migration following the standard deprecation process.

Truly breaking changes require:
1. Creating a new DataTransformer with new output datastore
2. Consumer migration to the new output
3. Deprecation of the old DataTransformer
4. Retention of old data for audit/regulatory requirements

## Example Workflow

1. **Model Update**: Workspace with DataTransformer added to model
2. **Model Merge**: `handleModelMerge` triggered, populates DataTransformer table
3. **Factory Execution**: DataTransformer factory DAG reads table, creates execution and output DAGs
4. **Data Ingestion**: Input data changes trigger regular ingestion DAGs
5. **External Sensors**: DataTransformer execution DAG sensors detect ingestion completion  
6. **Transformer Execution**: DataTransformer job runs, writes to `dt_` staging tables
7. **Output Ingestion**: Output ingestion DAG triggered, moves data to final tables
8. **Consumer Access**: Downstream workspaces can now access the transformed data

This implementation provides a complete, production-ready DataTransformer system that integrates seamlessly with the existing YellowDataPlatform infrastructure.

## DataTransformer Trigger revisions

The existing DSL had a specific trigger for DataTransformer which looking at it now makes no sense. I have switched to a StepTrigger and I also had to modify StepTrigger to be a UserDSLObject for linting purposes. The StepTrigger lint method had too many parameters also so I removed them. This also means the data transformer factory dag needs to use the cron expression if the step trigger is specified. I need to change YellowDataPlatform to lint either None or CronTrigger for a DataTransformer.

If a DataTransformer trigger is present then there is no need for a sensor for its DAG and the schedule string should be set to the triggers string just like normal ingestion DAGs. So, if a trigger is present then use it for the schedule and no sensors. If no trigger is present then have the sensors and the datatransformer DAG waits for the sensor.

The merge handler may also need to store the schedule string for a DataTransformer when a steptrigger is present. If it's not present then omit it like now. The factory dag could use the presence of the schedule string to just make the DAG scheduled versus use the sensors on the inputs.

### Implementation Status: ✅ COMPLETED

The DataTransformer trigger logic has been fully implemented with the following changes:

#### 1. **Updated Trigger Linting**
- YellowDataPlatform now accepts `None` or `CronTrigger` for DataTransformers
- `None` trigger = sensor-based mode (wait for input data changes)
- `CronTrigger` = scheduled mode (run on cron schedule)

#### 2. **Enhanced Configuration Storage**
- `config_json` in `{platform}_airflow_datatransformer` table now includes `schedule_string` field
- `schedule_string` is populated with cron expression when trigger is present
- `schedule_string` is omitted when no trigger (sensor-based mode)

#### 3. **Updated Factory DAG Logic**
- Factory DAG checks for presence of `schedule_string` in configuration
- **Scheduled Mode**: Uses cron schedule, no external task sensors
- **Sensor Mode**: Uses external task sensors, no schedule (triggered by input data)

#### 4. **Conditional Task Creation**
- External task sensors are only created when `schedule_string` is absent
- Task dependencies automatically adjust based on sensor presence
- Debugging logs show which mode is being used for each DataTransformer

#### 5. **Database Schema**
The existing `{platform}_airflow_datatransformer` table structure remains unchanged:
```sql
CREATE TABLE platform_airflow_datatransformer (
    workspace_name VARCHAR(255) PRIMARY KEY,
    config_json VARCHAR(4096) NOT NULL,  -- Now includes optional schedule_string
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

#### 6. **Example Configurations**

**Scheduled DataTransformer** (with CronTrigger):
```json
{
    "workspace_name": "customer_masking",
    "output_datastore_name": "masked_customers",
    "schedule_string": "0 2 * * *",  // Run daily at 2 AM
    "input_dag_ids": [...],
    "input_dataset_list": [...],
    "output_dataset_list": [...]
}
```

**Sensor-based DataTransformer** (no trigger):
```json
{
    "workspace_name": "real_time_analytics", 
    "output_datastore_name": "analytics_output",
    // No schedule_string = sensor mode
    "input_dag_ids": [...],
    "input_dataset_list": [...],
    "output_dataset_list": [...]
}
```

This implementation provides flexible DataTransformer execution patterns while maintaining backward compatibility and clear operational behavior.

## Issues

While trying to bring this up, I noticed that DSG linting requires a platformMD to be specified. A terminal Workspace is a user facing Workspace where they specified requirements. A terminal Workspace cannot have a DataTransformer because that would be no consumer for the output datastore. A DataTransformer is only included in a pipeline graph IFF there is a consumer for its output datastore. The lint for Workspace needs to be updated to reflect either platformMD or a dataTransformer. DataTransformer workspaces basically implicitly use the platformMD of the terminal Workspace which caused it to be included in the pipeline graph based on that terminal Workspace's dataset requirements.

Fixed.

## Separate the execute transformer job to use its own CLI main in that file.

### Implementation Status: ✅ COMPLETED

The `transformerjob.py` now has its own CLI main function that:

- **Accepts DataTransformer-specific arguments**: `--platform-name`, `--workspace-name`, `--operation`, `--working-folder`, etc.
- **Handles ecosystem model loading**: Clones git repository and loads the latest model
- **Validates workspace configuration**: Ensures workspace exists and has a DataTransformer
- **Executes DataTransformer jobs**: Creates and runs `DataTransformerJob` instances
- **Returns proper exit codes**: 0 for success, 1 for keep working, -1 for errors
- **Follows same pattern as jobs.py**: Uses `DATASURFACE_RESULT_CODE` for Airflow log parsing

DataTransformer DAGs now call `transformerjob.py` directly instead of going through `jobs.py`, providing better separation of concerns and cleaner code organization.

#### Updated DataTransformer Factory DAG Template

The `datatransformer_factory_dag.py.j2` template has been updated to:

- **Call the new CLI**: Uses `python -m datasurface.platforms.yellow.transformerjob` instead of `jobs`
- **Simplified arguments**: Removes unnecessary database connection arguments (handled internally)
- **Correct parameter order**: Matches the new CLI interface for `transformerjob.py`
- **Cleaner separation**: DataTransformer execution is now completely separated from regular ingestion jobs

