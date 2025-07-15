# Reconcile Workspace View Schemas

This command-line utility creates and updates workspace views in a YellowDataPlatform based on the ecosystem model and platform pipeline graph.

## Overview

The `reconcile_workspace_views.py` utility:

1. **Loads an ecosystem model** from a Python module
2. **Generates a platform pipeline graph** for the specified YellowDataPlatform
3. **Finds all ExportNodes** in the graph that represent workspace exports
4. **Creates views** for workspaces with `LIVE_ONLY` retention policy
5. **Points views to merge tables** while including only original dataset columns
6. **Reports results** with appropriate exit codes

## Usage

```bash
python reconcile_workspace_views.py --model <ecosystem_module> --platform <platform_name> [--credential-store <type>]
```

### Arguments

- `--model`: Python module name containing the ecosystem model (e.g., 'my_ecosystem')
- `--platform`: Name of the YellowDataPlatform to reconcile views for
- `--credential-store`: Credential store type (default: 'env' for environment variables)

### Examples

```bash
# Basic usage
python reconcile_workspace_views.py --model my_ecosystem --platform yellow-dp

# With custom credential store
python reconcile_workspace_views.py --model eco --platform yellow-dp --credential-store env
```

## Environment Variables

When using the default environment variable credential store, set these variables:

- `POSTGRES_USER`: Database username
- `POSTGRES_PASSWORD`: Database password
- `POSTGRES_HOST`: Database host (optional, uses platform default)
- `POSTGRES_PORT`: Database port (optional, uses platform default)
- `POSTGRES_DB`: Database name (optional, uses platform default)

## View Naming Convention

Views are named using the pattern:

```text
{dataplatform}_{workspace}_{dsg}_{dataset}_view
```

Example: `yellowdp_analytics_workspace_customer_data_customers_view`

## Merge Table Naming Convention

Merge tables are expected to be named:

```text
{store}_{dataset}_merge
```

Example: `customers_customer_data_merge`

## View Content

Views include only the original dataset columns, excluding merge table columns like:

- `ds_surf_batch_id`
- `ds_surf_all_hash`
- `ds_surf_key_hash`

Example view SQL:

```sql
CREATE OR REPLACE VIEW yellowdp_analytics_workspace_customer_data_customers_view AS
SELECT id, name, email, created_date
FROM customers_customer_data_merge;
```

## Exit Codes

- **0**: All views successfully created/updated
- **1**: Some views could not be created/updated (missing tables or columns)

## Prerequisites

1. **YellowDataPlatform**: Must be configured in the ecosystem
2. **Workspaces**: Must have `LIVE_ONLY` retention policy
3. **Merge Tables**: Must exist in the database
4. **Database Access**: Credentials must be available
5. **Ecosystem Model**: Must be valid and pass linting

## Error Handling

The utility handles various error conditions:

- **Platform not found**: Reports error and exits with code 1
- **Wrong platform type**: Reports error if platform is not YellowDataPlatform
- **Missing merge tables**: Reports error for each missing table
- **Missing columns**: Reports error for each missing column in merge table
- **Database connection issues**: Reports connection errors
- **Invalid ecosystem**: Reports validation errors

## Integration

This utility is designed to be integrated into:

- **CI/CD pipelines**: Run after schema changes
- **Airflow DAGs**: Run as part of data pipeline
- **Kubernetes jobs**: Run as a scheduled job
- **Manual operations**: Run when needed for schema updates

## Example Output

```text
Loading ecosystem model from module: my_ecosystem
Validating ecosystem...
Reconciling workspace view schemas for platform: yellow-dp
Created view: yellowdp_analytics_workspace_customer_data_customers_view
Updated view: yellowdp_analytics_workspace_customer_data_orders_view
View already up to date: yellowdp_analytics_workspace_customer_data_products_view

Summary:
  Views created: 1
  Views updated: 1
  Views failed: 0

Exit code: 0 (all views successfully processed)
```

## Testing

Run the test suite:

```bash
python -m pytest tests/test_reconcile_workspace_views.py -v
```

## Dependencies

- `datasurface.md`: Core DataSurface model classes
- `sqlalchemy`: Database operations
- `datasurface.platforms.yellow`: YellowDataPlatform implementation
- `argparse`: Command-line argument parsing

## Security Considerations

- Database credentials should be managed securely (e.g., Kubernetes secrets)
- The utility only creates/updates views, not tables
- Views inherit permissions from underlying merge tables
- No data modification is performed

## Troubleshooting

### Common Issues

1. **"Platform not found"**: Check that the platform name matches exactly
2. **"Database connection failed"**: Verify credentials and network connectivity
3. **"Merge table does not exist"**: Ensure merge tables are created before running
4. **"Missing columns"**: Verify merge table schema matches expected columns

### Debug Mode

For debugging, you can add print statements to the utility or run with verbose logging.

## Related Files

- `reconcile_workspace_views.py`: Main utility script
- `test_reconcile_workspace_views.py`: Test suite
- `examples/reconcile_workspace_views_example.py`: Usage example
- `yellow_dp.py`: YellowDataPlatform implementation
- `sqlalchemyutils.py`: Database utility functions
