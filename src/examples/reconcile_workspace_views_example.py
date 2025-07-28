"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import os
import sys

# Add the src directory to the path so we can import the utility
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

"""
Example: Reconcile Workspace View Schemas

This example demonstrates how to use the reconcile_workspace_views.py command-line utility
to create and update workspace views in a YellowDataPlatform.

Prerequisites:
1. A YellowDataPlatform configured in your ecosystem
2. Workspaces with LIVE_ONLY retention policy
3. Merge tables exist in the database
4. Database credentials available as environment variables

Usage:
    python reconcile_workspace_views.py --model my_ecosystem --platform my_yellow_platform

Environment Variables Required:
    - POSTGRES_USER: Database username
    - POSTGRES_PASSWORD: Database password
    - POSTGRES_HOST: Database host (if not using platform default)
    - POSTGRES_PORT: Database port (if not using platform default)
    - POSTGRES_DB: Database name (if not using platform default)

Example ecosystem structure:
    - YellowDataPlatform named "my_yellow_platform"
    - Workspace named "analytics_workspace" with LIVE_ONLY retention
    - DatasetGroup named "customer_data" with dataset "customers"
    - Merge table "customers_merge" exists in database

The utility will:
1. Load the ecosystem model
2. Generate the platform pipeline graph
3. Find all ExportNodes for LIVE_ONLY workspaces
4. Create views named: my_yellow_platform_analytics_workspace_customer_data_customers_view
5. Point views to merge tables: customers_merge
6. Include only original dataset columns (excluding merge table columns like batch_id, hashes)

Exit Codes:
    0: All views successfully created/updated
    1: Some views could not be created (missing tables/columns)
"""


def main():
    """Example usage of the reconcile workspace views utility."""
    
    # Example ecosystem model path (adjust to your actual path)
    model_path = "path/to/your/ecosystem"
    
    # Example platform name (adjust to your actual platform name)
    platform_name = "my_yellow_platform"
    
    # Set up environment variables for database connection
    # In a real deployment, these would be set by your deployment system
    os.environ.setdefault("POSTGRES_USER", "datasurface_user")
    os.environ.setdefault("POSTGRES_PASSWORD", "datasurface_password")
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "5432")
    os.environ.setdefault("POSTGRES_DB", "datasurface_merge")
    
    print("Example: Reconcile Workspace View Schemas")
    print("=" * 50)
    print(f"Model path: {model_path}")
    print(f"Platform: {platform_name}")
    print()
    
    # Example command that would be run
    command = f"python reconcile_workspace_views.py --model {model_path} --platform {platform_name}"
    print("Command to run:")
    print(f"  {command}")
    print()
    
    print("Expected output:")
    print("  Loading ecosystem model from module: path/to/your/ecosystem")
    print("  Validating ecosystem...")
    print("  Reconciling workspace view schemas for platform: my_yellow_platform")
    print("  Created view: my_yellow_platform_analytics_workspace_customer_data_customers_view")
    print("  Updated view: my_yellow_platform_analytics_workspace_customer_data_orders_view")
    print("  View already up to date: my_yellow_platform_analytics_workspace_customer_data_products_view")
    print()
    print("  Summary:")
    print("    Views created: 1")
    print("    Views updated: 1")
    print("    Views failed: 0")
    print()
    print("  Exit code: 0 (all views successfully processed)")
    print()
    
    print("Example ecosystem structure:")
    print("  Ecosystem:")
    print("    YellowDataPlatform: my_yellow_platform")
    print("      - mergeStore: PostgresDatabase")
    print("      - postgresCredential: UserPassword credential")
    print("    Workspace: analytics_workspace")
    print("      - dataContainer: PostgresDatabase")
    print("      - DatasetGroup: customer_data")
    print("        - retention: LIVE_ONLY")
    print("        - sinks:")
    print("          - storeName: customers")
    print("            datasetName: customer_data")
    print("    Datastore: customers")
    print("      - datasets:")
    print("        - customer_data (original schema)")
    print("      - merge table: customers_merge (includes batch_id, hashes)")
    print()
    
    print("Generated view SQL example:")
    print("  CREATE OR REPLACE VIEW my_yellow_platform_analytics_workspace_customer_data_customers_view AS")
    print("  SELECT id, name, email, created_date")
    print("  FROM customers_merge;")
    print()
    
    print("Note: The view only includes original dataset columns, not merge table columns like:")
    print("  - ds_surf_batch_id")
    print("  - ds_surf_all_hash")
    print("  - ds_surf_key_hash")


if __name__ == "__main__":
    main() 