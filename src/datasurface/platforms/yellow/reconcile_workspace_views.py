"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import argparse
import sys
from typing import List, Optional, Tuple
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine

from datasurface.md import Ecosystem, DataPlatform, PlatformPipelineGraph, ExportNode, Workspace, DatasetGroup
from datasurface.md import DataMilestoningStrategy, WorkspacePlatformConfig
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md.sqlalchemyutils import createOrUpdateView
from datasurface.md.credential import CredentialStore
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform
from datasurface.md.schema import DDLTable
from datasurface.platforms.yellow.jobs import createEngine


def generate_view_name(dataplatform_name: str, workspace_name: str, dsg_name: str, dataset_name: str) -> str:
    """Generate a view name following the pattern: dataplatform/workspace/dsg/dataset_view"""
    # Convert to lowercase and replace spaces/special chars with underscores
    dp_name = dataplatform_name.lower().replace(' ', '_').replace('-', '_')
    ws_name = workspace_name.lower().replace(' ', '_').replace('-', '_')
    dsg_name = dsg_name.lower().replace(' ', '_').replace('-', '_')
    dataset_name = dataset_name.lower().replace(' ', '_').replace('-', '_')

    return f"{dp_name}_{ws_name}_{dsg_name}_{dataset_name}_view"


def generate_merge_table_name(store_name: str, dataset_name: str) -> str:
    """Generate the merge table name for a dataset"""
    # Convert to lowercase and replace spaces/special chars with underscores
    store_name = store_name.lower().replace(' ', '_').replace('-', '_')
    dataset_name = dataset_name.lower().replace(' ', '_').replace('-', '_')

    return f"{store_name}_{dataset_name}_merge"


def get_original_dataset_schema(dataset_name: str, store_name: str, eco: Ecosystem) -> Optional[DDLTable]:
    """Get the original dataset schema (without merge table columns)"""
    try:
        store_entry = eco.cache_getDatastoreOrThrow(store_name)
        dataset = store_entry.datastore.datasets.get(dataset_name)
        if dataset and isinstance(dataset.originalSchema, DDLTable):
            return dataset.originalSchema
    except Exception:
        pass
    return None


def check_merge_table_exists(engine: Engine, merge_table_name: str) -> bool:
    """Check if the merge table exists in the database"""
    inspector = inspect(engine)
    return inspector.has_table(merge_table_name)


def check_merge_table_has_required_columns(engine: Engine, merge_table_name: str, required_columns: List[str]) -> Tuple[bool, List[str]]:
    """Check if the merge table has all required columns for the view"""
    try:
        with engine.connect() as conn:
            # Get column information from the merge table
            result = conn.execute(text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{merge_table_name}'
                AND table_schema = 'public'
            """))
            existing_columns = [row[0] for row in result.fetchall()]

            missing_columns = [col for col in required_columns if col not in existing_columns]
            return len(missing_columns) == 0, missing_columns
    except Exception:
        return False, required_columns


def reconcile_workspace_view_schemas(eco: Ecosystem, dataplatform_name: str, cred_store: CredentialStore) -> int:
    """
    Reconcile workspace view schemas for the specified data platform.

    Returns:
        0: All views were successfully created/updated
        1: Some views could not be created/updated (missing columns or tables)
    """
    # Find the specified data platform
    dataplatform: Optional[DataPlatform] = None
    try:
        dataplatform = eco.getDataPlatformOrThrow(dataplatform_name)
    except Exception:
        print(f"Error: Data platform '{dataplatform_name}' not found in ecosystem")
        return 1

    if not isinstance(dataplatform, YellowDataPlatform):
        print(f"Error: Data platform '{dataplatform_name}' is not a YellowDataPlatform")
        return 1

    yellow_dp: YellowDataPlatform = dataplatform

    # Generate the platform pipeline graph
    pipeline_graph = PlatformPipelineGraph(eco, yellow_dp)
    pipeline_graph.generateGraph()

    # Get database connection using the comprehensive createEngine method
    try:
        db_user, db_password = cred_store.getAsUserPassword(yellow_dp.postgresCredential)
        engine = createEngine(yellow_dp.mergeStore, db_user, db_password)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return 1

    # Track results
    views_created = 0
    views_updated = 0
    views_failed = 0

    # Process all ExportNodes in the graph

    for node in pipeline_graph.nodes.values():
        if isinstance(node, ExportNode):
            export_node: ExportNode = node

            # Find the workspace and dataset group for this export
            workspace: Optional[Workspace] = None
            dataset_group: Optional[DatasetGroup] = None

            # Find the workspace that uses this export
            for ws in pipeline_graph.workspaces.values():
                if ws.dataContainer == export_node.dataContainer:
                    for dsg in ws.dsgs.values():
                        for sink in dsg.sinks.values():
                            if sink.storeName == export_node.storeName and sink.datasetName == export_node.datasetName:
                                workspace = ws
                                dataset_group = dsg
                                break
                        if workspace:
                            break
                    if workspace:
                        break

            if not workspace or not dataset_group:
                print(f"Warning: Could not find workspace/dataset group for export {export_node.name}")
                views_failed += 1
                continue

            # Check if this workspace uses LIVE_ONLY retention policy
            if not isinstance(dataset_group.platformMD, WorkspacePlatformConfig):
                print(f"Warning: Dataset group {dataset_group.name} in workspace {workspace.name} does not have WorkspacePlatformConfig")
                views_failed += 1
                continue

            platform_config: WorkspacePlatformConfig = dataset_group.platformMD
            if platform_config.retention.milestoningStrategy != DataMilestoningStrategy.LIVE_ONLY:
                print(f"Info: Skipping workspace {workspace.name} - not LIVE_ONLY (policy: {platform_config.retention.milestoningStrategy.name})")
                continue

            # Generate view and table names
            view_name = generate_view_name(dataplatform_name, workspace.name, dataset_group.name, export_node.datasetName)
            merge_table_name = generate_merge_table_name(export_node.storeName, export_node.datasetName)

            # Get the original dataset schema (without merge table columns)
            original_schema = get_original_dataset_schema(export_node.datasetName, export_node.storeName, eco)
            if not original_schema:
                print(f"Error: Could not find original schema for dataset {export_node.datasetName} in store {export_node.storeName}")
                views_failed += 1
                continue

            # Check if merge table exists
            if not check_merge_table_exists(engine, merge_table_name):
                print(f"Error: Merge table '{merge_table_name}' does not exist for view '{view_name}'")
                views_failed += 1
                continue

            # Check if merge table has all required columns
            required_columns = list(original_schema.columns.keys())
            has_columns, missing_columns = check_merge_table_has_required_columns(engine, merge_table_name, required_columns)

            if not has_columns:
                print(f"Error: Merge table '{merge_table_name}' missing columns for view '{view_name}': {missing_columns}")
                views_failed += 1
                continue

            # Create a dataset object for the view creation
            from datasurface.md import Dataset
            dataset = Dataset(export_node.datasetName, original_schema)

            # Create or update the view
            try:
                was_changed = createOrUpdateView(engine, dataset, view_name, merge_table_name)
                if was_changed:
                    if check_merge_table_exists(engine, view_name):
                        views_updated += 1
                        print(f"Updated view: {view_name}")
                    else:
                        views_created += 1
                        print(f"Created view: {view_name}")
                else:
                    print(f"View already up to date: {view_name}")
            except Exception as e:
                print(f"Error creating/updating view '{view_name}': {e}")
                views_failed += 1

    # Print summary
    print("\nSummary:")
    print(f"  Views created: {views_created}")
    print(f"  Views updated: {views_updated}")
    print(f"  Views failed: {views_failed}")

    # Return appropriate exit code
    if views_failed > 0:
        print("\nExit code: 1 (some views could not be created/updated)")
        return 1
    else:
        print("\nExit code: 0 (all views successfully processed)")
        return 0


def main():
    """Main entry point for the command-line utility"""
    parser = argparse.ArgumentParser(
        description="Reconcile workspace view schemas for YellowDataPlatform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python reconcile_workspace_views.py --model my_ecosystem --platform my_yellow_platform
  python reconcile_workspace_views.py --model eco --platform yellow-dp --credential-store env
        """
    )

    parser.add_argument(
        "--model",
        required=True,
        help="Python module name containing the ecosystem model (e.g., 'my_ecosystem')"
    )

    parser.add_argument(
        "--platform",
        required=True,
        help="Name of the YellowDataPlatform to reconcile views for"
    )

    parser.add_argument(
        "--credential-store",
        default="env",
        help="Credential store type (default: env for environment variables)"
    )

    args = parser.parse_args()

    try:
        # Load the ecosystem model
        print(f"Loading ecosystem model from module: {args.model}")
        eco, validation_tree = loadEcosystemFromEcoModule(args.model)

        if eco is None:
            print(f"Error: Could not load ecosystem model from module '{args.model}'")
            return 1

        # Validate the ecosystem
        print("Validating ecosystem...")
        if validation_tree and validation_tree.hasErrors():
            print("Ecosystem validation failed:")
            for problem in validation_tree.getProblems():
                print(f"  {problem}")
            return 1

        # Create credential store
        if args.credential_store == "env":
            from datasurface.platforms.yellow.yellow_dp import KubernetesEnvVarsCredentialStore
            cred_store = KubernetesEnvVarsCredentialStore(
                name="env-cred-store",
                locs=set(),  # Will be populated by the platform
                namespace="default"  # Will be overridden by the platform
            )
        else:
            print(f"Error: Unsupported credential store type: {args.credential_store}")
            return 1

        # Reconcile workspace view schemas
        print(f"Reconciling workspace view schemas for platform: {args.platform}")
        return reconcile_workspace_view_schemas(eco, args.platform, cred_store)

    except ImportError as e:
        print(f"Error importing ecosystem model '{args.model}': {e}")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())