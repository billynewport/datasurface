"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import argparse
import sys
from typing import List, Optional, Tuple
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine

from datasurface.md import Ecosystem, DataPlatform, PlatformPipelineGraph, EcosystemPipelineGraph
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md.sqlalchemyutils import createOrUpdateView
from datasurface.md.credential import CredentialStore
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform, YellowMilestoneStrategy
from datasurface.md.schema import DDLTable
from datasurface.platforms.yellow.jobs import createEngine, YellowDatasetUtilities


def generate_view_name(dataplatform_name: str, workspace_name: str, dsg_name: str, dataset_name: str) -> str:
    """Generate a view name following the pattern: dataplatform/workspace/dsg/dataset_view"""
    # Convert to lowercase and replace spaces/special chars with underscores
    dp_name = dataplatform_name.lower().replace(' ', '_').replace('-', '_')
    ws_name = workspace_name.lower().replace(' ', '_').replace('-', '_')
    dsg_name = dsg_name.lower().replace(' ', '_').replace('-', '_')
    dataset_name = dataset_name.lower().replace(' ', '_').replace('-', '_')

    return f"{dp_name}_{ws_name}_{dsg_name}_{dataset_name}_view"


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

    if yellow_dp.milestoneStrategy != YellowMilestoneStrategy.LIVE_ONLY:
        print(f"WARNING: Skipping Data platform '{dataplatform_name}', live only supported for views at the moment")
        return 1

    print(f"Reconciling workspace view schemas for platform: {dataplatform_name}")

    # Generate the platform pipeline graph
    ecoGraph = EcosystemPipelineGraph(eco)
    print(f"DEBUG: ecoGraph.roots keys: {list(ecoGraph.roots.keys())}")
    print(f"DEBUG: yellow_dp.name: {yellow_dp.name}")

    pipeline_graph: Optional[PlatformPipelineGraph] = ecoGraph.roots[yellow_dp.name]

    if pipeline_graph is None:
        print(f"Error: No pipeline graph found for platform: {dataplatform_name}")
        return 1

    print(f"DEBUG: Found pipeline graph for platform: {dataplatform_name}")
    print(f"DEBUG: pipeline_graph.workspaces keys: {list(pipeline_graph.workspaces.keys())}")

    # Get database connection using the comprehensive createEngine method
    try:
        db_user, db_password = cred_store.getAsUserPassword(yellow_dp.postgresCredential)
        engine = createEngine(yellow_dp.mergeStore, db_user, db_password)
        print("Connected to the merge store database")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return 1

    # Track results
    views_created = 0
    views_updated = 0
    views_failed = 0

    # Process all DSG datasinks assigned to our dataplatform
    print(f"DEBUG: eco.dsgPlatformMappings keys: {list(eco.dsgPlatformMappings.keys())}")

    for dsgAssignment in eco.dsgPlatformMappings.values():
        print(f"DEBUG: Processing dsgAssignment: {dsgAssignment}")
        for assigment in dsgAssignment.assignments:
            print(f"DEBUG: Processing assignment: {assigment}")
            print(f"DEBUG: assignment.dataPlatform.name: {assigment.dataPlatform.name}")
            print(f"DEBUG: dataplatform_name: {dataplatform_name}")
            if assigment.dataPlatform.name == dataplatform_name:
                print(f"DEBUG: Found matching assignment for platform: {dataplatform_name}")
                workspace = pipeline_graph.workspaces.get(assigment.workspace)
                print(f"DEBUG: workspace: {workspace}")
                if workspace is None:
                    print(f"DEBUG: Workspace {assigment.workspace} not found in pipeline graph")
                    continue
                dataset_group = workspace.dsgs[assigment.dsgName]
                print(f"DEBUG: dataset_group: {dataset_group}")
                print(f"DEBUG: dataset_group.sinks: {list(dataset_group.sinks.keys())}")
                for sink in dataset_group.sinks.values():
                    print(f"DEBUG: Processing sink: {sink}")
                    
                    # Create utilities instance for this specific sink
                    try:
                        store_entry = eco.cache_getDatastoreOrThrow(sink.storeName)
                        utils = YellowDatasetUtilities(eco, cred_store, yellow_dp, store_entry.datastore, sink.datasetName)
                        
                        if utils.dataset is None:
                            print(f"Error: Dataset {sink.datasetName} not found in store {sink.storeName}")
                            views_failed += 1
                            continue
                            
                    except Exception as e:
                        print(f"Error: Could not load datastore {sink.storeName}: {e}")
                        views_failed += 1
                        continue
                    
                    # Generate names using utilities and conventional methods
                    view_name = generate_view_name(dataplatform_name, workspace.name, dataset_group.name, sink.datasetName)
                    merge_table_name = utils.getMergeTableNameForDataset(utils.dataset)
                    print(f"DEBUG: view_name: {view_name}")
                    print(f"DEBUG: merge_table_name: {merge_table_name}")

                    # Get the original dataset schema - we already have it from utils.dataset
                    original_schema = utils.dataset.originalSchema
                    if not isinstance(original_schema, DDLTable):
                        print(f"Error: Invalid schema type for dataset {sink.datasetName} in store {sink.storeName}")
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

                    # Create or update the view using the dataset from utils
                    try:
                        was_changed = createOrUpdateView(engine, utils.dataset, view_name, merge_table_name)
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

        if validation_tree and validation_tree.hasErrors():
            print("Ecosystem validation failed:")
            validation_tree.printTree()
            return 1

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
