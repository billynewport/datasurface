"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import argparse
import sys
from typing import List, Optional, Tuple
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine

from datasurface.md import Ecosystem, DataPlatform, PlatformPipelineGraph, EcosystemPipelineGraph, PlatformServicesProvider
from datasurface.md.sqlalchemyutils import createOrUpdateView
from datasurface.md.credential import CredentialStore
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform, YellowMilestoneStrategy
from datasurface.md.schema import DDLTable
from datasurface.platforms.yellow.yellow_dp import YellowDatasetUtilities, createEngine
from datasurface.platforms.yellow.db_utils import createInspector
from datasurface.platforms.yellow.logging_utils import (
    setup_logging_for_environment, get_contextual_logger, set_context,
    log_operation_timing
)
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants

# Setup logging for Kubernetes environment
setup_logging_for_environment()
logger = get_contextual_logger(__name__)


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


def reconcile_workspace_view_schemas(eco: Ecosystem, psp_name: str, cred_store: CredentialStore) -> int:
    # Find the specified data platform
    psp: Optional[PlatformServicesProvider] = None
    for psp_item in eco.platformServicesProviders:
        if psp_item.name == psp_name:
            psp = psp_item
            break
    for dataplatform in psp.dataPlatforms.values():
        rc: int = reconcile_workspace_view_schemas_for_dp(eco, psp_name, cred_store, dataplatform)
        if rc != 0:
            return rc
    return 0


def reconcile_workspace_view_schemas_for_dp(eco: Ecosystem, psp_name: str, cred_store: CredentialStore, dataplatform: DataPlatform) -> int:
    """
    Reconcile workspace view schemas for the specified data platform.

    Returns:
        0: All views were successfully created/updated
        1: Some views could not be created/updated (missing columns or tables)
    """

    if not isinstance(dataplatform, YellowDataPlatform):
        logger.error("Data platform is not a YellowDataPlatform", platform_name=dataplatform.name, platform_type=type(dataplatform).__name__)
        return 1

    yellow_dp: YellowDataPlatform = dataplatform

    # Set logging context for this reconciliation operation
    set_context(platform=yellow_dp.name)

    logger.info("Starting workspace view schema reconciliation", platform_name=dataplatform.name, milestone_strategy=yellow_dp.milestoneStrategy.value)

    # Generate the platform pipeline graph
    ecoGraph = EcosystemPipelineGraph(eco)
    logger.debug("Generated ecosystem pipeline graph", root_platforms=list(ecoGraph.roots.keys()))

    pipeline_graph: Optional[PlatformPipelineGraph] = ecoGraph.roots[yellow_dp.name]

    if pipeline_graph is None:
        logger.error("No pipeline graph found for platform", platform_name=dataplatform.name)
        return 1

    logger.debug(
        "Found pipeline graph for platform",
        platform_name=dataplatform.name,
        workspace_count=len(pipeline_graph.workspaces),
        stores_to_ingest=list(pipeline_graph.storesToIngest))

    # Get database connection using the comprehensive createEngine method
    try:
        with log_operation_timing(logger, "database_connection_setup"):
            db_user, db_password = cred_store.getAsUserPassword(yellow_dp.psp.mergeRW_Credential)
            engine = createEngine(yellow_dp.psp.mergeStore, db_user, db_password)
        logger.info("Connected to merge store database")
    except Exception as e:
        logger.error("Failed to connect to database", error=str(e))
        return 1

    # Create schema projector to access constants and determine platform behavior
    is_forensic_platform = yellow_dp.milestoneStrategy == YellowMilestoneStrategy.SCD2

    logger.info("Platform configuration determined",
                platform_type="Forensic (BATCH_MILESTONED)" if is_forensic_platform else "Live-only (LIVE_ONLY)")

    # Track results
    views_created = 0
    views_updated = 0
    views_failed = 0

    # STEP 1: First, collect all unique stores that need processing and create/update their merge tables
    logger.info("=== STEP 1: Creating/Updating Merge Tables ===")

    # Get all stores that need to be ingested from the pipeline graph
    stores_to_process = pipeline_graph.storesToIngest

    logger.info("Found stores to process", store_count=len(stores_to_process), stores=sorted(stores_to_process))

    # Create/update merge tables for each store
    merge_tables_processed = 0
    merge_tables_failed = 0

    inspector = createInspector(engine)
    with engine.begin() as connection:
        for store_name in sorted(stores_to_process):
            try:
                logger.info("Processing merge tables for store", store_name=store_name)
                store_entry = eco.cache_getDatastoreOrThrow(store_name)
                store = store_entry.datastore

                # Create a YellowDatasetUtilities instance for this store
                utils = YellowDatasetUtilities(eco, cred_store, yellow_dp, store)

                # Create/update all merge tables for this store
                with log_operation_timing(logger, "reconcile_merge_tables", store_name=store_name):
                    utils.reconcileMergeTableSchemas(connection, inspector, store)

                logger.info("Successfully processed merge tables for store",
                            store_name=store_name,
                            dataset_count=len(store.datasets))
                merge_tables_processed += 1

            except Exception as e:
                logger.error("Error processing merge tables for store", store_name=store_name, error=str(e))
                merge_tables_failed += 1

    logger.info("Merge table processing summary",
                stores_processed=merge_tables_processed,
                stores_failed=merge_tables_failed)

    # STEP 2: Now proceed with view creation/update
    logger.info("=== STEP 2: Creating/Updating Views ===")

    # Process all DSG datasinks assigned to our dataplatform
    logger.debug("Processing DSG platform mappings", mapping_count=len(eco.dsgPlatformMappings))

    for dsgAssignment in eco.dsgPlatformMappings.values():
        logger.debug("Processing DSG assignment", assignment=str(dsgAssignment))
        for assigment in dsgAssignment.assignments:
            logger.debug("Processing assignment",
                         assignment_platform=assigment.dataPlatform.name,
                         target_platform=dataplatform.name)
            if assigment.dataPlatform.name == dataplatform.name:
                logger.debug("Found matching assignment for platform", platform_name=dataplatform.name)
                workspace = pipeline_graph.workspaces.get(assigment.workspace)
                if workspace is None:
                    logger.debug("Workspace not found in pipeline graph", workspace_name=assigment.workspace)
                    continue
                dataset_group = workspace.dsgs[assigment.dsgName]
                logger.debug("Processing dataset group",
                             dsg_name=dataset_group.name,
                             sink_count=len(dataset_group.sinks))
                for sink in dataset_group.sinks.values():
                    logger.debug("Processing sink",
                                 store_name=sink.storeName,
                                 dataset_name=sink.datasetName)

                    # Create utilities instance for this specific sink
                    try:
                        store_entry = eco.cache_getDatastoreOrThrow(sink.storeName)
                        utils = YellowDatasetUtilities(eco, cred_store, yellow_dp, store_entry.datastore, sink.datasetName)

                        if utils.dataset is None:
                            logger.error("Dataset not found in store",
                                         dataset_name=sink.datasetName,
                                         store_name=sink.storeName)
                            views_failed += 1
                            continue

                    except Exception as e:
                        logger.error("Could not load datastore",
                                     store_name=sink.storeName,
                                     error=str(e))
                        views_failed += 1
                        continue

                    # Generate merge table name using utilities
                    merge_table_name = utils.getPhysMergeTableNameForDataset(utils.dataset)
                    logger.debug("Generated merge table name",
                                 merge_table_name=merge_table_name,
                                 dataset_name=sink.datasetName)

                    # Get the original dataset schema - we already have it from utils.dataset
                    original_schema = utils.dataset.originalSchema
                    if not isinstance(original_schema, DDLTable):
                        logger.error("Invalid schema type for dataset",
                                     dataset_name=sink.datasetName,
                                     store_name=sink.storeName,
                                     schema_type=type(original_schema).__name__)
                        views_failed += 1
                        continue

                    # Check if merge table exists
                    if not check_merge_table_exists(engine, merge_table_name):
                        logger.error("Merge table does not exist for dataset",
                                     merge_table_name=merge_table_name,
                                     dataset_name=sink.datasetName)
                        views_failed += 1
                        continue

                    # Check if merge table has all required columns
                    required_columns = list(original_schema.columns.keys())
                    has_columns, missing_columns = check_merge_table_has_required_columns(engine, merge_table_name, required_columns)

                    if not has_columns:
                        logger.error("Merge table missing required columns",
                                     merge_table_name=merge_table_name,
                                     dataset_name=sink.datasetName,
                                     missing_columns=missing_columns)
                        views_failed += 1
                        continue

                    # Create views based on platform type
                    try:
                        view_changes = []

                        with engine.begin() as connection:
                            if is_forensic_platform:
                                # For forensic platforms: create both _view_full and _live views

                                # 1. Create full view (all historical records)
                                full_view_name = utils.getPhysWorkspaceFullViewName(workspace.name, dataset_group.name)

                                with log_operation_timing(logger, "create_full_view", view_name=full_view_name):
                                    full_was_changed = createOrUpdateView(connection, inspector, utils.dataset, full_view_name, merge_table_name)

                                if full_was_changed:
                                    if check_merge_table_exists(engine, full_view_name):
                                        views_updated += 1
                                        view_changes.append(f"Updated full view: {full_view_name}")
                                    else:
                                        views_created += 1
                                        view_changes.append(f"Created full view: {full_view_name}")
                                else:
                                    view_changes.append(f"Full view already up to date: {full_view_name}")

                                # 2. Create live view (only live records with WHERE clause)
                                live_view_name = utils.getPhysWorkspaceLiveViewName(workspace.name, dataset_group.name)
                                live_where_clause = f"{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = {YellowSchemaConstants.LIVE_RECORD_ID}"

                                with log_operation_timing(logger, "create_live_view", view_name=live_view_name):
                                    live_was_changed = createOrUpdateView(connection, inspector, utils.dataset, live_view_name,
                                                                          merge_table_name, live_where_clause)

                                if live_was_changed:
                                    if check_merge_table_exists(engine, live_view_name):
                                        views_updated += 1
                                        view_changes.append(f"Updated live view: {live_view_name}")
                                    else:
                                        views_created += 1
                                        view_changes.append(f"Created live view: {live_view_name}")
                                else:
                                    view_changes.append(f"Live view already up to date: {live_view_name}")

                            else:
                                # For live-only platforms: create only _live view (no filtering needed)
                                live_view_name = utils.getPhysWorkspaceLiveViewName(workspace.name, dataset_group.name)

                                with log_operation_timing(logger, "create_live_view", view_name=live_view_name):
                                    live_was_changed = createOrUpdateView(connection, inspector, utils.dataset, live_view_name,
                                                                          merge_table_name, None)

                                if live_was_changed:
                                    if check_merge_table_exists(engine, live_view_name):
                                        views_updated += 1
                                        view_changes.append(f"Updated live view: {live_view_name}")
                                    else:
                                        views_created += 1
                                        view_changes.append(f"Created live view: {live_view_name}")
                                else:
                                    view_changes.append(f"Live view already up to date: {live_view_name}")

                            # Log all view changes for this dataset
                            for change in view_changes:
                                logger.info("View operation completed", operation=change)

                    except Exception as e:
                        logger.error("Error creating/updating views for dataset",
                                     dataset_name=sink.datasetName,
                                     error=str(e))
                        views_failed += 1

    # STEP 3: Process all workspaces in pipeline graph that weren't already processed by DSG assignments
    logger.info("=== STEP 3: Processing Pipeline Graph Workspaces ===")

    # Track which workspaces were already processed by DSG assignments
    processed_workspace_dsgs = set()
    for dsgAssignment in eco.dsgPlatformMappings.values():
        for assignment in dsgAssignment.assignments:
            if assignment.dataPlatform.name == dataplatform.name:
                processed_workspace_dsgs.add(f"{assignment.workspace}#{assignment.dsgName}")

    logger.debug("Already processed workspace DSGs", processed_count=len(processed_workspace_dsgs), processed_dsgs=list(processed_workspace_dsgs))

    # Now process all workspaces in the pipeline graph
    for workspace_name, workspace in pipeline_graph.workspaces.items():
        logger.debug("Checking workspace in pipeline graph", workspace_name=workspace_name, dsg_count=len(workspace.dsgs))

        for dsg_name, dataset_group in workspace.dsgs.items():
            workspace_dsg_key = f"{workspace_name}#{dsg_name}"

            # Skip if this workspace+dsg was already processed by explicit DSG assignments
            if workspace_dsg_key in processed_workspace_dsgs:
                logger.debug("Skipping already processed workspace DSG", workspace_dsg_key=workspace_dsg_key)
                continue

            logger.info("Processing unassigned workspace DSG", workspace_name=workspace_name, dsg_name=dsg_name, sink_count=len(dataset_group.sinks))

            for sink in dataset_group.sinks.values():
                logger.debug("Processing unassigned sink", store_name=sink.storeName, dataset_name=sink.datasetName)

                # Create utilities instance for this specific sink
                try:
                    store_entry = eco.cache_getDatastoreOrThrow(sink.storeName)
                except Exception as e:
                    logger.error("Could not load datastore for unassigned workspace",
                                 store_name=sink.storeName,
                                 error=str(e))
                    views_failed += 1
                    continue

                utils = YellowDatasetUtilities(eco, cred_store, yellow_dp, store_entry.datastore, sink.datasetName)

                if utils.dataset is None:
                    logger.error("Dataset not found in store",
                                 dataset_name=sink.datasetName,
                                 store_name=sink.storeName)
                    views_failed += 1
                    continue
                # Generate merge table name using utilities
                merge_table_name = utils.getPhysMergeTableNameForDataset(utils.dataset)
                logger.debug("Generated merge table name for unassigned workspace",
                             merge_table_name=merge_table_name,
                             dataset_name=sink.datasetName)

                # Get the original dataset schema
                original_schema = utils.dataset.originalSchema
                if not isinstance(original_schema, DDLTable):
                    logger.error("Invalid schema type for dataset in unassigned workspace",
                                 dataset_name=sink.datasetName,
                                 store_name=sink.storeName,
                                 schema_type=type(original_schema).__name__)
                    views_failed += 1
                    continue

                # Check if merge table exists
                if not check_merge_table_exists(engine, merge_table_name):
                    logger.error("Merge table does not exist for dataset in unassigned workspace",
                                 merge_table_name=merge_table_name,
                                 dataset_name=sink.datasetName)
                    views_failed += 1
                    continue

                # Check if merge table has all required columns
                required_columns = list(original_schema.columns.keys())
                has_columns, missing_columns = check_merge_table_has_required_columns(engine, merge_table_name, required_columns)

                if not has_columns:
                    logger.error("Merge table missing required columns for unassigned workspace",
                                 merge_table_name=merge_table_name,
                                 dataset_name=sink.datasetName,
                                 missing_columns=missing_columns)
                    views_failed += 1
                    continue

                # Create views based on platform type
                try:
                    view_changes = []

                    with engine.begin() as connection:
                        if is_forensic_platform:
                            # For forensic platforms: create both _view_full and _live views

                            # 1. Create full view (all historical records)
                            full_view_name = utils.getPhysWorkspaceFullViewName(workspace.name, dataset_group.name)

                            with log_operation_timing(logger, "create_full_view", view_name=full_view_name):
                                full_was_changed = createOrUpdateView(connection, inspector, utils.dataset, full_view_name, merge_table_name)

                            if full_was_changed:
                                if check_merge_table_exists(engine, full_view_name):
                                    views_updated += 1
                                    view_changes.append(f"Updated full view: {full_view_name}")
                                else:
                                    views_created += 1
                                    view_changes.append(f"Created full view: {full_view_name}")
                            else:
                                view_changes.append(f"Full view already up to date: {full_view_name}")

                            # 2. Create live view (only live records with WHERE clause)
                            live_view_name = utils.getPhysWorkspaceLiveViewName(workspace.name, dataset_group.name)
                            live_where_clause = f"{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = {YellowSchemaConstants.LIVE_RECORD_ID}"

                            with log_operation_timing(logger, "create_live_view", view_name=live_view_name):
                                live_was_changed = createOrUpdateView(connection, inspector, utils.dataset, live_view_name,
                                                                      merge_table_name, live_where_clause)

                            if live_was_changed:
                                if check_merge_table_exists(engine, live_view_name):
                                    views_updated += 1
                                    view_changes.append(f"Updated live view: {live_view_name}")
                                else:
                                    views_created += 1
                                    view_changes.append(f"Created live view: {live_view_name}")
                            else:
                                view_changes.append(f"Live view already up to date: {live_view_name}")

                        else:
                            # For live-only platforms: create only _live view (no filtering needed)
                            live_view_name = utils.getPhysWorkspaceLiveViewName(workspace.name, dataset_group.name)

                            with log_operation_timing(logger, "create_live_view", view_name=live_view_name):
                                live_was_changed = createOrUpdateView(connection, inspector, utils.dataset, live_view_name,
                                                                      merge_table_name, None)

                            if live_was_changed:
                                if check_merge_table_exists(engine, live_view_name):
                                    views_updated += 1
                                    view_changes.append(f"Updated live view: {live_view_name}")
                                else:
                                    views_created += 1
                                    view_changes.append(f"Created live view: {live_view_name}")
                            else:
                                view_changes.append(f"Live view already up to date: {live_view_name}")

                        # Log all view changes for this dataset
                        for change in view_changes:
                            logger.info("View operation completed for unassigned workspace", operation=change)

                except Exception as e:
                    logger.error("Error creating/updating views for dataset in unassigned workspace",
                                 dataset_name=sink.datasetName,
                                 workspace_name=workspace_name,
                                 dsg_name=dsg_name,
                                 error=str(e))
                    views_failed += 1

    # Log final summary
    logger.info(
        "View reconciliation summary",
        views_created=views_created,
        views_updated=views_updated,
        views_failed=views_failed)

    # Return appropriate exit code
    if views_failed > 0:
        logger.warning(
            "Some views could not be created/updated",
            failed_count=views_failed,
            exit_code=1)
        return 1
    else:
        logger.info("All views successfully processed", exit_code=0)
        return 0


def main():
    """Main entry point for the command-line utility"""
    parser = argparse.ArgumentParser(
        description="Reconcile workspace view schemas for YellowDataPlatform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using shared git cache (recommended for production):
  python -m datasurface.platforms.yellow.reconcile_workspace_views \\
    --psp Test_DP \\
    --git-repo-path /cache/git-models \\
    --git-repo-owner billynewport \\
    --git-repo-name yellow_starter \\
    --git-repo-branch main \\
    --git-platform-repo-credential-name git \\
    --use-git-cache \\
    --max-cache-age-minutes 5

  # Direct clone (for testing):
  python -m datasurface.platforms.yellow.reconcile_workspace_views \\
    --psp Test_DP \\
    --git-repo-path /workspace/git-repo \\
    --git-repo-owner billynewport \\
    --git-repo-name yellow_starter \\
    --git-repo-branch main \\
    --git-platform-repo-credential-name git \\
    --no-use-git-cache
        """
    )

    parser.add_argument(
        "--psp",
        required=True,
        help="Name of the YellowPlatformServicesProvider to reconcile views for"
    )

    parser.add_argument(
        "--git-repo-path",
        required=True,
        help="Path to the git repository or cache"
    )

    parser.add_argument(
        "--git-repo-owner",
        required=True,
        help="GitHub repository owner (e.g., billynewport)"
    )

    parser.add_argument(
        "--git-repo-name",
        required=True,
        help="GitHub repository name (e.g., yellow_starter)"
    )

    parser.add_argument(
        "--git-repo-branch",
        required=True,
        help="GitHub repository live branch (e.g., main)"
    )

    parser.add_argument(
        "--git-platform-repo-credential-name",
        required=True,
        help="GitHub credential name for accessing the model repository (e.g., git)"
    )

    parser.add_argument(
        "--use-git-cache",
        action='store_true',
        default=True,
        help="Use shared git cache for better performance (default: True)"
    )

    parser.add_argument(
        "--max-cache-age-minutes",
        type=int,
        default=5,
        help="Maximum cache age in minutes before checking remote (default: 5)"
    )

    parser.add_argument(
        "--credential-store",
        default="env",
        help="Credential store type (default: env for environment variables)"
    )

    args = parser.parse_args()

    try:
        # Create credential store first
        if args.credential_store == "env":
            from datasurface.platforms.yellow.yellow_dp import KubernetesEnvVarsCredentialStore
            cred_store = KubernetesEnvVarsCredentialStore(
                name="env-cred-store",
                locs=set()  # Will be populated by the platform
            )
        else:
            logger.error(f"Error: Unsupported credential store type: {args.credential_store}")
            return 1

        # Load the ecosystem model from git repository
        from datasurface.cmd.platform import getLatestModelAtTimestampedFolder
        from datasurface.md.repo import GitHubRepository
        from datasurface.md.credential import Credential, CredentialType
        import os

        logger.info(f"Loading ecosystem model from git repository: {args.git_repo_owner}/{args.git_repo_name}")

        # Ensure the directory exists
        os.makedirs(args.git_repo_path, exist_ok=True)

        eco, validation_tree = getLatestModelAtTimestampedFolder(
            cred_store,
            GitHubRepository(
                f"{args.git_repo_owner}/{args.git_repo_name}",
                args.git_repo_branch,
                credential=Credential(args.git_platform_repo_credential_name, CredentialType.API_TOKEN)
            ),
            args.git_repo_path,
            doClone=not args.use_git_cache,  # Only do direct clone if not using cache
            useCache=args.use_git_cache,     # Use cache by default
            maxCacheAgeMinutes=args.max_cache_age_minutes
        )

        if validation_tree and validation_tree.hasErrors():
            logger.error("Ecosystem model has errors", validation_errors=validation_tree.getErrorsAsStructuredData())
            return 1

        if eco is None:
            logger.error("Failed to load ecosystem")
            return 1

        # Reconcile workspace view schemas
        logger.info(f"Reconciling workspace view schemas for platform: {args.psp}")
        return reconcile_workspace_view_schemas(eco, args.psp, cred_store)

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
