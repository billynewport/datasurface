"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from unittest.mock import patch, MagicMock
from sqlalchemy.engine import Engine
from sqlalchemy import text

from datasurface.platforms.yellow.reconcile_workspace_views import (
    generate_view_name,
    generate_merge_table_name,
    get_original_dataset_schema,
    check_merge_table_exists,
    check_merge_table_has_required_columns,
    reconcile_workspace_view_schemas
)
from datasurface.md import Ecosystem, DataPlatform
from datasurface.md.schema import DDLTable, DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import Integer, VarChar
from datasurface.md.credential import CredentialStore
from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform


class TestReconcileWorkspaceViews:
    """Test cases for the reconcile workspace views utility."""

    def test_generate_view_name(self) -> None:
        """Test view name generation."""
        view_name = generate_view_name("YellowDP", "TestWorkspace", "TestDSG", "TestDataset")
        assert view_name == "yellowdp_testworkspace_testdsg_testdataset_view"
        
        # Test with special characters
        view_name = generate_view_name("Yellow-DP", "Test Workspace", "Test-DSG", "Test Dataset")
        assert view_name == "yellow_dp_test_workspace_test_dsg_test_dataset_view"

    def test_generate_merge_table_name(self) -> None:
        """Test merge table name generation."""
        table_name = generate_merge_table_name("TestStore", "TestDataset")
        assert table_name == "teststore_testdataset_merge"
        
        # Test with special characters
        table_name = generate_merge_table_name("Test Store", "Test Dataset")
        assert table_name == "test_store_test_dataset_merge"

    def test_get_original_dataset_schema(self) -> None:
        """Test getting original dataset schema."""
        # Create a mock ecosystem with a dataset
        eco = MagicMock(spec=Ecosystem)
        store_entry = MagicMock()
        dataset = MagicMock()
        dataset.originalSchema = DDLTable(
            DDLColumn("id", Integer(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("name", VarChar(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
        )
        store_entry.datastore.datasets = {"test_dataset": dataset}
        eco.cache_getDatastoreOrThrow.return_value = store_entry
        
        schema = get_original_dataset_schema("test_dataset", "test_store", eco)
        assert schema is not None
        assert "id" in schema.columns
        assert "name" in schema.columns

    def test_check_merge_table_exists(self, test_db: Engine) -> None:
        """Test checking if merge table exists."""
        # Test with non-existent table
        assert not check_merge_table_exists(test_db, "non_existent_table")
        
        # Create a table and test
        with test_db.begin() as conn:
            conn.execute(text("CREATE TABLE test_merge_table (id INTEGER, name VARCHAR(50))"))
        
        assert check_merge_table_exists(test_db, "test_merge_table")
        
        # Clean up
        with test_db.begin() as conn:
            conn.execute(text("DROP TABLE test_merge_table"))

    def test_check_merge_table_has_required_columns(self, test_db: Engine) -> None:
        """Test checking if merge table has required columns."""
        # Create a test table
        with test_db.begin() as conn:
            conn.execute(text("CREATE TABLE test_merge_table (id INTEGER, name VARCHAR(50), extra_col VARCHAR(100))"))
        
        # Test with all required columns present
        has_columns, missing = check_merge_table_has_required_columns(test_db, "test_merge_table", ["id", "name"])
        assert has_columns is True
        assert missing == []
        
        # Test with missing columns
        has_columns, missing = check_merge_table_has_required_columns(test_db, "test_merge_table", ["id", "name", "missing_col"])
        assert has_columns is False
        assert "missing_col" in missing
        
        # Clean up
        with test_db.begin() as conn:
            conn.execute(text("DROP TABLE test_merge_table"))

    @patch('datasurface.platforms.yellow.reconcile_workspace_views.createOrUpdateView')
    @patch('datasurface.platforms.yellow.reconcile_workspace_views.createEngine')
    @patch('datasurface.platforms.yellow.reconcile_workspace_views.PlatformPipelineGraph')
    def test_reconcile_workspace_view_schemas_success(self, mock_graph_class, mock_create_engine, mock_create_view) -> None:
        """Test successful reconciliation of workspace view schemas."""
        # Mock the createOrUpdateView function
        mock_create_view.return_value = True
        
        # Create a mock ecosystem
        eco = MagicMock(spec=Ecosystem)
        eco.dataPlatforms = {}
        
        # Create a mock YellowDataPlatform - use a real instance instead of MagicMock
        yellow_dp = MagicMock()
        yellow_dp.__class__ = YellowDataPlatform  # Make isinstance() work
        yellow_dp.name = "test_yellow_dp"
        yellow_dp.postgresCredential = MagicMock()
        yellow_dp.mergeStore = MagicMock()
        yellow_dp.mergeStore.hostPortPair.hostName = "localhost"
        yellow_dp.mergeStore.hostPortPair.port = 5432
        yellow_dp.mergeStore.databaseName = "test_db"
        eco.dataPlatforms["test_yellow_dp"] = yellow_dp
        
        # Mock the getDataPlatformOrThrow method to return our mock
        eco.getDataPlatformOrThrow.return_value = yellow_dp
        
        # Create a mock credential store
        cred_store = MagicMock(spec=CredentialStore)
        cred_store.getAsUserPassword.return_value = ("user", "password")
        
        # Mock the database engine
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        
        # Mock the platform pipeline graph
        mock_graph = MagicMock()
        mock_graph.nodes = {}  # No export nodes
        mock_graph.workspaces = {}
        mock_graph_class.return_value = mock_graph
        
        # Test the function
        result = reconcile_workspace_view_schemas(eco, "test_yellow_dp", cred_store)
        assert result == 0  # Success

    def test_reconcile_workspace_view_schemas_platform_not_found(self) -> None:
        """Test reconciliation when platform is not found."""
        eco = MagicMock(spec=Ecosystem)
        eco.dataPlatforms = {}
        cred_store = MagicMock(spec=CredentialStore)
        
        result = reconcile_workspace_view_schemas(eco, "non_existent_platform", cred_store)
        assert result == 1  # Error

    def test_reconcile_workspace_view_schemas_wrong_platform_type(self) -> None:
        """Test reconciliation when platform is not a YellowDataPlatform."""
        eco = MagicMock(spec=Ecosystem)
        wrong_dp = MagicMock(spec=DataPlatform)
        wrong_dp.name = "wrong_platform"
        eco.dataPlatforms = {"wrong_platform": wrong_dp}
        cred_store = MagicMock(spec=CredentialStore)
        
        result = reconcile_workspace_view_schemas(eco, "wrong_platform", cred_store)
        assert result == 1  # Error 