"""
type: ignore[attr-defined, unknown-member, unknown-argument, unknown-variable, unknown-parameter]
Test module for the view reconciliation functions.
Tests view creation, updating, and change detection.
"""

from unittest.mock import patch
from sqlalchemy import Table, MetaData, Column, Integer, String, inspect, text
from sqlalchemy.engine import Engine

from datasurface.md.sqlalchemyutils import createOrUpdateView, reconcileViewSchemas, datasetToSQLAlchemyView
from datasurface.md import Dataset, DDLTable, DDLColumn
from datasurface.md.schema import NullableStatus, PrimaryKeyStatus
from datasurface.md.types import Integer as DSInteger, VarChar
from typing import cast
from datasurface.md import Datastore


class TestDatasetToSQLAlchemyView:
    """Test cases for the datasetToSQLAlchemyView function."""

    def test_generate_view_sql(self) -> None:
        """Test generating CREATE OR REPLACE VIEW SQL from a dataset."""
        # Create a test dataset
        columns = [
            DDLColumn("id", DSInteger(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("name", VarChar(100), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK),
            DDLColumn("description", VarChar(255), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
        ]

        table = DDLTable(*columns)
        dataset = Dataset("test_dataset", table)

        # Generate view SQL
        viewSql = datasetToSQLAlchemyView(dataset, "test_view", "underlying_table", MetaData())

        # Verify the SQL
        expectedSql = "CREATE OR REPLACE VIEW test_view AS SELECT id, name, description FROM underlying_table"
        assert viewSql == expectedSql

    def test_generate_view_sql_with_single_column(self) -> None:
        """Test generating view SQL with a single column."""
        columns = [
            DDLColumn("id", DSInteger(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)
        ]

        table = DDLTable(*columns)
        dataset = Dataset("single_column_dataset", table)

        viewSql = datasetToSQLAlchemyView(dataset, "single_view", "single_table", MetaData())

        expectedSql = "CREATE OR REPLACE VIEW single_view AS SELECT id FROM single_table"
        assert viewSql == expectedSql


class TestCreateOrUpdateView:
    """Test cases for the createOrUpdateView function."""

    def test_create_new_view(self, test_db: Engine) -> None:
        """Test creating a new view when it doesn't exist."""
        # Create underlying table first
        metadata = MetaData()
        underlying_table = Table(
            'test_underlying_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('description', String(100))
        )

        # Clean up if exists: drop view first, then table
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        with test_db.begin() as conn:
            conn.execute(text("DROP VIEW IF EXISTS test_new_view"))
        if inspector.has_table('test_underlying_table'):  # type: ignore[attr-defined]
            underlying_table.drop(test_db)

        # Create the underlying table
        underlying_table.create(test_db)

        # Insert some test data
        with test_db.begin() as conn:
            conn.execute(text("INSERT INTO test_underlying_table (id, name, description) VALUES (1, 'Test1', 'Desc1')"))
            conn.execute(text("INSERT INTO test_underlying_table (id, name, description) VALUES (2, 'Test2', 'Desc2')"))

        # Create dataset
        columns = [
            DDLColumn("id", DSInteger(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("name", VarChar(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK),
            DDLColumn("description", VarChar(100), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
        ]
        table = DDLTable(*columns)
        dataset = Dataset("test_dataset", table)

        # Create the view
        with patch('datasurface.md.sqlalchemyutils.logger.info') as mock_info:
            with test_db.begin() as connection:
                wasChanged = createOrUpdateView(connection, inspector, dataset, "test_new_view", "test_underlying_table")

        # Verify view was created
        assert wasChanged is True
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        assert inspector.has_table('test_new_view')  # type: ignore[attr-defined]

        # Verify view returns correct data
        with test_db.connect() as conn:
            result = conn.execute(text("SELECT * FROM test_new_view ORDER BY id"))
            rows = result.fetchall()
            assert len(rows) == 2
            assert rows[0][0] == 1  # id
            assert rows[0][1] == 'Test1'  # name
            assert rows[0][2] == 'Desc1'  # description

        # Verify log message
        mock_info.assert_any_call("Created view %s with current schema", "test_new_view")

        # Clean up
        with test_db.begin() as conn:
            conn.execute(text("DROP VIEW test_new_view"))
        underlying_table.drop(test_db)

    def test_update_existing_view(self, test_db: Engine) -> None:
        """Test updating an existing view when schema changes."""
        # Create underlying table
        metadata = MetaData()
        underlying_table = Table(
            'test_update_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('description', String(100)),
            Column('new_column', String(50))  # Extra column in underlying table
        )

        # Clean up if exists: drop view first, then table
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        with test_db.begin() as conn:
            conn.execute(text("DROP VIEW IF EXISTS test_update_view"))
            if inspector.has_table('test_update_table'):  # type: ignore[attr-defined]
                underlying_table.drop(conn)

            # Create the underlying table
            underlying_table.create(conn)

            # Create initial dataset (only id and name)
            initial_columns = [
                DDLColumn("id", DSInteger(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                DDLColumn("name", VarChar(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
            ]
            initial_table = DDLTable(*initial_columns)
            initial_dataset = Dataset("initial_dataset", initial_table)

            # Create initial view
            wasChanged = createOrUpdateView(conn, inspector, initial_dataset, "test_update_view", "test_update_table")
            assert wasChanged is True

            # Verify initial view structure
            result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_update_view' ORDER BY ordinal_position"))
            columns = [row[0] for row in result.fetchall()]
            assert columns == ['id', 'name']

            # Create updated dataset (adds description)
            updated_columns = [
                DDLColumn("id", DSInteger(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                DDLColumn("name", VarChar(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK),
                DDLColumn("description", VarChar(100), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
            ]
            updated_table = DDLTable(*updated_columns)
            updated_dataset = Dataset("updated_dataset", updated_table)

            # Update the view
            with patch('datasurface.md.sqlalchemyutils.logger.info') as mock_info:
                # Create fresh inspector to see the view that was just created
                fresh_inspector = inspect(conn)  # type: ignore[attr-defined]
                wasChanged = createOrUpdateView(conn, fresh_inspector, updated_dataset, "test_update_view", "test_update_table")

            # Verify view was updated
            assert wasChanged is True

            # Verify updated view structure
            result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_update_view' ORDER BY ordinal_position"))
            columns = [row[0] for row in result.fetchall()]
            assert columns == ['id', 'name', 'description']

            # Verify log message
            mock_info.assert_any_call("Updated view %s to match current schema", "test_update_view")

            # Clean up
            conn.execute(text("DROP VIEW test_update_view"))
            underlying_table.drop(conn)

    def test_no_changes_needed(self, test_db: Engine) -> None:
        """Test that no changes are made when view already matches schema."""
        # Create underlying table
        metadata = MetaData()
        underlying_table = Table(
            'test_no_change_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

        # Clean up if exists (drop view first, then table)
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        if inspector.has_table('test_no_change_view'):  # type: ignore[attr-defined]
            with test_db.begin() as conn:
                conn.execute(text("DROP VIEW test_no_change_view"))
        if inspector.has_table('test_no_change_table'):  # type: ignore[attr-defined]
            underlying_table.drop(test_db)

        # Create the underlying table
        underlying_table.create(test_db)

        # Create dataset
        columns = [
            DDLColumn("id", DSInteger(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("name", VarChar(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
        ]
        table = DDLTable(*columns)
        dataset = Dataset("test_dataset", table)

        # Create the view first time
        with test_db.begin() as connection:
            inspector1 = inspect(connection)  # type: ignore[attr-defined]
            wasChanged1 = createOrUpdateView(connection, inspector1, dataset, "test_no_change_view", "test_no_change_table")
        assert wasChanged1 is True

        # Try to create/update the view again with same schema
        with patch('datasurface.md.sqlalchemyutils.logger.info') as mock_info:
            with test_db.begin() as connection:
                inspector2 = inspect(connection)  # type: ignore[attr-defined]
                wasChanged2 = createOrUpdateView(connection, inspector2, dataset, "test_no_change_view", "test_no_change_table")

        # Verify no changes were needed and appropriate log was emitted
        assert wasChanged2 is False
        mock_info.assert_any_call("View %s already matches current schema", "test_no_change_view")

        # Clean up (drop view first, then table)
        with test_db.begin() as conn:
            conn.execute(text("DROP VIEW test_no_change_view"))
        underlying_table.drop(test_db)


class TestReconcileViewSchemas:
    """Test cases for the reconcileViewSchemas function."""

    def test_reconcile_multiple_views(self, test_db: Engine) -> None:
        """Test reconciling multiple views in a datastore."""
        # Create underlying tables
        metadata = MetaData()
        table1 = Table(
            'test_table1',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )
        table2 = Table(
            'test_table2',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('description', String(100))
        )

        # Clean up if exists
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        for table_name in ['test_table1', 'test_table2']:
            if inspector.has_table(table_name):  # type: ignore[attr-defined]
                Table(table_name, metadata).drop(test_db)
        for view_name in ['view1', 'view2']:
            if inspector.has_table(view_name):  # type: ignore[attr-defined]
                with test_db.begin() as conn:
                    conn.execute(text(f"DROP VIEW {view_name}"))

        # Create the underlying tables
        table1.create(test_db)
        table2.create(test_db)

        # Create datasets
        columns1 = [
            DDLColumn("id", DSInteger(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("name", VarChar(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
        ]
        table1_ddl = DDLTable(*columns1)
        dataset1 = Dataset("dataset1", table1_ddl)

        columns2 = [
            DDLColumn("id", DSInteger(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("description", VarChar(100), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
        ]
        table2_ddl = DDLTable(*columns2)
        dataset2 = Dataset("dataset2", table2_ddl)

        # Create mock datastore
        class MockDatastore:
            def __init__(self, datasets):
                self.datasets = datasets

        store = MockDatastore({
            "dataset1": dataset1,
            "dataset2": dataset2
        })

        # Define mapper functions
        def view_name_mapper(dataset: Dataset) -> str:
            return f"view{dataset.name[-1]}"  # view1, view2

        def underlying_table_mapper(dataset: Dataset) -> str:
            return f"test_table{dataset.name[-1]}"  # test_table1, test_table2

        # Reconcile views
        changedViews = reconcileViewSchemas(test_db, cast(Datastore, store), view_name_mapper, underlying_table_mapper)

        # Verify results
        assert len(changedViews) == 2
        assert changedViews["view1"] is True  # Should be created
        assert changedViews["view2"] is True  # Should be created

        # Verify views exist
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        assert inspector.has_table('view1')  # type: ignore[attr-defined]
        assert inspector.has_table('view2')  # type: ignore[attr-defined]

        # Clean up
        with test_db.begin() as conn:
            conn.execute(text("DROP VIEW view1"))
            conn.execute(text("DROP VIEW view2"))
        table1.drop(test_db)
        table2.drop(test_db)

    def test_reconcile_with_no_changes(self, test_db: Engine) -> None:
        """Test reconciling when no views need changes."""
        # Create underlying table
        metadata = MetaData()
        underlying_table = Table(
            'test_reconcile_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

        # Clean up if exists: drop view first, then table
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        with test_db.begin() as conn:
            conn.execute(text("DROP VIEW IF EXISTS test_reconcile_view"))
        if inspector.has_table('test_reconcile_table'):  # type: ignore[attr-defined]
            underlying_table.drop(test_db)

        # Create the underlying table
        underlying_table.create(test_db)

        # Create dataset
        columns = [
            DDLColumn("id", DSInteger(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("name", VarChar(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
        ]
        table = DDLTable(*columns)
        dataset = Dataset("test_dataset", table)

        # Create mock datastore
        class MockDatastore:
            def __init__(self, datasets):
                self.datasets = datasets

        store = MockDatastore({"test_dataset": dataset})

        # Define mapper functions
        def view_name_mapper(dataset: Dataset) -> str:
            return "test_reconcile_view"

        def underlying_table_mapper(dataset: Dataset) -> str:
            return "test_reconcile_table"

        # Create view first time
        changedViews1 = reconcileViewSchemas(test_db, cast(Datastore, store), view_name_mapper, underlying_table_mapper)
        assert changedViews1["test_reconcile_view"] is True

        # Reconcile again - should not change
        changedViews2 = reconcileViewSchemas(test_db, cast(Datastore, store), view_name_mapper, underlying_table_mapper)
        assert changedViews2["test_reconcile_view"] is False

        # Clean up (drop view first, then table)
        with test_db.begin() as conn:
            conn.execute(text("DROP VIEW test_reconcile_view"))
        underlying_table.drop(test_db)
