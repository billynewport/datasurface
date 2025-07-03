# type: ignore[attr-defined, unknown-member, unknown-argument, unknown-variable, unknown-parameter]
"""
Test module for the createOrUpdateTable function.
Tests table creation, column addition, and column type changes.
"""

import pytest
from unittest.mock import patch
from sqlalchemy import Table, MetaData, Column, Integer, String, Boolean, inspect
from sqlalchemy.engine import Engine

from datasurface.platforms.kubpgstarter.jobs import createOrUpdateTable


class TestCreateOrUpdateTable:
    """Test cases for the createOrUpdateTable function."""

    def test_create_new_table(self, test_db: Engine) -> None:
        """Test creating a new table when it doesn't exist."""
        metadata = MetaData()

        # Create a test table definition
        test_table = Table(
            'test_new_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('active', Boolean)
        )

        # Ensure table doesn't exist
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        if inspector.has_table('test_new_table'):  # type: ignore[attr-defined]
            test_table.drop(test_db)

        # Call the function
        createOrUpdateTable(test_db, test_table)

        # Verify table was created
        assert inspector.has_table('test_new_table')  # type: ignore[attr-defined]

        # Verify table structure
        created_table = Table('test_new_table', MetaData(), autoload_with=test_db)
        column_names = [col.name for col in created_table.columns]  # type: ignore[attr-defined]
        assert 'id' in column_names
        assert 'name' in column_names
        assert 'active' in column_names

        # Clean up
        test_table.drop(test_db)

    def test_add_new_columns_to_existing_table(self, test_db: Engine) -> None:
        """Test adding new columns to an existing table."""
        metadata = MetaData()

        # Create initial table
        initial_table = Table(
            'test_add_columns',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

        # Clean up if exists
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        if inspector.has_table('test_add_columns'):  # type: ignore[attr-defined]
            initial_table.drop(test_db)

        initial_table.create(test_db)

        # Create updated table definition with new columns
        updated_table = Table(
            'test_add_columns',
            MetaData(),
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('email', String(100)),  # New column
            Column('created_at', String(50))  # New column
        )

        # Call the function
        with patch('builtins.print') as mock_print:
            createOrUpdateTable(test_db, updated_table)

        # Verify new columns were added
        current_table = Table('test_add_columns', MetaData(), autoload_with=test_db)
        column_names = [col.name for col in current_table.columns]  # type: ignore[attr-defined]
        assert 'email' in column_names
        assert 'created_at' in column_names

        # Verify print was called with correct message
        mock_print.assert_called()
        print_args = str(mock_print.call_args)
        assert 'Added columns' in print_args
        assert 'email' in print_args
        assert 'created_at' in print_args

        # Clean up
        initial_table.drop(test_db)

    def test_alter_column_types(self, test_db: Engine) -> None:
        """Test altering column types in an existing table."""
        metadata = MetaData()

        # Create initial table with smaller string column
        initial_table = Table(
            'test_alter_types',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('description', String(50))
        )

        # Clean up if exists
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        if inspector.has_table('test_alter_types'):  # type: ignore[attr-defined]
            initial_table.drop(test_db)

        initial_table.create(test_db)

        # Create updated table definition with larger string column
        updated_table = Table(
            'test_alter_types',
            MetaData(),
            Column('id', Integer, primary_key=True),
            Column('description', String(200))  # Widened column
        )

        # Call the function
        with patch('builtins.print') as mock_print:
            createOrUpdateTable(test_db, updated_table)

        # Verify column type was altered
        # Note: Exact type verification depends on database dialect
        current_table = Table('test_alter_types', MetaData(), autoload_with=test_db)
        # Column type verification depends on database dialect, so we just check it exists
        assert 'description' in [col.name for col in current_table.columns]  # type: ignore[attr-defined]

        # Verify print was called with correct message
        mock_print.assert_called()
        print_args = str(mock_print.call_args)
        assert 'Altered columns' in print_args
        assert 'description' in print_args

        # Clean up
        initial_table.drop(test_db)

    def test_add_columns_and_alter_types_together(self, test_db: Engine) -> None:
        """Test adding new columns and altering existing column types in one operation."""
        metadata = MetaData()

        # Create initial table
        initial_table = Table(
            'test_combined_changes',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(30))
        )

        # Clean up if exists
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        if inspector.has_table('test_combined_changes'):  # type: ignore[attr-defined]
            initial_table.drop(test_db)

        initial_table.create(test_db)

        # Create updated table definition
        updated_table = Table(
            'test_combined_changes',
            MetaData(),
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),  # Widened existing column
            Column('email', String(150)),  # New column
            Column('status', String(20))   # New column
        )

        # Call the function
        with patch('builtins.print') as mock_print:
            createOrUpdateTable(test_db, updated_table)

        # Verify changes were made
        current_table = Table('test_combined_changes', MetaData(), autoload_with=test_db)
        column_names = [col.name for col in current_table.columns]  # type: ignore[attr-defined]

        # Check new columns exist
        assert 'email' in column_names
        assert 'status' in column_names

        # Verify both print statements were called
        assert mock_print.call_count == 2
        all_print_args = ' '.join([str(call) for call in mock_print.call_args_list])
        assert 'Added columns' in all_print_args
        assert 'Altered columns' in all_print_args
        assert 'email' in all_print_args
        assert 'status' in all_print_args
        assert 'name' in all_print_args

        # Clean up
        initial_table.drop(test_db)

    def test_no_changes_needed(self, test_db: Engine) -> None:
        """Test when table already has the correct schema."""
        metadata = MetaData()

        # Create table
        test_table = Table(
            'test_no_changes',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('active', Boolean)
        )

        # Clean up if exists
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        if inspector.has_table('test_no_changes'):  # type: ignore[attr-defined]
            test_table.drop(test_db)

        test_table.create(test_db)

        # Create identical table definition
        identical_table = Table(
            'test_no_changes',
            MetaData(),
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('active', Boolean)
        )

        # Call the function
        with patch('builtins.print') as mock_print:
            createOrUpdateTable(test_db, identical_table)

        # Verify no print statements were called (no changes made)
        mock_print.assert_not_called()

        # Clean up
        test_table.drop(test_db)

    def test_table_with_nullable_constraints(self, test_db: Engine) -> None:
        """Test adding columns with NOT NULL constraints."""
        metadata = MetaData()

        # Create initial table
        initial_table = Table(
            'test_nullable',
            metadata,
            Column('id', Integer, primary_key=True)
        )

        # Clean up if exists
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        if inspector.has_table('test_nullable'):  # type: ignore[attr-defined]
            initial_table.drop(test_db)

        initial_table.create(test_db)

        # Create updated table with nullable and non-nullable columns
        updated_table = Table(
            'test_nullable',
            MetaData(),
            Column('id', Integer, primary_key=True),
            Column('optional_field', String(50), nullable=True),
            Column('required_field', String(50), nullable=False)
        )

        # Call the function
        with patch('builtins.print'):
            createOrUpdateTable(test_db, updated_table)

        # Verify columns were added
        current_table = Table('test_nullable', MetaData(), autoload_with=test_db)
        column_names = [col.name for col in current_table.columns]  # type: ignore[attr-defined]
        assert 'optional_field' in column_names
        assert 'required_field' in column_names

        # Clean up
        initial_table.drop(test_db)

    def test_error_handling_invalid_table_name(self, test_db: Engine) -> None:
        """Test error handling with invalid table operations."""
        metadata = MetaData()

        # Create a table with an invalid name for testing
        # This test depends on the specific database's naming rules
        invalid_table = Table(
            '',  # Empty table name should cause issues
            metadata,
            Column('id', Integer, primary_key=True)
        )

        # This should raise an exception
        with pytest.raises(Exception):
            createOrUpdateTable(test_db, invalid_table)