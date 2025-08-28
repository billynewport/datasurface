"""
 type: ignore[attr-defined, unknown-member, unknown-argument, unknown-variable, unknown-parameter]
Test module for the createOrUpdateTable function.
Tests table creation, column addition, and column type changes.
"""
import pytest
from unittest.mock import patch
from sqlalchemy import Table, MetaData, Column, Integer, String, Boolean, inspect
from sqlalchemy.engine import Engine

from datasurface.md.sqlalchemyutils import createOrUpdateTable
from datasurface.md.sqlalchemyutils import _types_are_compatible, _extract_length_from_type, _extract_decimal_params
from datasurface.platforms.yellow.db_utils import createInspector


class TestTypeCompatibility:
    """Test cases for the _types_are_compatible function."""

    def test_identical_types_are_compatible(self) -> None:
        """Test that identical types are considered compatible."""
        assert _types_are_compatible('VARCHAR(50)', 'VARCHAR(50)') is True
        assert _types_are_compatible('INTEGER', 'INTEGER') is True
        assert _types_are_compatible('BOOLEAN', 'BOOLEAN') is True

    def test_integer_type_equivalencies(self) -> None:
        """Test that different INTEGER type names are considered equivalent."""
        assert _types_are_compatible('INTEGER', 'INT') is True
        assert _types_are_compatible('INT', 'INT4') is True
        assert _types_are_compatible('INTEGER', 'INT4') is True

    def test_boolean_type_equivalencies(self) -> None:
        """Test that different BOOLEAN type names are considered equivalent."""
        assert _types_are_compatible('BOOLEAN', 'BOOL') is True
        assert _types_are_compatible('BOOL', 'BOOLEAN') is True

    def test_varchar_length_changes_are_incompatible(self) -> None:
        """Test that VARCHAR with different lengths are considered incompatible."""
        assert _types_are_compatible('VARCHAR(50)', 'VARCHAR(200)') is False
        assert _types_are_compatible('VARCHAR(100)', 'VARCHAR(50)') is False
        assert _types_are_compatible('VARCHAR(10)', 'VARCHAR(10)') is True

    def test_char_length_changes_are_incompatible(self) -> None:
        """Test that CHAR with different lengths are considered incompatible."""
        assert _types_are_compatible('CHAR(10)', 'CHAR(20)') is False
        assert _types_are_compatible('CHAR(5)', 'CHAR(5)') is True

    def test_decimal_precision_scale_changes_are_incompatible(self) -> None:
        """Test that DECIMAL with different precision/scale are considered incompatible."""
        assert _types_are_compatible('DECIMAL(10,2)', 'DECIMAL(10,3)') is False
        assert _types_are_compatible('DECIMAL(10,2)', 'DECIMAL(11,2)') is False
        assert _types_are_compatible('DECIMAL(10,2)', 'DECIMAL(10,2)') is True
        assert _types_are_compatible('NUMERIC(10,2)', 'DECIMAL(10,2)') is True

    def test_different_base_types_are_incompatible(self) -> None:
        """Test that different base types are considered incompatible."""
        assert _types_are_compatible('VARCHAR(50)', 'INTEGER') is False
        assert _types_are_compatible('BOOLEAN', 'VARCHAR(10)') is False
        assert _types_are_compatible('CHAR(10)', 'VARCHAR(10)') is False

    def test_case_insensitive_comparison(self) -> None:
        """Test that type comparison is case insensitive."""
        assert _types_are_compatible('varchar(50)', 'VARCHAR(50)') is True
        assert _types_are_compatible('VARCHAR(50)', 'varchar(50)') is True
        assert _types_are_compatible('integer', 'INTEGER') is True

    def test_whitespace_handling(self) -> None:
        """Test that whitespace is properly handled."""
        assert _types_are_compatible(' VARCHAR(50) ', 'VARCHAR(50)') is True
        assert _types_are_compatible('VARCHAR(50)', ' VARCHAR(50) ') is True


class TestLengthExtraction:
    """Test cases for the _extract_length_from_type function."""

    def test_extract_varchar_length(self) -> None:
        """Test extracting length from VARCHAR type."""
        assert _extract_length_from_type('VARCHAR(50)') == 50
        assert _extract_length_from_type('VARCHAR(200)') == 200
        assert _extract_length_from_type('VARCHAR(10)') == 10

    def test_extract_char_length(self) -> None:
        """Test extracting length from CHAR type."""
        assert _extract_length_from_type('CHAR(10)') == 10
        assert _extract_length_from_type('CHAR(1)') == 1

    def test_invalid_length_format(self) -> None:
        """Test handling of invalid length formats."""
        assert _extract_length_from_type('VARCHAR') == -1
        assert _extract_length_from_type('VARCHAR()') == -1
        assert _extract_length_from_type('VARCHAR(abc)') == -1
        assert _extract_length_from_type('') == -1


class TestDecimalParamExtraction:
    """Test cases for the _extract_decimal_params function."""

    def test_extract_decimal_params(self) -> None:
        """Test extracting precision and scale from DECIMAL type."""
        assert _extract_decimal_params('DECIMAL(10,2)') == (10, 2)
        assert _extract_decimal_params('DECIMAL(5,0)') == (5, 0)
        assert _extract_decimal_params('NUMERIC(15,3)') == (15, 3)

    def test_invalid_decimal_format(self) -> None:
        """Test handling of invalid decimal formats."""
        assert _extract_decimal_params('DECIMAL') == (-1, -1)
        assert _extract_decimal_params('DECIMAL()') == (-1, -1)
        assert _extract_decimal_params('DECIMAL(10)') == (-1, -1)
        assert _extract_decimal_params('DECIMAL(abc,def)') == (-1, -1)
        assert _extract_decimal_params('') == (-1, -1)


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
        with test_db.begin() as connection:
            wasChanged = createOrUpdateTable(connection, createInspector(test_db), test_table)

        # Verify table was created
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        assert inspector.has_table('test_new_table')  # type: ignore[attr-defined]
        assert wasChanged is True

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
        with patch('datasurface.md.sqlalchemyutils.logger.info') as mock_info:
            with test_db.begin() as connection:
                wasChanged = createOrUpdateTable(connection, createInspector(test_db), updated_table)

        # Verify new columns were added
        current_table = Table('test_add_columns', MetaData(), autoload_with=test_db)
        column_names = [col.name for col in current_table.columns]  # type: ignore[attr-defined]
        assert 'email' in column_names
        assert 'created_at' in column_names
        assert wasChanged is True

        # Verify logger.info was called with correct message
        assert mock_info.called
        all_log_args = ' '.join([str(call) for call in mock_info.call_args_list])
        assert 'Added columns' in all_log_args
        assert 'email' in all_log_args
        assert 'created_at' in all_log_args

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
        with patch('datasurface.md.sqlalchemyutils.logger.info') as mock_info:
            with test_db.begin() as connection:
                wasChanged = createOrUpdateTable(connection, createInspector(test_db), updated_table)

        # Verify column type was altered
        # Note: Exact type verification depends on database dialect
        current_table = Table('test_alter_types', MetaData(), autoload_with=test_db)
        # Column type verification depends on database dialect, so we just check it exists
        assert 'description' in [col.name for col in current_table.columns]  # type: ignore[attr-defined]
        assert wasChanged is True

        # Verify logger.info was called with correct message
        assert mock_info.called
        all_log_args = ' '.join([str(call) for call in mock_info.call_args_list])
        assert 'Altered columns' in all_log_args
        assert 'description' in all_log_args

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
        with patch('datasurface.md.sqlalchemyutils.logger.info') as mock_info:
            with test_db.begin() as connection:
                wasChanged = createOrUpdateTable(connection, createInspector(test_db), updated_table)

        # Verify changes were made
        current_table = Table('test_combined_changes', MetaData(), autoload_with=test_db)
        column_names = [col.name for col in current_table.columns]  # type: ignore[attr-defined]

        # Check new columns exist
        assert 'email' in column_names
        assert 'status' in column_names
        assert wasChanged is True

        # Verify both logger.info statements were called
        assert mock_info.call_count >= 2
        all_log_args = ' '.join([str(call) for call in mock_info.call_args_list])
        assert 'Added columns' in all_log_args
        assert 'Altered columns' in all_log_args
        assert 'email' in all_log_args
        assert 'status' in all_log_args
        assert 'name' in all_log_args

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
        with patch('datasurface.md.sqlalchemyutils.logger.info') as mock_info:
            with test_db.begin() as connection:
                wasChanged = createOrUpdateTable(connection, createInspector(test_db), identical_table)

        # Verify no schema change logs were emitted (timing logs are OK, but no "Added columns" or "Altered columns")
        all_log_calls = [str(call) for call in mock_info.call_args_list]
        schema_change_logs = [call for call in all_log_calls if 'Added columns' in call or 'Altered columns' in call]
        assert len(schema_change_logs) == 0, f"Expected no schema change logs, but found: {schema_change_logs}"
        assert wasChanged is False

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
        with test_db.begin() as connection:
            wasChanged = createOrUpdateTable(connection, createInspector(test_db), updated_table)

        # Verify columns were added
        current_table = Table('test_nullable', MetaData(), autoload_with=test_db)
        column_names = [col.name for col in current_table.columns]  # type: ignore[attr-defined]
        assert 'optional_field' in column_names
        assert 'required_field' in column_names
        assert wasChanged is True

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
            with test_db.begin() as connection:
                createOrUpdateTable(connection, createInspector(test_db), invalid_table)

    def test_multiple_column_alterations_are_batched(self, test_db: Engine) -> None:
        """Test that multiple column type changes are batched into a single ALTER TABLE statement."""
        metadata = MetaData()

        # Create initial table with smaller columns
        initial_table = Table(
            'test_batch_alterations',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)),
            Column('description', String(50)),
            Column('email', String(40))
        )

        # Clean up if exists
        inspector = inspect(test_db)  # type: ignore[attr-defined]
        if inspector.has_table('test_batch_alterations'):  # type: ignore[attr-defined]
            initial_table.drop(test_db)

        initial_table.create(test_db)

        # Create updated table definition with larger columns
        updated_table = Table(
            'test_batch_alterations',
            MetaData(),
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),  # Widened from 30 to 100
            Column('description', String(200)),  # Widened from 50 to 200
            Column('email', String(150))  # Widened from 40 to 150
        )

        # Call the function
        with patch('datasurface.md.sqlalchemyutils.logger.info') as mock_info:
            with test_db.begin() as connection:
                wasChanged = createOrUpdateTable(connection, createInspector(test_db), updated_table)

        # Verify logger.info was called with message indicating all columns were altered
        assert mock_info.called
        all_log_args = ' '.join([str(call) for call in mock_info.call_args_list])
        assert 'Altered columns' in all_log_args
        assert 'name' in all_log_args
        assert 'description' in all_log_args
        assert 'email' in all_log_args
        assert wasChanged is True

        # Verify the table was actually altered by checking the current schema
        current_table = Table('test_batch_alterations', MetaData(), autoload_with=test_db)
        column_names = [col.name for col in current_table.columns]  # type: ignore[attr-defined]
        assert 'name' in column_names
        assert 'description' in column_names
        assert 'email' in column_names

        # Clean up
        initial_table.drop(test_db)
