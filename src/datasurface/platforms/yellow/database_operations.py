"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Any, Union
import datetime
from sqlalchemy.engine import Connection
from sqlalchemy import text
from datasurface.md import SchemaProjector, DataContainerNamingMapper
from datasurface.platforms.yellow.logging_utils import get_contextual_logger

# Forward declaration to avoid circular imports
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants
from datasurface.md import DataContainer

logger = get_contextual_logger(__name__)


class DatabaseOperations(ABC):
    """Abstract base class for database-specific operations.

    This class provides an abstraction layer for database-specific SQL operations
    used in the Yellow platform. Different database implementations (PostgreSQL,
    SQL Server, etc.) can provide their own implementations of these operations.
    """

    def __init__(self, schema_projector: SchemaProjector, data_container: DataContainer) -> None:
        self.schema_projector: SchemaProjector = schema_projector
        self.data_container: DataContainer = data_container
        self.nm: DataContainerNamingMapper = data_container.getNamingAdapter()

    # Hash and String Functions
    @abstractmethod
    def get_hash_expression(self, columns: List[str]) -> str:
        """Generate database-specific hash expression for the given columns.

        Args:
            columns: List of column names to hash

        Returns:
            SQL expression that computes MD5 hash of concatenated columns
        """
        pass

    @abstractmethod
    def get_hash_column_width(self) -> int:
        """Get the width of the hash column.

        Returns:
            Width of the hash column
        """
        pass

    @abstractmethod
    def get_coalesce_expression(self, column: str, default_value: str = "''") -> str:
        """Generate database-specific COALESCE expression.

        Args:
            column: Column name to coalesce
            default_value: Default value if column is NULL

        Returns:
            SQL expression for COALESCE
        """
        pass

    @abstractmethod
    def get_string_concat_operator(self) -> str:
        """Get the string concatenation operator for this database.

        Returns:
            String concatenation operator ('||' for PostgreSQL, '+' for SQL Server)
        """
        pass

    # Date/Time Functions
    @abstractmethod
    def get_current_timestamp_expression(self) -> str:
        """Get the current timestamp expression.

        Returns:
            SQL expression for current timestamp (NOW() for PostgreSQL, GETDATE() for SQL Server)
        """
        pass

    # Pagination and Limiting
    @abstractmethod
    def get_limit_offset_clause(self, limit: int, offset: int) -> str:
        """Generate database-specific LIMIT/OFFSET clause.

        Args:
            limit: Maximum number of rows to return
            offset: Number of rows to skip

        Returns:
            SQL clause for limiting results
        """
        pass

    # Merge/Upsert Operations
    @abstractmethod
    def supports_merge_statement(self) -> bool:
        """Check if database supports MERGE statements.

        Returns:
            True if database supports MERGE, False otherwise
        """
        pass

    @abstractmethod
    def get_upsert_sql(
        self,
        target_table: str,
        source_table: str,
        all_columns: List[str],
        key_hash_column: str,
        all_hash_column: str,
        batch_id_column: str,
        batch_id: int
    ) -> str:
        """Generate database-specific upsert SQL.

        Args:
            target_table: Target merge table name
            source_table: Source staging table name
            all_columns: List of data column names
            key_hash_column: Name of key hash column
            all_hash_column: Name of all columns hash column
            batch_id_column: Name of batch ID column
            batch_id: Current batch ID

        Returns:
            SQL statement for upsert operation
        """
        pass

    @abstractmethod
    def get_delete_missing_records_sql(
        self,
        target_table: str,
        source_table: str,
        key_hash_column: str,
        batch_id_column: str,
        batch_id: int
    ) -> str:
        """Generate SQL to delete records that don't exist in source.

        Args:
            target_table: Target merge table name
            source_table: Source staging table name
            key_hash_column: Name of key hash column
            batch_id_column: Name of batch ID column
            batch_id: Current batch ID

        Returns:
            SQL statement for deleting missing records
        """
        pass

    @abstractmethod
    def get_delete_marked_records_sql(
        self,
        target_table: str,
        staging_table: str,
        key_hash_column: str,
        batch_id_column: str,
        iud_column: str,
        batch_id: int
    ) -> str:
        """Generate SQL for deleting records marked as deleted in staging.

        Args:
            target_table: Target table name
            staging_table: Staging table name
            key_hash_column: Key hash column name
            batch_id_column: Batch ID column name
            iud_column: IUD column name
            batch_id: Current batch ID

        Returns:
            SQL DELETE statement
        """
        pass

    # Forensic Operations (SCD2)
    @abstractmethod
    def get_forensic_merge_sql(
        self,
        target_table: str,
        source_table: str,
        all_columns: List[str],
        key_hash_column: str,
        all_hash_column: str,
        batch_id_column: str,
        batch_in_column: str,
        batch_out_column: str,
        live_record_id: int,
        batch_id: int
    ) -> List[str]:
        """Generate database-specific SQL statements for forensic (SCD2) merge.

        Args:
            target_table: Target merge table name
            source_table: Source staging table name
            all_columns: List of data column names
            key_hash_column: Name of key hash column
            all_hash_column: Name of all columns hash column
            batch_id_column: Name of batch ID column
            batch_in_column: Name of batch in column
            batch_out_column: Name of batch out column
            live_record_id: ID representing live records
            batch_id: Current batch ID

        Returns:
            List of SQL statements to execute for forensic merge
        """
        pass

    # Schema Introspection
    def check_table_exists(self, connection: Connection, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            connection: Database connection
            table_name: Name of table to check

        Returns:
            True if table exists, False otherwise
        """
        from sqlalchemy import inspect
        inspector = inspect(connection.engine)
        return inspector.has_table(table_name)

    @abstractmethod
    def check_constraint_exists(
        self,
        connection: Connection,
        table_name: str,
        constraint_type: str,
        column_name: Optional[str] = None
    ) -> bool:
        """Check if a constraint exists on a table.

        Args:
            connection: Database connection
            table_name: Name of table
            constraint_type: Type of constraint (UNIQUE, PRIMARY KEY, etc.)
            column_name: Optional column name for column-specific constraints

        Returns:
            True if constraint exists, False otherwise
        """
        pass

    @abstractmethod
    def check_index_exists(self, connection: Connection, table_name: str, index_name: str) -> bool:
        """Check if an index exists.

        Args:
            connection: Database connection
            table_name: Name of table
            index_name: Name of index

        Returns:
            True if index exists, False otherwise
        """
        pass

    @abstractmethod
    def create_index_sql(self, index_name: str, table_name: str, columns: List[str], unique: bool = False) -> str:
        """Generate SQL to create an index.

        Args:
            index_name: Name of index to create
            table_name: Name of table
            columns: List of column names for the index
            unique: Whether to create a unique index

        Returns:
            SQL statement to create index
        """
        pass

    @abstractmethod
    def create_unique_constraint_sql(self, table_name: str, column_name: str, constraint_name: str) -> str:
        """Generate SQL to create a unique constraint.

        Args:
            table_name: Name of table
            column_name: Name of column
            constraint_name: Name of constraint

        Returns:
            SQL statement to create unique constraint
        """
        pass

    # JSON Operations (for Debezium CDC)
    @abstractmethod
    def get_json_extract_expression(self, json_column: str, key: str) -> str:
        """Generate database-specific JSON extraction expression.

        Args:
            json_column: Name of JSON column
            key: JSON key to extract

        Returns:
            SQL expression to extract JSON value as text
        """
        pass

    # Window Functions
    @abstractmethod
    def get_row_number_expression(self, partition_by: List[str], order_by: List[str]) -> str:
        """Generate database-specific ROW_NUMBER() window function.

        Args:
            partition_by: List of columns to partition by
            order_by: List of columns to order by (with optional ASC/DESC)

        Returns:
            SQL expression for ROW_NUMBER() window function
        """
        pass

    # Aggregate Functions with FILTER
    @abstractmethod
    def get_count_filter_expression(self, filter_condition: str) -> str:
        """Generate database-specific COUNT with FILTER expression.

        Args:
            filter_condition: WHERE condition for the filter

        Returns:
            SQL expression for COUNT with FILTER
        """
        pass

    # CTE and Subquery Support
    def supports_cte(self) -> bool:
        """Check if database supports Common Table Expressions (WITH clauses).

        Returns:
            True if database supports CTEs (most modern databases do)
        """
        return True

    # Remote Merge Operations
    def get_remote_seed_batch_sql(
        self,
        source_table: str,
        all_columns: List[str],
        all_hash_column: str,
        key_hash_column: str,
        batch_in_column: str,
        batch_out_column: str,
        iud_column: str,
        current_remote_batch_id: int
    ) -> str:
        """Generate SQL for remote seed batch (pull all live records).

        Args:
            source_table: Remote forensic merge table
            all_columns: List of data column names
            all_hash_column: Name of all columns hash column
            key_hash_column: Name of key hash column
            batch_in_column: Name of batch in column
            batch_out_column: Name of batch out column
            iud_column: Name of IUD column
            current_remote_batch_id: Current remote batch ID

        Returns:
            SQL statement for seed batch ingestion
        """
        quoted_columns = self.get_quoted_columns(all_columns)
        return f"""
        SELECT {', '.join(quoted_columns)},
            {self.nm.fmtCol(all_hash_column)},
            {self.nm.fmtCol(key_hash_column)},
            'I' as {self.nm.fmtCol(iud_column)}
        FROM {source_table}
        WHERE {self.nm.fmtCol(batch_in_column)} <= {current_remote_batch_id}
        AND {self.nm.fmtCol(batch_out_column)} > {current_remote_batch_id}
        """

    def get_remote_incremental_batch_sql(
        self,
        source_table: str,
        all_columns: List[str],
        all_hash_column: str,
        key_hash_column: str,
        batch_in_column: str,
        batch_out_column: str,
        iud_column: str,
        last_remote_batch_id: int,
        current_remote_batch_id: int
    ) -> str:
        """Generate SQL for remote incremental batch (pull only changes).

        Args:
            source_table: Remote forensic merge table
            all_columns: List of data column names
            all_hash_column: Name of all columns hash column
            key_hash_column: Name of key hash column
            batch_in_column: Name of batch in column
            batch_out_column: Name of batch out column
            iud_column: Name of IUD column
            last_remote_batch_id: Last processed remote batch ID
            current_remote_batch_id: Current remote batch ID

        Returns:
            SQL statement for incremental batch ingestion
        """
        quoted_columns = self.get_quoted_columns(all_columns)
        formatted_all_hash = self.nm.fmtCol(all_hash_column)
        formatted_key_hash = self.nm.fmtCol(key_hash_column)
        formatted_batch_in = self.nm.fmtCol(batch_in_column)
        formatted_batch_out = self.nm.fmtCol(batch_out_column)
        formatted_iud = self.nm.fmtCol(iud_column)

        return f"""
        WITH changed_keys AS (
            -- Find all keys that had any changes between batches
            SELECT DISTINCT {formatted_key_hash}
            FROM {source_table}
            WHERE ({formatted_batch_in} > {last_remote_batch_id}
                   AND {formatted_batch_in} <= {current_remote_batch_id})
               OR ({formatted_batch_out} >= {last_remote_batch_id}
                   AND {formatted_batch_out} <= {current_remote_batch_id})
        ),
        current_state AS (
            -- Get the current state of changed keys
            SELECT {', '.join(quoted_columns)},
                {formatted_all_hash},
                {formatted_key_hash},
                {formatted_batch_in},
                {formatted_batch_out}
            FROM {source_table} s
            WHERE s.{formatted_key_hash} IN (SELECT {formatted_key_hash} FROM changed_keys)
              AND s.{formatted_batch_in} <= {current_remote_batch_id}
              AND s.{formatted_batch_out} > {current_remote_batch_id}
        ),
        previous_state AS (
            -- Get the previous state of changed keys
            SELECT {formatted_key_hash},
                {formatted_all_hash} as prev_all_hash
            FROM {source_table} s
            WHERE s.{formatted_key_hash} IN (SELECT {formatted_key_hash} FROM changed_keys)
              AND s.{formatted_batch_in} <= {last_remote_batch_id}
              AND s.{formatted_batch_out} >= {last_remote_batch_id}
        )
        SELECT {', '.join(quoted_columns)},
            cs.{formatted_all_hash},
            cs.{formatted_key_hash},
            CASE
                WHEN ps.{formatted_key_hash} IS NULL THEN 'I'  -- New record
                WHEN cs.{formatted_all_hash} != ps.prev_all_hash THEN 'U'  -- Updated record
                ELSE 'U'  -- Default to update for safety
            END as {formatted_iud}
        FROM current_state cs
        LEFT JOIN previous_state ps ON cs.{formatted_key_hash} = ps.{formatted_key_hash}

        UNION ALL

        -- Handle deletions: keys that existed at last batch but not at current batch
        SELECT {', '.join([f'ps.' + self.get_quoted_columns([col])[0] for col in all_columns])},
            ps.{formatted_all_hash},
            ps.{formatted_key_hash},
            'D' as {formatted_iud}
        FROM (
            SELECT {', '.join(quoted_columns)},
                {formatted_all_hash},
                {formatted_key_hash}
            FROM {source_table} s
            WHERE s.{formatted_key_hash} IN (SELECT {formatted_key_hash} FROM changed_keys)
              AND s.{formatted_batch_in} <= {last_remote_batch_id}
              AND s.{formatted_batch_out} >= {last_remote_batch_id}
        ) ps
        WHERE ps.{formatted_key_hash} NOT IN (SELECT {formatted_key_hash} FROM current_state)
        """

    # Remote Forensic Operations
    @abstractmethod
    def get_remote_forensic_update_closed_sql(
        self, merge_table: str, staging_table: str, sp: Any, batch_id: int
    ) -> str:
        """Generate SQL for updating records that were closed remotely."""
        pass

    # CDC Operations
    def get_cdc_conflation_sql(
        self,
        cdc_table: str,
        staging_table: str,
        all_columns: List[str],
        pk_columns: List[str],
        tx_column: str,
        iud_column: str,
        all_hash_column: str,
        key_hash_column: str,
        batch_id_column: str,
        batch_id: int,
        last_tx: int,
        high_tx: int,
        map_insert: str,
        map_update: str,
        map_delete: str
    ) -> str:
        """Generate SQL for CDC conflation and staging insertion.

        Args:
            cdc_table: CDC source table name
            staging_table: Target staging table name
            all_columns: List of data column names
            pk_columns: List of primary key column names
            tx_column: Transaction ID column name
            iud_column: IUD operation column name
            all_hash_column: Name of all columns hash column
            key_hash_column: Name of key hash column
            batch_id_column: Name of batch ID column
            batch_id: Current batch ID
            last_tx: Last processed transaction ID
            high_tx: Highest transaction ID to process
            map_insert: Source value that maps to 'I'
            map_update: Source value that maps to 'U'
            map_delete: Source value that maps to 'D'

        Returns:
            SQL statement for CDC conflation and insertion
        """
        quoted_columns = self.get_quoted_columns(all_columns)
        all_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_hash_expr = self.build_hash_expression_for_columns(pk_columns)
        row_number_expr = self.get_row_number_expression([key_hash_column], [f'"{tx_column}" DESC'])

        return f"""
        WITH cdc_filtered AS (
            SELECT {', '.join(quoted_columns)},
                   {all_hash_expr} AS {all_hash_column},
                   {key_hash_expr} AS {key_hash_column},
                   "{tx_column}" AS tx,
                   "{iud_column}" AS src_iud
            FROM {cdc_table}
            WHERE "{tx_column}" > {last_tx} AND "{tx_column}" <= {high_tx}
        ), labeled AS (
            SELECT *,
                   CASE
                       WHEN src_iud = '{map_insert}' THEN 'I'
                       WHEN src_iud = '{map_update}' THEN 'U'
                       WHEN src_iud = '{map_delete}' THEN 'D'
                       ELSE 'U'
                   END AS mapped_iud
            FROM cdc_filtered
        ), ranked AS (
            SELECT *, {row_number_expr} AS rn
            FROM labeled
        )
        INSERT INTO {staging_table} (
            {', '.join(quoted_columns)},
            {batch_id_column}, {all_hash_column}, {key_hash_column}, ds_surf_iud
        )
        SELECT {', '.join([f'r."{col}"' for col in all_columns])},
               {batch_id}, r.{all_hash_column}, r.{key_hash_column}, r.mapped_iud
        FROM ranked r
        WHERE r.rn = 1
        """

    def get_debezium_cdc_conflation_sql(
        self,
        cdc_table: str,
        staging_table: str,
        all_columns: List[str],
        pk_columns: List[str],
        tx_column: str,
        tx_order_column: Optional[str],
        op_column: str,
        after_column: str,
        before_column: str,
        all_hash_column: str,
        key_hash_column: str,
        batch_id_column: str,
        batch_id: int,
        last_tx: int,
        high_tx: int,
        map_insert: str,
        map_update: str,
        map_delete: str
    ) -> str:
        """Generate SQL for Debezium CDC conflation and staging insertion.

        Args:
            cdc_table: Debezium CDC source table name
            staging_table: Target staging table name
            all_columns: List of data column names
            pk_columns: List of primary key column names
            tx_column: Transaction ID column name
            tx_order_column: Optional transaction order column name
            op_column: Operation column name
            after_column: After state JSON column name
            before_column: Before state JSON column name
            all_hash_column: Name of all columns hash column
            key_hash_column: Name of key hash column
            batch_id_column: Name of batch ID column
            batch_id: Current batch ID
            last_tx: Last processed transaction ID
            high_tx: Highest transaction ID to process
            map_insert: Source value that maps to 'I'
            map_update: Source value that maps to 'U'
            map_delete: Source value that maps to 'D'

        Returns:
            SQL statement for Debezium CDC conflation and insertion
        """
        quoted_columns = self.get_quoted_columns(all_columns)

        # Build hash expressions using JSON values
        key_parts = [
            f'COALESCE({self.get_json_extract_expression(after_column, pk)}, '
            f'{self.get_json_extract_expression(before_column, pk)}, \'\')'
            for pk in pk_columns
        ]
        key_concat = f" {self.get_string_concat_operator()} ".join(key_parts) if key_parts else "''"

        all_parts = [f'COALESCE({self.get_json_extract_expression(after_column, col)}, \'\')' for col in all_columns]
        all_concat = f" {self.get_string_concat_operator()} ".join(all_parts) if all_parts else "''"

        # ORDER for conflation: tx desc, then tx_order desc if provided
        order_by_parts = [f'"{tx_column}" DESC']
        if tx_order_column:
            order_by_parts.append(f'"{tx_order_column}" DESC')

        row_number_expr = self.get_row_number_expression([key_hash_column], order_by_parts)

        return f"""
        WITH cdc_filtered AS (
            SELECT "{after_column}" AS after, "{before_column}" AS before, "{op_column}" AS src_op,
                   "{tx_column}" AS tx{', "' + tx_order_column + '" AS tx_order' if tx_order_column else ''},
                   {self.get_hash_expression([all_concat])} AS {all_hash_column},
                   {self.get_hash_expression([key_concat])} AS {key_hash_column}
            FROM {cdc_table}
            WHERE "{tx_column}" > {last_tx} AND "{tx_column}" <= {high_tx}
        ), labeled AS (
            SELECT *,
                   CASE
                       WHEN src_op = '{map_insert}' THEN 'I'
                       WHEN src_op = '{map_update}' THEN 'U'
                       WHEN src_op = '{map_delete}' THEN 'D'
                       ELSE 'U'
                   END AS mapped_iud
            FROM cdc_filtered
        ), ranked AS (
            SELECT *, {row_number_expr} AS rn
            FROM labeled
        )
        INSERT INTO {staging_table} (
            {', '.join(quoted_columns)},
            {batch_id_column}, {all_hash_column}, {key_hash_column}, ds_surf_iud
        )
        SELECT
            {', '.join([f"CASE WHEN r.mapped_iud IN ('I','U') THEN {self.get_json_extract_expression('r.after', col)} ELSE NULL END" for col in all_columns])},
            {batch_id}, r.{all_hash_column}, r.{key_hash_column}, r.mapped_iud
        FROM ranked r
        WHERE r.rn = 1
        """

    # Utility Methods
    def build_hash_expression_for_columns(self, columns: List[str]) -> str:
        """Build a hash expression by concatenating columns with COALESCE.

        Args:
            columns: List of column names

        Returns:
            SQL expression for hashing concatenated columns
        """
        concat_op = self.get_string_concat_operator()
        coalesced_columns = [self.get_coalesce_expression(self.cast_to_text(f'"{col}"')) for col in columns]
        concatenated = f" {concat_op} ".join(coalesced_columns)
        return self.get_hash_expression([concatenated])

    @abstractmethod
    def cast_to_text(self, column_expr: str) -> str:
        """Cast a column expression to text/string type.

        Args:
            column_expr: Column expression to cast

        Returns:
            Database-specific text casting expression
        """
        pass

    @abstractmethod
    def get_datetime_type(self) -> str:
        """Get the appropriate datetime type for this database.

        Returns:
            Database-specific datetime type name
        """
        pass

    def get_datetime_sqlalchemy_type(self) -> Any:
        """Get the appropriate SQLAlchemy datetime type for this database.

        Returns:
            SQLAlchemy datetime type instance
        """
        from sqlalchemy.types import TIMESTAMP, DATETIME
        datetime_type_name = self.get_datetime_type()
        if datetime_type_name == "DATETIME2":
            return DATETIME()
        else:  # TIMESTAMP
            return TIMESTAMP()

    @abstractmethod
    def get_update_from_syntax(self, target_table: str, source_table: str,
                               target_alias: str = "m", source_alias: str = "s") -> str:
        """Get the database-specific UPDATE FROM syntax.

        Args:
            target_table: Target table name
            source_table: Source table name
            target_alias: Alias for target table
            source_alias: Alias for source table

        Returns:
            Database-specific UPDATE FROM clause
        """
        pass

    @abstractmethod
    def get_drop_table_sql(self, table_name: str) -> str:
        """Get the database-specific DROP TABLE syntax.

        Args:
            table_name: Name of table to drop

        Returns:
            Database-specific DROP TABLE statement
        """
        pass

    def get_quoted_columns(self, columns: List[str]) -> List[str]:
        """Get quoted column names for SQL.

        Args:
            columns: List of column names

        Returns:
            List of quoted column names
        """
        return [f'"{col}"' for col in columns]

    # Watermark Operations
    def format_watermark_value(self, watermark_value: Union[int, datetime.datetime, str]) -> str:
        """Format watermark value for SQL based on its type.

        Args:
            watermark_value: Watermark value (integer, datetime, or string)

        Returns:
            Properly formatted SQL value
        """
        if isinstance(watermark_value, int):
            return str(watermark_value)
        elif isinstance(watermark_value, datetime.datetime):
            # Format datetime for SQL - subclasses can override for database-specific formatting
            return f"'{watermark_value.isoformat()}'"
        elif isinstance(watermark_value, str):
            # Assume it's already properly formatted (e.g., "'2024-01-01'" or "12345")
            return watermark_value
        else:
            # Fallback to string representation
            return str(watermark_value)

    def get_watermark_records_lt(
        self,
        source_table: str,
        all_columns: List[str],
        pk_columns: List[str],
        watermark_column: str,
        watermark_value: Union[int, datetime.datetime, str]
    ) -> str:
        """Build a query to select records where watermark column < provided value with computed hashes.

        Args:
            source_table: Source table name
            all_columns: List of all column names
            pk_columns: List of primary key column names
            watermark_column: Name of the watermark column
            watermark_value: Value to compare watermark against (integer, datetime, or formatted string)

        Returns:
            SQL query string to select records with hashes
        """
        quoted_columns = self.get_quoted_columns(all_columns)
        formatted_value = self.format_watermark_value(watermark_value)

        # Build hash expressions
        all_columns_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_columns_hash_expr = self.build_hash_expression_for_columns(pk_columns)

        return f"""
        SELECT {', '.join(quoted_columns)},
            {all_columns_hash_expr} as {YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
        FROM {source_table}
        WHERE "{watermark_column}" < {formatted_value}
        """

    def get_watermark_records_range(
        self,
        source_table: str,
        all_columns: List[str],
        pk_columns: List[str],
        watermark_column: str,
        low_watermark_value: Union[int, datetime.datetime, str],
        high_watermark_value: Union[int, datetime.datetime, str]
    ) -> str:
        """Build a query to select records where watermark column >= low_value and < high_value with computed hashes.

        Args:
            source_table: Source table name
            all_columns: List of all column names
            pk_columns: List of primary key column names
            watermark_column: Name of the watermark column
            low_watermark_value: Lower bound value (integer, datetime, or formatted string)
            high_watermark_value: Upper bound value (integer, datetime, or formatted string)

        Returns:
            SQL query string to select records with hashes
        """
        quoted_columns = self.get_quoted_columns(all_columns)
        formatted_low = self.format_watermark_value(low_watermark_value)
        formatted_high = self.format_watermark_value(high_watermark_value)

        # Build hash expressions
        all_columns_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_columns_hash_expr = self.build_hash_expression_for_columns(pk_columns)

        return f"""
        SELECT {', '.join(quoted_columns)},
            {all_columns_hash_expr} as {YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
        FROM {source_table}
        WHERE "{watermark_column}" >= {formatted_low} AND "{watermark_column}" < {formatted_high}
        """

    def get_max_watermark_value(self, source_table: str, watermark_column: str) -> str:
        """Build a query to get the maximum value from a watermark column.

        Args:
            source_table: Source table name
            watermark_column: Name of the watermark column

        Returns:
            SQL query string to get the maximum watermark value
        """
        return f"""
        SELECT MAX("{watermark_column}")
        FROM {source_table}
        """

    # High-level operations that use the primitives above
    def build_source_query_with_hashes(
        self,
        source_table: str,
        all_columns: List[str],
        pk_columns: List[str]
    ) -> str:
        """Build a query to select data from source with computed hashes."""
        quoted_columns = self.get_quoted_columns(all_columns)

        # Build hash expressions
        all_columns_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_columns_hash_expr = self.build_hash_expression_for_columns(pk_columns)

        return f"""
        SELECT {', '.join(quoted_columns)},
            {all_columns_hash_expr} as {YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
        FROM {source_table}
        """

    def execute_batch_insert(
        self,
        connection: Connection,
        target_table: str,
        all_columns: List[str],
        batch_id: int,
        batch_values: List[List[Any]]
    ) -> int:
        """Execute a batch insert operation."""
        quoted_insert_columns = self.get_quoted_columns(all_columns) + [
            f'"{YellowSchemaConstants.BATCH_ID_COLUMN_NAME}"',
            f'"{YellowSchemaConstants.ALL_HASH_COLUMN_NAME}"',
            f'"{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}"'
        ]

        placeholders = ", ".join([f":{i}" for i in range(len(quoted_insert_columns))])
        insert_sql = f"INSERT INTO {target_table} ({', '.join(quoted_insert_columns)}) VALUES ({placeholders})"

        # Prepare parameters for batch execution
        all_params = [{str(i): val for i, val in enumerate(values)} for values in batch_values]
        connection.execute(text(insert_sql), all_params)
        return len(batch_values)

    @abstractmethod
    def check_unique_constraint_exists(self, connection: Connection, table_name: str, column_name: str) -> bool:
        """Check if a unique constraint exists on the specified column.

        Args:
            connection: Database connection
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            True if constraint exists, False otherwise
        """
        pass

    @abstractmethod
    def create_unique_constraint(self, connection: Connection, table_name: str, column_name: str) -> str:
        """Create a unique constraint on the specified column.

        Args:
            connection: Database connection
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            Name of the created constraint
        """
        pass
