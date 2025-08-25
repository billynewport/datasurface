"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import os
import time
import functools
from abc import ABC, abstractmethod
from typing import List, Optional, Any, Callable, cast, Union
import datetime
from sqlalchemy.engine import Connection
from sqlalchemy import text
from datasurface.md import DataContainer, PostgresDatabase, SQLServerDatabase
from datasurface.platforms.yellow.logging_utils import get_contextual_logger

# Forward declaration to avoid circular imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from datasurface.platforms.yellow.yellow_dp import YellowSchemaProjector

logger = get_contextual_logger(__name__)


class DatabaseOperations(ABC):
    """Abstract base class for database-specific operations.

    This class provides an abstraction layer for database-specific SQL operations
    used in the Yellow platform. Different database implementations (PostgreSQL,
    SQL Server, etc.) can provide their own implementations of these operations.
    """

    def __init__(self, schema_projector: 'YellowSchemaProjector') -> None:
        self.schema_projector = schema_projector

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
            {all_hash_column},
            {key_hash_column},
            'I' as {iud_column}
        FROM {source_table}
        WHERE {batch_in_column} <= {current_remote_batch_id}
        AND {batch_out_column} > {current_remote_batch_id}
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
        return f"""
        WITH changed_keys AS (
            -- Find all keys that had any changes between batches
            SELECT DISTINCT {key_hash_column}
            FROM {source_table}
            WHERE ({batch_in_column} > {last_remote_batch_id}
                   AND {batch_in_column} <= {current_remote_batch_id})
               OR ({batch_out_column} >= {last_remote_batch_id}
                   AND {batch_out_column} <= {current_remote_batch_id})
        ),
        current_state AS (
            -- Get the current state of changed keys
            SELECT {', '.join(quoted_columns)},
                {all_hash_column},
                {key_hash_column},
                {batch_in_column},
                {batch_out_column}
            FROM {source_table} s
            WHERE s.{key_hash_column} IN (SELECT {key_hash_column} FROM changed_keys)
              AND s.{batch_in_column} <= {current_remote_batch_id}
              AND s.{batch_out_column} > {current_remote_batch_id}
        ),
        previous_state AS (
            -- Get the previous state of changed keys
            SELECT {key_hash_column},
                {all_hash_column} as prev_all_hash
            FROM {source_table} s
            WHERE s.{key_hash_column} IN (SELECT {key_hash_column} FROM changed_keys)
              AND s.{batch_in_column} <= {last_remote_batch_id}
              AND s.{batch_out_column} >= {last_remote_batch_id}
        )
        SELECT {', '.join(quoted_columns)},
            cs.{all_hash_column},
            cs.{key_hash_column},
            CASE
                WHEN ps.{key_hash_column} IS NULL THEN 'I'  -- New record
                WHEN cs.{all_hash_column} != ps.prev_all_hash THEN 'U'  -- Updated record
                ELSE 'U'  -- Default to update for safety
            END as {iud_column}
        FROM current_state cs
        LEFT JOIN previous_state ps ON cs.{key_hash_column} = ps.{key_hash_column}

        UNION ALL

        -- Handle deletions: keys that existed at last batch but not at current batch
        SELECT {', '.join([f'ps."{col}"' for col in all_columns])},
            ps.{all_hash_column},
            ps.{key_hash_column},
            'D' as {iud_column}
        FROM (
            SELECT {', '.join(quoted_columns)},
                {all_hash_column},
                {key_hash_column}
            FROM {source_table} s
            WHERE s.{key_hash_column} IN (SELECT {key_hash_column} FROM changed_keys)
              AND s.{batch_in_column} <= {last_remote_batch_id}
              AND s.{batch_out_column} >= {last_remote_batch_id}
        ) ps
        WHERE ps.{key_hash_column} NOT IN (SELECT {key_hash_column} FROM current_state)
        """

    # Remote Forensic Operations
    def get_remote_forensic_update_closed_sql(
        self, merge_table: str, staging_table: str, sp: Any, batch_id: int
    ) -> str:
        """Generate SQL for updating records that were closed remotely.

        Args:
            merge_table: Target merge table name
            staging_table: Source staging table name
            sp: Schema projector with column names
            batch_id: Current batch ID

        Returns:
            Database-specific UPDATE FROM SQL
        """
        if isinstance(self, PostgresDatabaseOperations):
            return f"""
            UPDATE {merge_table} m
            SET {sp.BATCH_OUT_COLUMN_NAME} = s."remote_{sp.BATCH_OUT_COLUMN_NAME}"
            FROM {staging_table} s
            WHERE m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                AND m.{sp.BATCH_IN_COLUMN_NAME} = s."remote_{sp.BATCH_IN_COLUMN_NAME}"
                AND m.{sp.BATCH_OUT_COLUMN_NAME} = {sp.LIVE_RECORD_ID}
                AND s."remote_{sp.BATCH_OUT_COLUMN_NAME}" != {sp.LIVE_RECORD_ID}
                AND s.{sp.BATCH_ID_COLUMN_NAME} = {batch_id}
            """
        else:  # SQL Server
            return f"""
            UPDATE m
            SET {sp.BATCH_OUT_COLUMN_NAME} = s."remote_{sp.BATCH_OUT_COLUMN_NAME}"
            FROM {merge_table} m
            INNER JOIN {staging_table} s ON m.{sp.KEY_HASH_COLUMN_NAME} = s.{sp.KEY_HASH_COLUMN_NAME}
                AND m.{sp.BATCH_IN_COLUMN_NAME} = s."remote_{sp.BATCH_IN_COLUMN_NAME}"
            WHERE m.{sp.BATCH_OUT_COLUMN_NAME} = {sp.LIVE_RECORD_ID}
                AND s."remote_{sp.BATCH_OUT_COLUMN_NAME}" != {sp.LIVE_RECORD_ID}
                AND s.{sp.BATCH_ID_COLUMN_NAME} = {batch_id}
            """

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
            {all_columns_hash_expr} as {self.schema_projector.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {self.schema_projector.KEY_HASH_COLUMN_NAME}
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
            {all_columns_hash_expr} as {self.schema_projector.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {self.schema_projector.KEY_HASH_COLUMN_NAME}
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
            {all_columns_hash_expr} as {self.schema_projector.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {self.schema_projector.KEY_HASH_COLUMN_NAME}
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
            f'"{self.schema_projector.BATCH_ID_COLUMN_NAME}"',
            f'"{self.schema_projector.ALL_HASH_COLUMN_NAME}"',
            f'"{self.schema_projector.KEY_HASH_COLUMN_NAME}"'
        ]

        placeholders = ", ".join([f":{i}" for i in range(len(quoted_insert_columns))])
        insert_sql = f"INSERT INTO {target_table} ({', '.join(quoted_insert_columns)}) VALUES ({placeholders})"

        # Prepare parameters for batch execution
        all_params = [{str(i): val for i, val in enumerate(values)} for values in batch_values]
        connection.execute(text(insert_sql), all_params)
        return len(batch_values)


class PostgresDatabaseOperations(DatabaseOperations):
    """PostgreSQL-specific database operations implementation."""

    def get_hash_expression(self, columns: List[str]) -> str:
        if len(columns) == 1:
            return f"MD5({columns[0]})"
        else:
            concat_op = self.get_string_concat_operator()
            concatenated = f" {concat_op} ".join(columns)
            return f"MD5({concatenated})"

    def get_coalesce_expression(self, column: str, default_value: str = "''") -> str:
        return f"COALESCE({column}, {default_value})"

    def get_string_concat_operator(self) -> str:
        return "||"

    def get_current_timestamp_expression(self) -> str:
        return "NOW()"

    def get_limit_offset_clause(self, limit: int, offset: int) -> str:
        return f"LIMIT {limit} OFFSET {offset}"

    def supports_merge_statement(self) -> bool:
        return True  # PostgreSQL 15+ supports MERGE

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
        quoted_columns = self.get_quoted_columns(all_columns)
        column_updates = [f'"{col}" = EXCLUDED."{col}"' for col in all_columns]

        return f"""
        INSERT INTO {target_table} (
            {', '.join(quoted_columns)},
            {batch_id_column},
            {all_hash_column},
            {key_hash_column}
        )
        SELECT {', '.join([f'b."{col}"' for col in all_columns])},
               {batch_id},
               b.{all_hash_column},
               b.{key_hash_column}
        FROM {source_table} b
        WHERE b.{batch_id_column} = {batch_id}
        ON CONFLICT ({key_hash_column})
        DO UPDATE SET
            {', '.join(column_updates)},
            {batch_id_column} = EXCLUDED.{batch_id_column},
            {all_hash_column} = EXCLUDED.{all_hash_column},
            {key_hash_column} = EXCLUDED.{key_hash_column}
        WHERE {target_table}.{all_hash_column} != EXCLUDED.{all_hash_column}
        """

    def get_delete_missing_records_sql(
        self,
        target_table: str,
        source_table: str,
        key_hash_column: str,
        batch_id_column: str,
        batch_id: int
    ) -> str:
        return f"""
        DELETE FROM {target_table} m
        WHERE NOT EXISTS (
            SELECT 1 FROM {source_table} s
            WHERE s.{batch_id_column} = {batch_id}
            AND s.{key_hash_column} = m.{key_hash_column}
        )
        """

    def get_delete_marked_records_sql(
        self,
        target_table: str,
        staging_table: str,
        key_hash_column: str,
        batch_id_column: str,
        iud_column: str,
        batch_id: int
    ) -> str:
        """Generate SQL for deleting records marked as deleted in staging."""
        return f"""
        DELETE FROM {target_table} m
        WHERE EXISTS (
            SELECT 1 FROM {staging_table} s
            WHERE s.{batch_id_column} = {batch_id}
            AND s.{iud_column} = 'D'
            AND s.{key_hash_column} = m.{key_hash_column}
        )
        """

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
        quoted_columns = self.get_quoted_columns(all_columns)

        # Step 1: Close changed records
        close_changed_sql = f"""
        UPDATE {target_table} m
        SET {batch_out_column} = {batch_id - 1}
        FROM {source_table} s
        WHERE m.{key_hash_column} = s.{key_hash_column}
            AND m.{batch_out_column} = {live_record_id}
            AND m.{all_hash_column} != s.{all_hash_column}
            AND s.{batch_id_column} = {batch_id}
        """

        # Step 2: Close deleted records
        close_deleted_sql = f"""
        UPDATE {target_table} m
        SET {batch_out_column} = {batch_id - 1}
        WHERE m.{batch_out_column} = {live_record_id}
        AND NOT EXISTS (
            SELECT 1 FROM {source_table} s
            WHERE s.{key_hash_column} = m.{key_hash_column}
            AND s.{batch_id_column} = {batch_id}
        )
        """

        # Step 3: Insert new records
        insert_new_sql = f"""
        INSERT INTO {target_table} (
            {', '.join(quoted_columns)},
            {all_hash_column},
            {key_hash_column},
            {batch_in_column},
            {batch_out_column}
        )
        SELECT
            {', '.join([f's."{col}"' for col in all_columns])},
            s.{all_hash_column},
            s.{key_hash_column},
            {batch_id},
            {live_record_id}
        FROM {source_table} s
        WHERE s.{batch_id_column} = {batch_id}
        AND NOT EXISTS (
            SELECT 1 FROM {target_table} m
            WHERE m.{key_hash_column} = s.{key_hash_column}
            AND m.{batch_out_column} = {live_record_id}
        )
        """

        # Step 4: Insert new versions for changed records
        insert_changed_sql = f"""
        INSERT INTO {target_table} (
            {', '.join(quoted_columns)},
            {all_hash_column},
            {key_hash_column},
            {batch_in_column},
            {batch_out_column}
        )
        SELECT
            {', '.join([f's."{col}"' for col in all_columns])},
            s.{all_hash_column},
            s.{key_hash_column},
            {batch_id},
            {live_record_id}
        FROM {source_table} s
        WHERE s.{batch_id_column} = {batch_id}
        AND EXISTS (
            SELECT 1 FROM {target_table} m
            WHERE m.{key_hash_column} = s.{key_hash_column}
            AND m.{batch_out_column} = {batch_id - 1}
        )
        AND NOT EXISTS (
            SELECT 1 FROM {target_table} m2
            WHERE m2.{key_hash_column} = s.{key_hash_column}
            AND m2.{batch_in_column} = {batch_id}
        )
        """

        return [close_changed_sql, close_deleted_sql, insert_new_sql, insert_changed_sql]

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
        if column_name:
            check_sql = f"""
            SELECT COUNT(*) FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = '{table_name}'
                AND tc.constraint_type = '{constraint_type}'
                AND kcu.column_name = '{column_name}'
            """
        else:
            check_sql = f"""
            SELECT COUNT(*) FROM information_schema.table_constraints
            WHERE table_name = '{table_name}' AND constraint_type = '{constraint_type}'
            """

        result = connection.execute(text(check_sql))
        return result.fetchone()[0] > 0

    def check_index_exists(self, connection: Connection, table_name: str, index_name: str) -> bool:
        """Check if an index exists.

        Args:
            connection: Database connection
            table_name: Name of table
            index_name: Name of index

        Returns:
            True if index exists, False otherwise
        """
        check_sql = f"""
        SELECT COUNT(*) FROM pg_indexes
        WHERE tablename = '{table_name}' AND indexname = '{index_name}'
        """
        result = connection.execute(text(check_sql))
        return result.fetchone()[0] > 0

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
        unique_keyword = "UNIQUE " if unique else ""
        quoted_columns = self.get_quoted_columns(columns)
        return f"CREATE {unique_keyword}INDEX {index_name} ON {table_name} ({', '.join(quoted_columns)})"

    def create_unique_constraint_sql(self, table_name: str, column_name: str, constraint_name: str) -> str:
        """Generate SQL to create a unique constraint.

        Args:
            table_name: Name of table
            column_name: Name of column
            constraint_name: Name of constraint

        Returns:
            SQL statement to create unique constraint
        """
        return f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({column_name})"

    def get_json_extract_expression(self, json_column: str, key: str) -> str:
        """PostgreSQL JSON extraction using ->> operator."""
        return f"{json_column}->>{key!r}"

    def get_row_number_expression(self, partition_by: List[str], order_by: List[str]) -> str:
        """PostgreSQL ROW_NUMBER() window function."""
        partition_clause = f"PARTITION BY {', '.join(partition_by)}" if partition_by else ""
        order_clause = f"ORDER BY {', '.join(order_by)}" if order_by else ""
        return f"ROW_NUMBER() OVER ({partition_clause} {order_clause})".strip()

    def get_count_filter_expression(self, filter_condition: str) -> str:
        """PostgreSQL COUNT with FILTER clause."""
        return f"COUNT(*) FILTER (WHERE {filter_condition})"

    def cast_to_text(self, column_expr: str) -> str:
        """PostgreSQL text casting using ::text."""
        return f"{column_expr}::text"

    def get_datetime_type(self) -> str:
        """Get the appropriate datetime type for PostgreSQL."""
        return "TIMESTAMP"

    def get_update_from_syntax(self, target_table: str, source_table: str,
                               target_alias: str = "m", source_alias: str = "s") -> str:
        """PostgreSQL UPDATE FROM syntax."""
        return f"UPDATE {target_table} {target_alias}\\nFROM {source_table} {source_alias}"

    def get_drop_table_sql(self, table_name: str) -> str:
        """PostgreSQL DROP TABLE with CASCADE."""
        return f'DROP TABLE IF EXISTS "{table_name}" CASCADE'

    def format_watermark_value(self, watermark_value: Union[int, datetime.datetime, str]) -> str:
        """PostgreSQL-specific watermark value formatting."""
        if isinstance(watermark_value, int):
            return str(watermark_value)
        elif isinstance(watermark_value, datetime.datetime):
            # PostgreSQL prefers ISO format with explicit timestamp casting
            return f"TIMESTAMP '{watermark_value.isoformat()}'"
        elif isinstance(watermark_value, str):
            # Handle string timestamps - check if it looks like a timestamp and format appropriately
            if '-' in watermark_value and ':' in watermark_value:
                # Looks like a timestamp string, format it properly for PostgreSQL
                return f"TIMESTAMP '{watermark_value}'"
            else:
                # Assume it's already properly formatted or is a simple value
                return watermark_value
        else:
            return str(watermark_value)

    def get_watermark_records_lt(
        self,
        source_table: str,
        all_columns: List[str],
        pk_columns: List[str],
        watermark_column: str,
        watermark_value: Union[int, datetime.datetime, str]
    ) -> str:
        """PostgreSQL-specific implementation for watermark < value query."""
        quoted_columns = self.get_quoted_columns(all_columns)
        formatted_value = self.format_watermark_value(watermark_value)

        # Build hash expressions
        all_columns_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_columns_hash_expr = self.build_hash_expression_for_columns(pk_columns)

        return f"""
        SELECT {', '.join(quoted_columns)},
            {all_columns_hash_expr} as {self.schema_projector.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {self.schema_projector.KEY_HASH_COLUMN_NAME}
        FROM {source_table}
        WHERE "{watermark_column}" < {formatted_value}
        ORDER BY "{watermark_column}"
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
        """PostgreSQL-specific implementation for watermark range query."""
        quoted_columns = self.get_quoted_columns(all_columns)
        formatted_low = self.format_watermark_value(low_watermark_value)
        formatted_high = self.format_watermark_value(high_watermark_value)

        # Build hash expressions
        all_columns_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_columns_hash_expr = self.build_hash_expression_for_columns(pk_columns)

        return f"""
        SELECT {', '.join(quoted_columns)},
            {all_columns_hash_expr} as {self.schema_projector.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {self.schema_projector.KEY_HASH_COLUMN_NAME}
        FROM {source_table}
        WHERE "{watermark_column}" >= {formatted_low} AND "{watermark_column}" < {formatted_high}
        ORDER BY "{watermark_column}"
        """

    def get_max_watermark_value(self, source_table: str, watermark_column: str) -> str:
        """PostgreSQL-specific implementation to get the maximum watermark value."""
        return f"""
        SELECT MAX("{watermark_column}")
        FROM {source_table}
        """


class SQLServerDatabaseOperations(DatabaseOperations):
    """SQL Server-specific database operations implementation."""

    def get_hash_expression(self, columns: List[str]) -> str:
        if len(columns) == 1:
            return f"CONVERT(VARCHAR(32), HASHBYTES('MD5', {columns[0]}), 2)"
        else:
            concat_op = self.get_string_concat_operator()
            concatenated = f" {concat_op} ".join(columns)
            return f"CONVERT(VARCHAR(32), HASHBYTES('MD5', {concatenated}), 2)"

    def get_coalesce_expression(self, column: str, default_value: str = "''") -> str:
        return f"COALESCE({column}, {default_value})"

    def get_string_concat_operator(self) -> str:
        return "+"

    def get_current_timestamp_expression(self) -> str:
        return "GETDATE()"

    def get_limit_offset_clause(self, limit: int, offset: int) -> str:
        return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

    def supports_merge_statement(self) -> bool:
        return True  # SQL Server has excellent MERGE support

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
        quoted_columns = self.get_quoted_columns(all_columns)

        # Handle complex subqueries vs simple table names
        if source_table.strip().startswith('('):
            # Complex subquery - use it directly
            source_clause = f"{source_table} AS source_data"
            source_select = f"""
            SELECT {', '.join([f'"{col}"' for col in all_columns])},
                   {all_hash_column},
                   {key_hash_column}
            FROM source_data
            """
        else:
            # Simple table name - add WHERE clause
            source_select = f"""
            SELECT {', '.join([f'"{col}"' for col in all_columns])},
                   {all_hash_column},
                   {key_hash_column}
            FROM {source_table}
            WHERE {batch_id_column} = {batch_id}
            """
            source_clause = f"({source_select}) AS source"

        # SQL Server MERGE statement
        if source_table.strip().startswith('('):
            return f"""
        MERGE {target_table} AS target
        USING {source_clause}
        ON target.{key_hash_column} = source_data.{key_hash_column}
        WHEN MATCHED AND target.{all_hash_column} != source_data.{all_hash_column} THEN
            UPDATE SET
                {', '.join([f'"{col}" = source_data."{col}"' for col in all_columns])},
                {batch_id_column} = {batch_id},
                {all_hash_column} = source_data.{all_hash_column}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(quoted_columns)}, {batch_id_column}, {all_hash_column}, {key_hash_column})
            VALUES ({', '.join([f'source_data."{col}"' for col in all_columns])}, {batch_id}, source_data.{all_hash_column}, source_data.{key_hash_column});
        """
        else:
            return f"""
        MERGE {target_table} AS target
        USING {source_clause}
        ON target.{key_hash_column} = source.{key_hash_column}
        WHEN MATCHED AND target.{all_hash_column} != source.{all_hash_column} THEN
            UPDATE SET
                {', '.join([f'"{col}" = source."{col}"' for col in all_columns])},
                {batch_id_column} = {batch_id},
                {all_hash_column} = source.{all_hash_column}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(quoted_columns)}, {batch_id_column}, {all_hash_column}, {key_hash_column})
            VALUES ({', '.join([f'source."{col}"' for col in all_columns])}, {batch_id}, source.{all_hash_column}, source.{key_hash_column});
        """

    def get_delete_missing_records_sql(
        self,
        target_table: str,
        source_table: str,
        key_hash_column: str,
        batch_id_column: str,
        batch_id: int
    ) -> str:
        # SQL Server requires DELETE alias FROM table alias syntax
        return f"""
        DELETE m FROM {target_table} m
        WHERE m.{key_hash_column} NOT IN (
            SELECT {key_hash_column} FROM {source_table}
            WHERE {batch_id_column} = {batch_id}
        )
        """

    def get_delete_marked_records_sql(
        self,
        target_table: str,
        staging_table: str,
        key_hash_column: str,
        batch_id_column: str,
        iud_column: str,
        batch_id: int
    ) -> str:
        """Generate SQL for deleting records marked as deleted in staging."""
        # SQL Server requires DELETE alias FROM table alias syntax
        return f"""
        DELETE m FROM {target_table} m
        WHERE EXISTS (
            SELECT 1 FROM {staging_table} s
            WHERE s.{batch_id_column} = {batch_id}
            AND s.{iud_column} = 'D'
            AND s.{key_hash_column} = m.{key_hash_column}
        )
        """

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
        quoted_columns = self.get_quoted_columns(all_columns)

        # SQL Server can use a single MERGE statement for forensic operations
        merge_sql = f"""
        MERGE {target_table} AS target
        USING (
            SELECT {', '.join([f'"{col}"' for col in all_columns])},
                   {all_hash_column},
                   {key_hash_column}
            FROM {source_table}
            WHERE {batch_id_column} = {batch_id}
        ) AS source ON target.{key_hash_column} = source.{key_hash_column}
                      AND target.{batch_out_column} = {live_record_id}
        WHEN MATCHED AND target.{all_hash_column} != source.{all_hash_column} THEN
            UPDATE SET {batch_out_column} = {batch_id - 1}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(quoted_columns)}, {all_hash_column}, {key_hash_column}, {batch_in_column}, {batch_out_column})
            VALUES ({', '.join([f'source."{col}"' for col in all_columns])}, source.{all_hash_column}, source.{key_hash_column}, {batch_id}, {live_record_id})
        WHEN NOT MATCHED BY SOURCE AND target.{batch_out_column} = {live_record_id} THEN
            UPDATE SET {batch_out_column} = {batch_id - 1};
        """

        # Insert new versions for changed records
        insert_changed_sql = f"""
        INSERT INTO {target_table} (
            {', '.join(quoted_columns)},
            {all_hash_column},
            {key_hash_column},
            {batch_in_column},
            {batch_out_column}
        )
        SELECT
            {', '.join([f's."{col}"' for col in all_columns])},
            s.{all_hash_column},
            s.{key_hash_column},
            {batch_id},
            {live_record_id}
        FROM {source_table} s
        WHERE s.{batch_id_column} = {batch_id}
        AND EXISTS (
            SELECT 1 FROM {target_table} m
            WHERE m.{key_hash_column} = s.{key_hash_column}
            AND m.{batch_out_column} = {batch_id - 1}
        )
        """

        return [merge_sql, insert_changed_sql]

    def check_constraint_exists(
        self,
        connection: Connection,
        table_name: str,
        constraint_type: str,
        column_name: Optional[str] = None
    ) -> bool:
        if column_name:
            check_sql = f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            WHERE tc.TABLE_NAME = '{table_name}'
                AND tc.CONSTRAINT_TYPE = '{constraint_type}'
                AND kcu.COLUMN_NAME = '{column_name}'
            """
        else:
            check_sql = f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_TYPE = '{constraint_type}'
            """

        result = connection.execute(text(check_sql))
        return result.fetchone()[0] > 0

    def check_index_exists(self, connection: Connection, table_name: str, index_name: str) -> bool:
        check_sql = f"""
        SELECT COUNT(*) FROM sys.indexes i
        JOIN sys.objects o ON i.object_id = o.object_id
        WHERE o.name = '{table_name}' AND i.name = '{index_name}'
        """
        result = connection.execute(text(check_sql))
        return result.fetchone()[0] > 0

    def create_index_sql(self, index_name: str, table_name: str, columns: List[str], unique: bool = False) -> str:
        unique_keyword = "UNIQUE " if unique else ""
        quoted_columns = self.get_quoted_columns(columns)
        return f"CREATE {unique_keyword}INDEX {index_name} ON {table_name} ({', '.join(quoted_columns)})"

    def create_unique_constraint_sql(self, table_name: str, column_name: str, constraint_name: str) -> str:
        return f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({column_name})"

    def get_json_extract_expression(self, json_column: str, key: str) -> str:
        """SQL Server JSON extraction using JSON_VALUE function."""
        return f"JSON_VALUE({json_column}, '$.{key}')"

    def get_row_number_expression(self, partition_by: List[str], order_by: List[str]) -> str:
        """SQL Server ROW_NUMBER() window function."""
        partition_clause = f"PARTITION BY {', '.join(partition_by)}" if partition_by else ""
        order_clause = f"ORDER BY {', '.join(order_by)}" if order_by else ""
        return f"ROW_NUMBER() OVER ({partition_clause} {order_clause})".strip()

    def get_count_filter_expression(self, filter_condition: str) -> str:
        """SQL Server COUNT with CASE expression (no FILTER clause)."""
        return f"SUM(CASE WHEN {filter_condition} THEN 1 ELSE 0 END)"

    def cast_to_text(self, column_expr: str) -> str:
        """SQL Server text casting using CAST or CONVERT."""
        return f"CAST({column_expr} AS NVARCHAR(MAX))"

    def get_datetime_type(self) -> str:
        """Get the appropriate datetime type for SQL Server.

        SQL Server's TIMESTAMP is actually a binary row version, not a datetime.
        We need DATETIME2 for actual date/time values.
        """
        return "DATETIME2"

    def get_update_from_syntax(self, target_table: str, source_table: str,
                               target_alias: str = "m", source_alias: str = "s") -> str:
        """SQL Server UPDATE FROM syntax - aliases go after table names in FROM clause."""
        return f"UPDATE {target_alias}\\nSET\\nFROM {target_table} {target_alias}, {source_table} {source_alias}"

    def get_drop_table_sql(self, table_name: str) -> str:
        """SQL Server DROP TABLE with object existence check."""
        return f'IF OBJECT_ID(\'{table_name}\', \'U\') IS NOT NULL DROP TABLE "{table_name}"'

    def format_watermark_value(self, watermark_value: Union[int, datetime.datetime, str]) -> str:
        """SQL Server-specific watermark value formatting."""
        if isinstance(watermark_value, int):
            return str(watermark_value)
        elif isinstance(watermark_value, datetime.datetime):
            # SQL Server prefers explicit DATETIME2 casting for precision
            return f"CAST('{watermark_value.isoformat()}' AS DATETIME2)"
        elif isinstance(watermark_value, str):
            # If it's already a string, assume it's properly formatted
            return watermark_value
        else:
            return str(watermark_value)

    def get_watermark_records_lt(
        self,
        source_table: str,
        all_columns: List[str],
        pk_columns: List[str],
        watermark_column: str,
        watermark_value: Union[int, datetime.datetime, str]
    ) -> str:
        """SQL Server-specific implementation for watermark < value query."""
        quoted_columns = self.get_quoted_columns(all_columns)
        formatted_value = self.format_watermark_value(watermark_value)

        # Build hash expressions
        all_columns_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_columns_hash_expr = self.build_hash_expression_for_columns(pk_columns)

        return f"""
        SELECT {', '.join(quoted_columns)},
            {all_columns_hash_expr} as {self.schema_projector.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {self.schema_projector.KEY_HASH_COLUMN_NAME}
        FROM {source_table}
        WHERE [{watermark_column}] < {formatted_value}
        ORDER BY [{watermark_column}]
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
        """SQL Server-specific implementation for watermark range query."""
        quoted_columns = self.get_quoted_columns(all_columns)
        formatted_low = self.format_watermark_value(low_watermark_value)
        formatted_high = self.format_watermark_value(high_watermark_value)

        # Build hash expressions
        all_columns_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_columns_hash_expr = self.build_hash_expression_for_columns(pk_columns)

        return f"""
        SELECT {', '.join(quoted_columns)},
            {all_columns_hash_expr} as {self.schema_projector.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {self.schema_projector.KEY_HASH_COLUMN_NAME}
        FROM {source_table}
        WHERE [{watermark_column}] >= {formatted_low} AND [{watermark_column}] < {formatted_high}
        ORDER BY [{watermark_column}]
        """

    def get_max_watermark_value(self, source_table: str, watermark_column: str) -> str:
        """SQL Server-specific implementation to get the maximum watermark value."""
        return f"""
        SELECT MAX([{watermark_column}])
        FROM {source_table}
        """


class TimingDatabaseOperationsWrapper:
    """Wrapper that adds timing to all DatabaseOperations method calls.

    Activated by setting environment variable ENABLE_DATABASE_TIMING=true
    """

    def __init__(self, wrapped_operations: DatabaseOperations, prefix: str = ""):
        self._wrapped = wrapped_operations
        self._prefix = prefix
        self._total_time = 0.0
        self._call_count = 0

        # Wrap all public methods that aren't private/dunder
        for attr_name in dir(wrapped_operations):
            if not attr_name.startswith('_') and callable(getattr(wrapped_operations, attr_name)):
                wrapped_method = getattr(wrapped_operations, attr_name)
                timing_method = self._create_timing_wrapper(attr_name, wrapped_method)
                setattr(self, attr_name, timing_method)

    def _create_timing_wrapper(self, method_name: str, original_method: Callable) -> Callable:
        """Create a timing wrapper for a specific method."""

        @functools.wraps(original_method)
        def timing_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = original_method(*args, **kwargs)
                return result
            finally:
                elapsed_time = time.time() - start_time
                self._total_time += elapsed_time
                self._call_count += 1

                # Log the timing
                logger.info(f"🔧 {self._prefix}DatabaseOps.{method_name} took {elapsed_time:.3f}s")

                # For SQL operations, show the SQL if it's a string return value
                if isinstance(result, str) and any(keyword in result.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE']):
                    # Show first line of SQL for context
                    first_line = result.strip().split('\n')[0][:100]
                    logger.info(f"   📄 SQL: {first_line}...")

        return timing_wrapper

    def get_timing_summary(self) -> dict:
        """Get summary of timing information."""
        return {
            'total_time': self._total_time,
            'call_count': self._call_count,
            'average_time': self._total_time / self._call_count if self._call_count > 0 else 0
        }

    def __getattr__(self, name: str) -> Any:
        """Delegate any uncaptured attributes to the wrapped object."""
        return getattr(self._wrapped, name)


class DatabaseOperationsFactory:
    """Factory class to create appropriate DatabaseOperations instance based on data container type."""

    @staticmethod
    def create_database_operations(
        data_container: DataContainer,
        schema_projector: 'YellowSchemaProjector'
    ) -> DatabaseOperations:
        """Create appropriate DatabaseOperations instance based on data container type.

        Args:
            data_container: The data container specifying database type
            schema_projector: Schema projector instance

        Returns:
            DatabaseOperations instance for the specific database type (wrapped with timing if ENABLE_DATABASE_TIMING=true)

        Raises:
            ValueError: If database type is not supported
        """
        # Create the appropriate database operations instance
        if isinstance(data_container, PostgresDatabase):
            operations = PostgresDatabaseOperations(schema_projector)
            prefix = "PG_"
        elif isinstance(data_container, SQLServerDatabase):
            operations = SQLServerDatabaseOperations(schema_projector)
            prefix = "MSSQL_"
        else:
            raise ValueError(f"Unsupported database type: {type(data_container)}. "
                             f"Supported types: PostgresDatabase, SQLServerDatabase")

        # Check if timing is enabled via environment variable
        enable_timing = os.getenv('ENABLE_DATABASE_TIMING', '').lower() in ('true', '1', 'yes', 'on')

        if enable_timing:
            logger.info(f"🔧 Database timing enabled for {prefix}DatabaseOperations")
            return cast(DatabaseOperations, TimingDatabaseOperationsWrapper(operations, prefix))
        else:
            return operations
