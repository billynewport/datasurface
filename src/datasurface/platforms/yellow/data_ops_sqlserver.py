"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import List, Optional, Any, Union
import datetime
from sqlalchemy.engine import Connection
from sqlalchemy import text
from datasurface.platforms.yellow.database_operations import DatabaseOperations
from datasurface.platforms.yellow.yellow_constants import YellowSchemaConstants
from datasurface.md import SchemaProjector, DataContainer


class SQLServerDatabaseOperations(DatabaseOperations):
    """SQL Server-specific database operations implementation."""

    def __init__(self, schema_projector: SchemaProjector, data_container: DataContainer) -> None:
        super().__init__(schema_projector, data_container)

    def get_hash_expression(self, columns: List[str]) -> str:
        if len(columns) == 1:
            return f"CONVERT(VARCHAR(32), HASHBYTES('MD5', {columns[0]}), 2)"
        else:
            concat_op = self.get_string_concat_operator()
            concatenated = f" {concat_op} ".join(columns)
            return f"CONVERT(VARCHAR(32), HASHBYTES('MD5', {concatenated}), 2)"

    def get_hash_column_width(self) -> int:
        return 32

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
            SELECT {', '.join([f'[{col}]' for col in all_columns])},
                   {all_hash_column},
                   {key_hash_column}
            FROM source_data
            """
        else:
            # Simple table name - add WHERE clause
            source_select = f"""
            SELECT {', '.join([f'[{col}]' for col in all_columns])},
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
                {', '.join([f'[{col}] = source_data.[{col}]' for col in all_columns])},
                {batch_id_column} = {batch_id},
                {all_hash_column} = source_data.{all_hash_column}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(quoted_columns)}, {batch_id_column}, {all_hash_column}, {key_hash_column})
            VALUES ({', '.join([f'source_data.[{col}]' for col in all_columns])}, {batch_id}, source_data.{all_hash_column}, source_data.{key_hash_column});
        """
        else:
            return f"""
        MERGE {target_table} AS target
        USING {source_clause}
        ON target.{key_hash_column} = source.{key_hash_column}
        WHEN MATCHED AND target.{all_hash_column} != source.{all_hash_column} THEN
            UPDATE SET
                {', '.join([f'[{col}] = source.[{col}]' for col in all_columns])},
                {batch_id_column} = {batch_id},
                {all_hash_column} = source.{all_hash_column}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(quoted_columns)}, {batch_id_column}, {all_hash_column}, {key_hash_column})
            VALUES ({', '.join([f'source.[{col}]' for col in all_columns])}, {batch_id}, source.{all_hash_column}, source.{key_hash_column});
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
            WHERE s.[{batch_id_column}] = {batch_id}
            AND s.[{iud_column}] = 'D'
            AND s.[{key_hash_column}] = m.[{key_hash_column}]
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
            SELECT {', '.join([f'[{col}]' for col in all_columns])},
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
            VALUES ({', '.join([f'source.[{col}]' for col in all_columns])}, source.{all_hash_column}, source.{key_hash_column}, {batch_id}, {live_record_id})
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
            {', '.join([f's.[{col}]' for col in all_columns])},
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

    # Removed duplicate early implementations of check_index_exists/create_index_sql; canonical versions are defined later

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
        return f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]"

    def get_quoted_columns(self, columns: List[str]) -> List[str]:
        """Override to use bracket quoting for SQL Server identifiers."""
        return [f'[{col}]' for col in columns]

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
            {all_columns_hash_expr} as {YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
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
            {all_columns_hash_expr} as {YellowSchemaConstants.ALL_HASH_COLUMN_NAME},
            {key_columns_hash_expr} as {YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
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

    def get_remote_forensic_update_closed_sql(self, merge_table: str, staging_table: str, sp: Any, batch_id: int) -> str:
        return f"""
        UPDATE m
        SET {YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = s.[remote_{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME}]
        FROM {merge_table} m
        INNER JOIN {staging_table} s ON m.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME} = s.{YellowSchemaConstants.KEY_HASH_COLUMN_NAME}
            AND m.{YellowSchemaConstants.BATCH_IN_COLUMN_NAME} = s.[remote_{YellowSchemaConstants.BATCH_IN_COLUMN_NAME}]
        WHERE m.{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME} = {YellowSchemaConstants.LIVE_RECORD_ID}
            AND s.[remote_{YellowSchemaConstants.BATCH_OUT_COLUMN_NAME}] != {YellowSchemaConstants.LIVE_RECORD_ID}
            AND s.{YellowSchemaConstants.BATCH_ID_COLUMN_NAME} = {batch_id}
        """

    def check_unique_constraint_exists(self, connection: Connection, table_name: str, column_name: str) -> bool:
        """Check if a unique constraint exists on the specified column using SQL Server INFORMATION_SCHEMA."""
        check_sql = """
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        WHERE tc.TABLE_NAME = :table_name
            AND tc.CONSTRAINT_TYPE = 'UNIQUE'
            AND kcu.COLUMN_NAME = :column_name
        """
        result = connection.execute(text(check_sql), {"table_name": table_name, "column_name": column_name})
        return result.fetchone()[0] > 0

    def create_unique_constraint(self, connection: Connection, table_name: str, column_name: str) -> str:
        """Create a unique constraint on the specified column."""
        constraint_name = f"{table_name}_{column_name}_unique"
        create_constraint_sql = f"ALTER TABLE [{table_name}] ADD CONSTRAINT [{constraint_name}] UNIQUE ([{column_name}])"
        connection.execute(text(create_constraint_sql))
        return constraint_name

    def check_index_exists(self, connection: Connection, table_name: str, index_name: str) -> bool:
        """Check if an index exists on the specified table using SQL Server system views."""
        check_sql = """
        SELECT COUNT(*) FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        WHERE t.name = :table_name
            AND i.name = :index_name
        """
        result = connection.execute(text(check_sql), {"table_name": table_name, "index_name": index_name})
        return result.fetchone()[0] > 0

    def create_index_sql(self, index_name: str, table_name: str, columns: List[str]) -> str:
        """Generate SQL to create an index."""
        formatted_columns = [f"[{col}]" for col in columns]
        return f"CREATE INDEX [{index_name}] ON [{table_name}] ({', '.join(formatted_columns)})"
