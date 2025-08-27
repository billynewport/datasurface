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


class PostgresDatabaseOperations(DatabaseOperations):
    """PostgreSQL-specific database operations implementation."""

    def __init__(self, schema_projector: SchemaProjector, data_container: DataContainer) -> None:
        super().__init__(schema_projector, data_container)

    def get_hash_expression(self, columns: List[str]) -> str:
        if len(columns) == 1:
            return f"MD5({columns[0]})"
        else:
            concat_op = self.get_string_concat_operator()
            concatenated = f" {concat_op} ".join(columns)
            return f"MD5({concatenated})"

    def get_hash_column_width(self) -> int:
        return 32

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
            WHERE s.{self.nm.fmtCol(batch_id_column)} = {batch_id}
            AND s.{self.nm.fmtCol(iud_column)} = 'D'
            AND s.{self.nm.fmtCol(key_hash_column)} = m.{self.nm.fmtCol(key_hash_column)}
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

        close_changed_sql = f"""
        UPDATE {target_table} m
        SET {batch_out_column} = {batch_id - 1}
        FROM {source_table} s
        WHERE m.{key_hash_column} = s.{key_hash_column}
            AND m.{batch_out_column} = {live_record_id}
            AND m.{all_hash_column} != s.{all_hash_column}
            AND s.{batch_id_column} = {batch_id}
        """

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

    # Removed duplicate early implementations of check_index_exists/create_index_sql; canonical versions are defined later

    def create_unique_constraint_sql(self, table_name: str, column_name: str, constraint_name: str) -> str:
        return f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({column_name})"

    def get_json_extract_expression(self, json_column: str, key: str) -> str:
        return f"{json_column}->>{key!r}"

    def get_row_number_expression(self, partition_by: List[str], order_by: List[str]) -> str:
        partition_clause = f"PARTITION BY {', '.join(partition_by)}" if partition_by else ""
        order_clause = f"ORDER BY {', '.join(order_by)}" if order_by else ""
        return f"ROW_NUMBER() OVER ({partition_clause} {order_clause})".strip()

    def get_count_filter_expression(self, filter_condition: str) -> str:
        return f"COUNT(*) FILTER (WHERE {filter_condition})"

    def cast_to_text(self, column_expr: str) -> str:
        return f"{column_expr}::text"

    def get_datetime_type(self) -> str:
        return "TIMESTAMP"

    def get_update_from_syntax(self, target_table: str, source_table: str,
                               target_alias: str = "m", source_alias: str = "s") -> str:
        return f"UPDATE {target_table} {target_alias}\\nFROM {source_table} {source_alias}"

    def get_drop_table_sql(self, table_name: str) -> str:
        return f'DROP TABLE IF EXISTS "{table_name}" CASCADE'

    def format_watermark_value(self, watermark_value: Union[int, datetime.datetime, str]) -> str:
        if isinstance(watermark_value, int):
            return str(watermark_value)
        elif isinstance(watermark_value, datetime.datetime):
            return f"TIMESTAMP '{watermark_value.isoformat()}'"
        elif isinstance(watermark_value, str):
            if '-' in watermark_value and ':' in watermark_value:
                return f"TIMESTAMP '{watermark_value}'"
            else:
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
        quoted_columns = self.get_quoted_columns(all_columns)
        formatted_value = self.format_watermark_value(watermark_value)
        all_columns_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_columns_hash_expr = self.build_hash_expression_for_columns(pk_columns)
        return f"""
        SELECT {', '.join(quoted_columns)},
            {all_columns_hash_expr} as {self.nm.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME)},
            {key_columns_hash_expr} as {self.nm.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)}
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
        quoted_columns = self.get_quoted_columns(all_columns)
        formatted_low = self.format_watermark_value(low_watermark_value)
        formatted_high = self.format_watermark_value(high_watermark_value)
        all_columns_hash_expr = self.build_hash_expression_for_columns(all_columns)
        key_columns_hash_expr = self.build_hash_expression_for_columns(pk_columns)
        return f"""
        SELECT {', '.join(quoted_columns)},
            {all_columns_hash_expr} as {self.nm.fmtCol(YellowSchemaConstants.ALL_HASH_COLUMN_NAME)},
            {key_columns_hash_expr} as {self.nm.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)}
        FROM {source_table}
        WHERE "{watermark_column}" >= {formatted_low} AND "{watermark_column}" < {formatted_high}
        ORDER BY "{watermark_column}"
        """

    def get_max_watermark_value(self, source_table: str, watermark_column: str) -> str:
        return f"""
        SELECT MAX("{watermark_column}")
        FROM {source_table}
        """

    def get_remote_forensic_update_closed_sql(self, merge_table: str, staging_table: str, sp: Any, batch_id: int) -> str:
        remote_batch_out_column = self.nm.fmtCol("remote_" + YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)
        remote_batch_in_column = self.nm.fmtCol("remote_" + YellowSchemaConstants.BATCH_IN_COLUMN_NAME)
        return f"""
        UPDATE {merge_table} m
        SET {self.nm.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)} = s.{remote_batch_out_column}
        FROM {staging_table} s
        WHERE m.{self.nm.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)} = s.{self.nm.fmtCol(YellowSchemaConstants.KEY_HASH_COLUMN_NAME)}
            AND m.{self.nm.fmtCol(YellowSchemaConstants.BATCH_IN_COLUMN_NAME)} = s.{remote_batch_in_column}
            AND m.{self.nm.fmtCol(YellowSchemaConstants.BATCH_OUT_COLUMN_NAME)} = {YellowSchemaConstants.LIVE_RECORD_ID}
            AND s.{remote_batch_out_column} != {YellowSchemaConstants.LIVE_RECORD_ID}
            AND s.{self.nm.fmtCol(YellowSchemaConstants.BATCH_ID_COLUMN_NAME)} = {batch_id}
        """

    def check_unique_constraint_exists(self, connection: Connection, table_name: str, column_name: str) -> bool:
        """Check if a unique constraint exists on the specified column using PostgreSQL INFORMATION_SCHEMA."""
        check_sql = """
        SELECT COUNT(*) FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_name = :table_name
            AND tc.constraint_type = 'UNIQUE'
            AND kcu.column_name = :column_name
        """
        result = connection.execute(text(check_sql), {"table_name": table_name, "column_name": column_name})
        return result.fetchone()[0] > 0

    def create_unique_constraint(self, connection: Connection, table_name: str, column_name: str) -> str:
        """Create a unique constraint on the specified column."""
        constraint_name = f"{table_name}_{column_name}_unique"
        # Use the naming mapper to format table and column names
        formatted_table = self.nm.fmtTVI(table_name)
        formatted_column = self.nm.fmtCol(column_name)
        create_constraint_sql = f"ALTER TABLE {formatted_table} ADD CONSTRAINT {constraint_name} UNIQUE ({formatted_column})"
        connection.execute(text(create_constraint_sql))
        return constraint_name

    def check_index_exists(self, connection: Connection, table_name: str, index_name: str) -> bool:
        """Check if an index exists on the specified table using PostgreSQL system views."""
        check_sql = """
        SELECT COUNT(*) FROM pg_indexes
        WHERE tablename = :table_name
            AND indexname = :index_name
            AND schemaname = 'public'
        """
        result = connection.execute(text(check_sql), {"table_name": table_name, "index_name": index_name})
        return result.fetchone()[0] > 0

    def create_index_sql(self, index_name: str, table_name: str, columns: List[str]) -> str:
        """Generate SQL to create an index."""
        formatted_table = self.nm.fmtTVI(table_name)
        formatted_columns = [self.nm.fmtCol(col) for col in columns]
        return f"CREATE INDEX {index_name} ON {formatted_table} ({', '.join(formatted_columns)})"
