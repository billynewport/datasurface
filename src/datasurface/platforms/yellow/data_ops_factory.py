"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import Any, Callable, cast
from datasurface.md.governance import SchemaProjector
from datasurface.platforms.yellow.database_operations import DatabaseOperations
from datasurface.platforms.yellow.data_ops_postgres import PostgresDatabaseOperations
from datasurface.platforms.yellow.data_ops_sqlserver import SQLServerDatabaseOperations
from datasurface.platforms.yellow.data_ops_oracle import OracleDatabaseOperations
from datasurface.platforms.yellow.data_ops_db2 import DB2DatabaseOperations
from datasurface.platforms.yellow.data_ops_snowflake import SnowflakeDatabaseOperations
from datasurface.md import DataContainer, PostgresDatabase, SQLServerDatabase, OracleDatabase, DB2Database, SnowFlakeDatabase
import os
import time
import functools
from datasurface.platforms.yellow.logging_utils import get_contextual_logger

logger = get_contextual_logger(__name__)


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
            result: Any = None
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
        schema_projector: SchemaProjector
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
        from datasurface.platforms.yellow.yellow_dp import YellowSchemaProjector
        assert isinstance(schema_projector, YellowSchemaProjector)

        # Create the appropriate database operations instance
        if isinstance(data_container, PostgresDatabase):
            operations = PostgresDatabaseOperations(schema_projector, data_container)
            prefix = "PG_"
        elif isinstance(data_container, SQLServerDatabase):
            operations = SQLServerDatabaseOperations(schema_projector, data_container)
            prefix = "MSSQL_"
        elif isinstance(data_container, OracleDatabase):
            operations = OracleDatabaseOperations(schema_projector, data_container)
            prefix = "ORA_"
        elif isinstance(data_container, DB2Database):
            operations = DB2DatabaseOperations(schema_projector, data_container)
            prefix = "DB2_"
        elif isinstance(data_container, SnowFlakeDatabase):
            operations = SnowflakeDatabaseOperations(schema_projector, data_container)
            prefix = "SNOWFLAKE_"
        else:
            raise ValueError(f"Unsupported database type: {type(data_container)}. "
                             f"Supported types: PostgresDatabase, SQLServerDatabase, OracleDatabase, DB2Database, SnowFlakeDatabase")

        # Check if timing is enabled via environment variable
        enable_timing = os.getenv('ENABLE_DATABASE_TIMING', '').lower() in ('true', '1', 'yes', 'on')

        if enable_timing:
            logger.info(f"🔧 Database timing enabled for {prefix}DatabaseOperations")
            return cast(DatabaseOperations, TimingDatabaseOperationsWrapper(operations, prefix))
        else:
            return operations
