"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import (
    DataContainer,
    PostgresDatabase,
    MySQLDatabase,
    OracleDatabase,
    SQLServerDatabase,
    DB2Database,
    HostPortSQLDatabase,
    SnowFlakeDatabase,
)
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from typing import Optional, Dict, Any
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.inspection import inspect


def getDriverNameAndQueryForDataContainer(container: DataContainer) -> tuple[str, Optional[str]]:
    """This returns the driver name and query for a given data container."""
    if isinstance(container, PostgresDatabase):
        return "postgresql", None
    elif isinstance(container, MySQLDatabase):
        return "mysql+pymysql", None
    elif isinstance(container, OracleDatabase):
        return "oracle+oracledb", None
    elif isinstance(container, SQLServerDatabase):
        return "mssql+pyodbc", "ODBC Driver 18 for SQL Server"
    elif isinstance(container, DB2Database):
        return "db2+ibm_db", None
    elif isinstance(container, SnowFlakeDatabase):
        return "snowflake", None
    else:
        raise ValueError(f"Unsupported data container type: {type(container)}")


def createInspector(engine: Engine) -> Inspector:
    # Time the inspector creation
    inspector = inspect(engine)  # type: ignore[attr-defined]
    return inspector


engineCache: Dict[str, Engine] = {}


def createEngine(container: DataContainer, userName: str, password: str) -> Engine:
    """This creates a SQLAlchemy engine for a given data container."""
    # Build a cache key depending on container type
    if isinstance(container, HostPortSQLDatabase):
        engineKey: str = f"{container.hostPortPair.hostName}:{container.hostPortPair.port}:{container.databaseName}:{userName}:{password}"
    elif isinstance(container, SnowFlakeDatabase):
        # Compose account with optional region if not already included
        account_full: str = container.account
        if container.region and "." not in account_full:
            account_full = f"{account_full}.{container.region}"
        engineKey = f"sf:{account_full}:{container.databaseName}:{container.warehouse}:{container.role}:{userName}:{password}"
    else:
        # Fallback generic key
        engineKey = f"{container.__class__.__name__}:{userName}:{password}:{getattr(container, 'databaseName', '')}"
    if engineKey in engineCache:
        return engineCache[engineKey]

    driverName, query = getDriverNameAndQueryForDataContainer(container)

    # Build URL parameters per container type
    url_params: Dict[str, Any]
    if isinstance(container, HostPortSQLDatabase):
        url_params = {
            "drivername": driverName,
            "username": userName,
            "password": password,
            "host": container.hostPortPair.hostName,
            "port": container.hostPortPair.port,
            "database": container.databaseName,
        }
    elif isinstance(container, SnowFlakeDatabase):
        # Snowflake uses account (as host portion), HTTPS on 443, and optional warehouse/role in query
        account_full: str = container.account
        if container.region and "." not in account_full:
            account_full = f"{account_full}.{container.region}"
        query_params: Dict[str, Any] = {}
        if container.warehouse:
            query_params["warehouse"] = container.warehouse
        if container.role:
            query_params["role"] = container.role
        if container.schema:
            query_params["schema"] = container.schema
        url_params = {
            "drivername": driverName,
            "username": userName,
            "password": password,
            # account identifier goes in the host component for SQLAlchemy's snowflake dialect
            "host": account_full,
            "database": container.databaseName,
            # schema not specified here; can be set by callers/session if needed
        }
        if query_params:
            url_params["query"] = query_params
    else:
        raise ValueError(f"Unsupported data container type for engine creation: {type(container)}")

    # Add query parameters if needed from getDriverNameAndQueryForDataContainer (e.g., SQL Server ODBC driver)
    if query:
        # Merge/extend existing query dict if present
        base_query: Dict[str, Any] = url_params.get("query", {})  # type: ignore[assignment]
        if isinstance(base_query, dict):
            base_query.update({"driver": query})
            if isinstance(container, SQLServerDatabase):
                base_query["TrustServerCertificate"] = "yes"
            url_params["query"] = base_query
        else:
            url_params["query"] = {"driver": query}

    db_url = URL.create(**url_params)

    # Performance optimizations based on database type
    engine_kwargs: Dict[str, Any] = {
        "isolation_level": "READ COMMITTED"
    }

    # Add performance optimizations for SQL Server
    if isinstance(container, SQLServerDatabase):
        engine_kwargs.update({
            # Optimized connection pooling for DDL performance
            "pool_size": 5,  # Smaller pool for better resource management
            "max_overflow": 10,  # Reduced overflow for faster allocation
            "pool_pre_ping": True,
            "pool_recycle": 1800,  # Recycle connections every 30 minutes
            "pool_timeout": 5,  # Fast timeout for connection acquisition
            # SQL Server specific optimizations for DDL performance
            "connect_args": {
                "timeout": 15,  # Reduced timeout for faster failures
                "autocommit": False,  # Keep False - this was proven faster in diagnostic results
                "fast_executemany": True,  # Faster bulk operations
                # Additional ODBC performance settings
                "MARS_Connection": "yes",  # Multiple Active Result Sets
                "Connection Timeout": "3",  # Much faster connection establishment
                "Command Timeout": "15",   # Faster command timeout for DDL
                # Additional optimizations for DDL performance
                "TrustServerCertificate": "yes",  # Avoid SSL overhead in dev environments
                "AnsiNPW": "yes",  # Use ANSI null padding - slightly faster
                "AnsiNulls": "yes",  # ANSI null handling
            }
        })
    elif isinstance(container, PostgresDatabase):
        engine_kwargs.update({
            # PostgreSQL optimizations
            "pool_size": 10,
            "max_overflow": 20,
            "pool_pre_ping": True,
            "pool_recycle": 3600,
        })
    elif isinstance(container, DB2Database):
        engine_kwargs.update({
            # DB2 optimizations
            "pool_size": 8,
            "max_overflow": 15,
            "pool_pre_ping": True,
            "pool_recycle": 3600,  # Recycle connections every hour
            "pool_timeout": 10,  # Timeout for connection acquisition
            "isolation_level": "CS",  # DB2 uses CS (Cursor Stability) instead of READ COMMITTED
        })
    elif isinstance(container, SnowFlakeDatabase):
        # Reasonable defaults for Snowflake; connections are HTTPS-based
        engine_kwargs.update({
            "pool_pre_ping": True,
        })

    engine: Engine = create_engine(db_url, **engine_kwargs)
    engineCache[engineKey] = engine
    return engine
