"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataContainer, PostgresDatabase, MySQLDatabase, OracleDatabase, SQLServerDatabase, DB2Database, HostPortSQLDatabase
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
        return "oracle+cx_oracle", None
    elif isinstance(container, SQLServerDatabase):
        return "mssql+pyodbc", "ODBC Driver 18 for SQL Server"
    elif isinstance(container, DB2Database):
        return "db2+ibm_db", None
    else:
        raise ValueError(f"Unsupported data container type: {type(container)}")


def createInspector(engine: Engine) -> Inspector:
    # Time the inspector creation
    inspector = inspect(engine)  # type: ignore[attr-defined]
    return inspector


engineCache: Dict[str, Engine] = {}


def createEngine(container: HostPortSQLDatabase, userName: str, password: str) -> Engine:
    """This creates a SQLAlchemy engine for a given data container."""
    engineKey: str = f"{container.hostPortPair.hostName}:{container.hostPortPair.port}:{container.databaseName}:{userName}:{password}"
    if engineKey in engineCache:
        return engineCache[engineKey]

    driverName, query = getDriverNameAndQueryForDataContainer(container)

    # Build common URL parameters
    url_params = {
        "drivername": driverName,
        "username": userName,
        "password": password,
        "host": container.hostPortPair.hostName,
        "port": container.hostPortPair.port,
        "database": container.databaseName,
    }

    # Add query parameters if needed (mainly for SQL Server)
    if query:
        query_params = {"driver": query}
        # Add TrustServerCertificate for SQL Server to handle self-signed certificates
        if isinstance(container, SQLServerDatabase):
            query_params["TrustServerCertificate"] = "yes"
        url_params["query"] = query_params

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

    engine: Engine = create_engine(db_url, **engine_kwargs)
    engineCache[engineKey] = engine
    return engine
