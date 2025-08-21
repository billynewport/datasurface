"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataContainer, PostgresDatabase, MySQLDatabase, OracleDatabase, SQLServerDatabase, DB2Database, HostPortSQLDatabase
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from typing import Optional


def getDriverNameAndQueryForDataContainer(container: DataContainer) -> tuple[str, Optional[str]]:
    """This returns the driver name and query for a given data container."""
    if isinstance(container, PostgresDatabase):
        return "postgresql", None
    elif isinstance(container, MySQLDatabase):
        return "mysql+pymysql", None
    elif isinstance(container, OracleDatabase):
        return "oracle+cx_oracle", None
    elif isinstance(container, SQLServerDatabase):
        return "mssql+pyodbc", "ODBC Driver 17 for SQL Server"
    elif isinstance(container, DB2Database):
        return "db2+ibm_db", None
    else:
        raise ValueError(f"Unsupported data container type: {type(container)}")


def createEngine(container: HostPortSQLDatabase, userName: str, password: str) -> Engine:
    """This creates a SQLAlchemy engine for a given data container."""
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
        url_params["query"] = {"driver": query}

    db_url = URL.create(**url_params)

    return create_engine(
        db_url,
        isolation_level="READ COMMITTED"
    )
