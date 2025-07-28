"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataContainer, PostgresDatabase, MySQLDatabase, OracleDatabase, SQLServerDatabase, DB2Database
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def createEngine(container: DataContainer, userName: str, password: str) -> Engine:
    """This creates a SQLAlchemy engine for a given data container."""
    if isinstance(container, PostgresDatabase):
        return create_engine(  # type: ignore[attr-defined]
            'postgresql://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                username=userName,
                password=password,
                hostName=container.hostPortPair.hostName,
                port=container.hostPortPair.port,
                databaseName=container.databaseName
            ),
            isolation_level="READ COMMITTED"
        )
    elif isinstance(container, MySQLDatabase):
        return create_engine(
            'mysql+pymysql://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                username=userName,
                password=password,
                hostName=container.hostPortPair.hostName,
                port=container.hostPortPair.port,
                databaseName=container.databaseName
            ),
            isolation_level="READ COMMITTED"
        )
    elif isinstance(container, OracleDatabase):
        return create_engine(
            'oracle+cx_oracle://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                username=userName,
                password=password,
                hostName=container.hostPortPair.hostName,
                port=container.hostPortPair.port,
                databaseName=container.databaseName
            ),
            isolation_level="READ COMMITTED"
        )
    elif isinstance(container, SQLServerDatabase):
        return create_engine(
            'mssql+pyodbc://{username}:{password}@{hostName}:{port}/{databaseName}?driver=ODBC+Driver+17+for+SQL+Server'.format(
                username=userName,
                password=password,
                hostName=container.hostPortPair.hostName,
                port=container.hostPortPair.port,
                databaseName=container.databaseName
            ),
            isolation_level="READ COMMITTED"
        )
    elif isinstance(container, DB2Database):
        return create_engine(
            'db2+ibm_db://{username}:{password}@{hostName}:{port}/{databaseName}'.format(
                username=userName,
                password=password,
                hostName=container.hostPortPair.hostName,
                port=container.hostPortPair.port,
                databaseName=container.databaseName
            ),
            isolation_level="READ COMMITTED"
        )
    else:
        raise ValueError(f"Unsupported data container type: {type(container)}") 