# conftest.py
import os
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Generator


@pytest.fixture(scope="session")
def db_engine() -> Generator[Engine, None, None]:
    """Create a SQLAlchemy engine that works both locally and in GitHub Actions.

    Returns:
        Generator[Engine, None, None]: SQLAlchemy engine connected to the test database
    """
    database_url: str = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/test_db"
    )
    engine: Engine = create_engine(database_url)

    # Make sure connection works
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def test_db(db_engine: Engine) -> Generator[Engine, None, None]:
    """Provide a clean database for each test.

    Args:
        db_engine (Engine): The SQLAlchemy engine from the db_engine fixture

    Returns:
        Generator[Engine, None, None]: The same engine, but with a clean database state
    """
    # Create schema for the test
    # Here you could create specific tables for each test

    yield db_engine

    # Clean up - drop tables or truncate as needed
    # Example: with db_engine.connect() as conn:
    #     conn.execute(text("DROP TABLE IF EXISTS test_table"))
