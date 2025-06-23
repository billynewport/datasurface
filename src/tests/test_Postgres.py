"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


from sqlalchemy import create_engine, MetaData  # type: ignore[attr-defined]
from sqlalchemy.exc import OperationalError
from datasurface.md import Datastore
from datasurface.md import convertSQLAlchemyTableSetToDatastore
from datasurface.codegen import generate_code

"""This test assumes a local postgres database with the northwind database loaded in the postgres database."""


def get_metadata():
    engine = None
    metadata = MetaData()
    try:
        engine = create_engine('postgresql://postgres:apjc3742@localhost:5432/postgres')  # type: ignore[attr-defined]
        metadata.reflect(bind=engine)  # type: ignore[attr-defined]
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return metadata


# Disabled except when running locally, needs a postgres database
# with the northwind database loaded
def xtest_get_metadata():
    """This test assumes a local postgres database with the northwind database loaded in the postgres database."""
    metadata = get_metadata()
    store: Datastore = convertSQLAlchemyTableSetToDatastore("Test_Store", list(metadata.tables.values()))
    for dataset in store.datasets.values():
        if (dataset):
            print("Dataset: {}".format(dataset.name))
    assert store.name == "Test_Store"
    code: str = generate_code(store)
    print(code)
    assert len(metadata.tables) > 0
