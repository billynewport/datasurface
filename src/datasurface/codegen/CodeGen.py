"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import Any
from jinja2 import Environment, PackageLoader, select_autoescape

from datasurface.md import Datastore
from datasurface.md import Dataset
from datasurface.md import DDLTable
from datasurface.md.schema import DDLColumn, DEFAULT_nullable, DEFAULT_primaryKey


def getDatasets(store: Datastore) -> list[Any]:
    datasets: list[Any] = []
    for dataset in store.datasets.values():
        if (dataset):
            datasets.append(dataset)
    return datasets


def getColumns(dataset: Dataset) -> list[DDLColumn]:
    columns: list[DDLColumn] = []
    if (dataset.originalSchema and isinstance(dataset.originalSchema, DDLTable)):
        table: DDLTable = dataset.originalSchema
        for column in table.columns.values():
            if (column):
                columns.append(column)
    return columns


def convertColumnAttributesToString(column: DDLColumn) -> str:
    rc: str = ""
    if (column.nullable != DEFAULT_nullable):
        rc += f", {column.nullable}"
    if (column.classification is not None):
        rc += f", {column.classification}"
    if (column.primaryKey != DEFAULT_primaryKey):
        rc += f", {column.primaryKey}"
    return rc


def generate_code(store: Datastore) -> str:
    env = Environment(
        loader=PackageLoader('datasurface.codegen', 'templates'),
        autoescape=select_autoescape(['html', 'xml'])
    )

    template = env.get_template('datastore.jinja2', None)

    data: dict[str, Any] = {}
    data["datastore"] = store
    data["getDatasets"] = getDatasets
    data["getColumns"] = getColumns
    data["convertColumnAttributesToString"] = convertColumnAttributesToString
    code: str = template.render(data)
    return code
