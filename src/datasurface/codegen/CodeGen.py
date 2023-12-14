from typing import Any
from jinja2 import Environment, PackageLoader, select_autoescape

from datasurface.md import Datastore
from datasurface.md import Dataset
from datasurface.md import DDLTable
from datasurface.md.Schema import DDLColumn

def getDatasets(store : Datastore) -> list[Any]:
    datasets : list[Any] = []
    for dataset in store.datasets.values():
        if(dataset):
            datasets.append(dataset)
    return datasets

def getColumns(dataset : Dataset) -> list[DDLColumn]:
    columns : list[DDLColumn] = []
    if(dataset.originalSchema and type(dataset.originalSchema) == DDLTable):
        table : DDLTable = dataset.originalSchema
        for column in table.columns.values():
            if(column):
                columns.append(column)
    return columns

def generate_code(store : Datastore) -> str:
    env = Environment(
        loader=PackageLoader('dataglide.codegen', 'templates'),
        autoescape=select_autoescape(['html', 'xml'])
    )

    template = env.get_template('datastore.jinja2', None)

    data : dict[str, Any]= {}
    data["datastore"] = store
    data["getDatasets"] = getDatasets
    data["getColumns"] = getColumns
    code : str = template.render(data)
    return code