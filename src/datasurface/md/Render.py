

from typing import Optional
from datasurface.md.Governance import Asset, Dataset, Datastore, Workspace

class Job:
    pass

class IngestJob(Job):
    """The ingestion of either ALL datasets in one batch OR a specific dataset"""
    def __init__(self, store : Datastore, dataset : Optional[Dataset] = None):
        self.store : Datastore = store
        self.dataset : Optional[Dataset] = dataset

class ExportJob(Job):
    """Exports are usually to a specific system table with Workspace specific views pointing to that physical table"""
    def __init__(self, asset : Asset, store : Datastore, dataset : Optional[Dataset]):
        self.asset : Asset = asset
        self.store : Datastore = store
        self.dataset : Optional[Dataset] = dataset


class TransformerJob(Job):
    def __init__(self, w : Workspace):
        self.workspace : Workspace = w

class TriggerJob(Job):
    pass

