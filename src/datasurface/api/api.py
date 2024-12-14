"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import sys
import threading
import time
from typing import Optional, cast

import grpc
from datasurface.api.api_pb2_grpc import ProducerServiceServicer, add_ProducerServiceServicer_to_server  # type: ignore
from datasurface.api.api_pb2 import DataStoreRequest
from datasurface.handler.action import GitHubCICD
from datasurface.handler.cicd import RepositorywithCICD
from datasurface.md import Schema, DDLTable, NullableStatus
from datasurface.md import Datastore, DatastoreCacheEntry, Ecosystem
from datasurface.api.api_pb2 import Datastore as DatastoreProto
from datasurface.api.api_pb2 import Dataset as DatasetProto
from datasurface.api.api_pb2 import Schema as SchemaProto
from datasurface.api.api_pb2 import DDLColumn as DDLColumnProto
from datasurface.api.api_pb2 import DDLSchema as DDLSchemaProto
from concurrent import futures
from threading import Thread


class ProducerServiceImpl(ProducerServiceServicer):
    def __init__(self, eco: Ecosystem):
        self.eco: Ecosystem = eco

    def getDatastore(self, request, context):  # type: ignore
        req: DataStoreRequest = cast(DataStoreRequest, request)
        store: DatastoreCacheEntry = self.eco.cache_getDatastoreOrThrow(req.dataStoreName)
        return self._convertFromDatastore(store.datastore)

    def _convertFromDatastore(self, store: Datastore) -> DatastoreProto:
        rc: DatastoreProto = DatastoreProto()
        rc.name = store.name
        assert store.key is not None
        rc.teamName = store.key.tdName
        for dataset in store.datasets.values():
            s: DatasetProto = DatasetProto()
            s.name = dataset.name
            s.datastoreName = store.name
            assert dataset.originalSchema is not None
            s.schema.CopyFrom(self._convertFromSchema(dataset.originalSchema))
            rc.datasets.add().CopyFrom(s)
        return rc

    def _convertFromSchema(self, schema: Schema) -> SchemaProto:
        rc: SchemaProto = SchemaProto()
        if schema.primaryKeyColumns is not None:
            for colName in schema.primaryKeyColumns.colNames:
                rc.primaryKeys.append(colName)
        if schema.ingestionPartitionColumns is not None:
            for colName in schema.ingestionPartitionColumns.colNames:
                rc.ingestionPartitionKeys.append(colName)
        assert isinstance(schema, DDLTable)

        d: DDLSchemaProto = DDLSchemaProto()
        for col in schema.columns.values():
            c: DDLColumnProto = DDLColumnProto()
            c.name = col.name
            # DataType and its subclasses should have an to_json method to return a good description of the type
            # to_json was used because it's easiest to parse by clients. str(type) was considered but its too hard
            # to easily parse on the client.
            c.type = col.type.to_json()
            c.isNullable = col.nullable == NullableStatus.NULLABLE
            d.columns.add().CopyFrom(c)
        rc.ddlSchema.CopyFrom(d)
        return rc


class DataSurfaceServer(Thread):
    """This is a GRPC server which provides an API to interrogate an Ecosystem model"""
    def __init__(self, eco: Ecosystem, port: int = 50051, maxWorkers: int = 10):
        super().__init__()
        self.port = port
        self.maxWorkers = maxWorkers
        self.eco = eco
        # An event we can set to tell the server to stop
        self.stop_server_event = threading.Event()
        # An event which the server will set when the server has started
        self.serverStarted_event = threading.Event()
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.maxWorkers))  # type: ignore

    def waitUntilStopped(self):
        # Wait for a stopEvent or a ctrl-c
        try:
            while True:
                if self.stop_server_event.is_set():
                    self.server.stop(0)
                    break
                time.sleep(1)  # Every 1 second check if we should stop
        except KeyboardInterrupt:
            self.server.stop(0)

    def serveAPIs(self):
        """This starts a grpc server to allow access to the model whose eco.py file is located in
        the file system at 'modelPath' and listens on port 'port' with 'maxWorkers' workers."""
        add_ProducerServiceServicer_to_server(ProducerServiceImpl(self.eco), self.server)
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()
        self.serverStarted_event.set()

        # Wait for a stopEvent or a ctrl-c
        self.waitUntilStopped()

    def run(self):
        self.serveAPIs()

    def waitForServerToStart(self):
        self.serverStarted_event.wait()

    def signalServerToStop(self):
        self.stop_server_event.set()


if __name__ == "__main__":
    modelLoader: RepositorywithCICD = GitHubCICD("github")
    modelFolder: str = sys.argv[1]
    portStr: str = sys.argv[2]

    # Parse port to be an int
    port = int(portStr)

    eco: Optional[Ecosystem] = modelLoader.getEcosystem(modelFolder)
    if eco is None:
        raise Exception("Ecosystem not found")
    server = DataSurfaceServer(eco, port, 10)
    server.start()
    server.waitForServerToStart()
    server.join()
