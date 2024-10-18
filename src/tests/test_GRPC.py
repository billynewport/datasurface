"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


from typing import Optional, cast
import unittest
from datasurface.api.api import DataSurfaceServer
import grpc
from datasurface.api.api_pb2_grpc import ProducerServiceStub
from datasurface.api.api_pb2 import DataStoreRequest, Datastore as DatastoreProto, Dataset as DatasetProto, \
    DDLSchema as DDLSchemaProto, DDLColumn as DDLColumnProto
from datasurface.handler.action import GitHubCICD
from datasurface.handler.cicd import RepositorywithCICD
from datasurface.md import Schema, DDLTable, NullableStatus
from datasurface.md.Governance import Datastore, Ecosystem


class Test_GRPC(unittest.TestCase):
    def test_GRPC_step4_model(self):
        """Start a GRPC server on a different thread and then test it with a
        client on the this thread. When the tests are finished, stop the GRPC server"""

        port: int = 50051
        # Start the GRPC server in a different thread
        modelLoader: RepositorywithCICD = GitHubCICD("github")
        eco: Optional[Ecosystem] = modelLoader.getEcosystem("src/tests/actionHandlerResources/step4")
        assert eco is not None

        server: DataSurfaceServer = DataSurfaceServer(eco, port, 10)
        server.start()

        # Start the DataSurface server on a different thread

        # Wait for the server to start, the ecosystem model has to be loaded and this
        # takes time so wait for the signal that the server has started
        server.waitForServerToStart()

        # With the server started, now Start the client and make a call
        # Now test the server

        try:
            channel = grpc.insecure_channel(f"localhost:{port}")
            client: ProducerServiceStub = ProducerServiceStub(channel)
            origStore: Datastore = eco.cache_getDatastoreOrThrow("USA_Customers").datastore

            # Grab the Datastore from the API and verify that all tables and columns are correct using the original Datastore as a reference
            response: DatastoreProto = cast(DatastoreProto, client.getDatastore(DataStoreRequest(dataStoreName=origStore.name), timeout=10))  # type: ignore
            self.assertEqual(response.name, "USA_Customers")
            self.assertEqual(response.teamName, "NYTeam")
            self.assertEqual(len(response.datasets), len(origStore.datasets))
            protoDatasets: dict[str, DatasetProto] = {dataset.name: dataset for dataset in response.datasets}

            for origDataset in origStore.datasets.values():
                customersDataset: DatasetProto = protoDatasets[origDataset.name]
                self.assertEqual(customersDataset.name, origDataset.name)
                self.assertEqual(customersDataset.datastoreName, origStore.name)

                origSchema: Optional[Schema] = origDataset.originalSchema
                assert origSchema is not None
                assert isinstance(origSchema, DDLTable)

                # Check ingestion partition keys match
                if (origSchema.ingestionPartitionColumns is not None):
                    self.assertEqual(len(customersDataset.schema.ingestionPartitionKeys), len(origSchema.ingestionPartitionColumns.colNames))
                    for colName in customersDataset.schema.ingestionPartitionKeys:
                        self.assertTrue(colName in origSchema.ingestionPartitionColumns.colNames)
                else:
                    self.assertEqual(len(customersDataset.schema.ingestionPartitionKeys), 0)

                # Check primary keys match
                assert origSchema.primaryKeyColumns is not None
                self.assertEqual(len(customersDataset.schema.primaryKeys), len(origSchema.primaryKeyColumns.colNames))
                for colName in customersDataset.schema.primaryKeys:
                    self.assertTrue(colName in origSchema.primaryKeyColumns.colNames)

                # Check columns match
                custSchema: DDLSchemaProto = customersDataset.schema.ddlSchema
                self.assertEqual(len(custSchema.columns), len(origSchema.columns))
                protoColumns: dict[str, DDLColumnProto] = {column.name: column for column in custSchema.columns}
                for origColumn in origSchema.columns.values():
                    custColumn = protoColumns[origColumn.name]
                    self.assertEqual(custColumn.name, origColumn.name)
                    self.assertEqual(custColumn.type, origColumn.type.to_json())
                    self.assertEqual(custColumn.isNullable, origColumn.nullable == NullableStatus.NULLABLE)

        finally:
            # Stop the server
            server.signalServerToStop()
            server.join()
