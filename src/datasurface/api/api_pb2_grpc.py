# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from datasurface.api import api_pb2 as datasurface_dot_api_dot_api__pb2


class ProducerServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.getDatastore = channel.unary_unary(
                '/datasurface.ProducerService/getDatastore',
                request_serializer=datasurface_dot_api_dot_api__pb2.DataStoreRequest.SerializeToString,
                response_deserializer=datasurface_dot_api_dot_api__pb2.Datastore.FromString,
                )


class ProducerServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def getDatastore(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ProducerServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'getDatastore': grpc.unary_unary_rpc_method_handler(
                    servicer.getDatastore,
                    request_deserializer=datasurface_dot_api_dot_api__pb2.DataStoreRequest.FromString,
                    response_serializer=datasurface_dot_api_dot_api__pb2.Datastore.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'datasurface.ProducerService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ProducerService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def getDatastore(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/datasurface.ProducerService/getDatastore',
            datasurface_dot_api_dot_api__pb2.DataStoreRequest.SerializeToString,
            datasurface_dot_api_dot_api__pb2.Datastore.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
