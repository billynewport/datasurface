package datasurface;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.62.2)",
    comments = "Source: api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ProducerServiceGrpc {

  private ProducerServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "datasurface.ProducerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<datasurface.Api.DataStoreRequest,
      datasurface.Api.Datastore> getGetDatastoreMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "getDatastore",
      requestType = datasurface.Api.DataStoreRequest.class,
      responseType = datasurface.Api.Datastore.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<datasurface.Api.DataStoreRequest,
      datasurface.Api.Datastore> getGetDatastoreMethod() {
    io.grpc.MethodDescriptor<datasurface.Api.DataStoreRequest, datasurface.Api.Datastore> getGetDatastoreMethod;
    if ((getGetDatastoreMethod = ProducerServiceGrpc.getGetDatastoreMethod) == null) {
      synchronized (ProducerServiceGrpc.class) {
        if ((getGetDatastoreMethod = ProducerServiceGrpc.getGetDatastoreMethod) == null) {
          ProducerServiceGrpc.getGetDatastoreMethod = getGetDatastoreMethod =
              io.grpc.MethodDescriptor.<datasurface.Api.DataStoreRequest, datasurface.Api.Datastore>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "getDatastore"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  datasurface.Api.DataStoreRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  datasurface.Api.Datastore.getDefaultInstance()))
              .setSchemaDescriptor(new ProducerServiceMethodDescriptorSupplier("getDatastore"))
              .build();
        }
      }
    }
    return getGetDatastoreMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ProducerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProducerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProducerServiceStub>() {
        @java.lang.Override
        public ProducerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProducerServiceStub(channel, callOptions);
        }
      };
    return ProducerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ProducerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProducerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProducerServiceBlockingStub>() {
        @java.lang.Override
        public ProducerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProducerServiceBlockingStub(channel, callOptions);
        }
      };
    return ProducerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ProducerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProducerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProducerServiceFutureStub>() {
        @java.lang.Override
        public ProducerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProducerServiceFutureStub(channel, callOptions);
        }
      };
    return ProducerServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void getDatastore(datasurface.Api.DataStoreRequest request,
        io.grpc.stub.StreamObserver<datasurface.Api.Datastore> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetDatastoreMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ProducerService.
   */
  public static abstract class ProducerServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ProducerServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ProducerService.
   */
  public static final class ProducerServiceStub
      extends io.grpc.stub.AbstractAsyncStub<ProducerServiceStub> {
    private ProducerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProducerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProducerServiceStub(channel, callOptions);
    }

    /**
     */
    public void getDatastore(datasurface.Api.DataStoreRequest request,
        io.grpc.stub.StreamObserver<datasurface.Api.Datastore> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetDatastoreMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ProducerService.
   */
  public static final class ProducerServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ProducerServiceBlockingStub> {
    private ProducerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProducerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProducerServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public datasurface.Api.Datastore getDatastore(datasurface.Api.DataStoreRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetDatastoreMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ProducerService.
   */
  public static final class ProducerServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ProducerServiceFutureStub> {
    private ProducerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProducerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProducerServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<datasurface.Api.Datastore> getDatastore(
        datasurface.Api.DataStoreRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetDatastoreMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_DATASTORE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_DATASTORE:
          serviceImpl.getDatastore((datasurface.Api.DataStoreRequest) request,
              (io.grpc.stub.StreamObserver<datasurface.Api.Datastore>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getGetDatastoreMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              datasurface.Api.DataStoreRequest,
              datasurface.Api.Datastore>(
                service, METHODID_GET_DATASTORE)))
        .build();
  }

  private static abstract class ProducerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ProducerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return datasurface.Api.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ProducerService");
    }
  }

  private static final class ProducerServiceFileDescriptorSupplier
      extends ProducerServiceBaseDescriptorSupplier {
    ProducerServiceFileDescriptorSupplier() {}
  }

  private static final class ProducerServiceMethodDescriptorSupplier
      extends ProducerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ProducerServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ProducerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ProducerServiceFileDescriptorSupplier())
              .addMethod(getGetDatastoreMethod())
              .build();
        }
      }
    }
    return result;
  }
}
