// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.dmsingestor;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import datasurface.Api.DataStoreRequest;
import datasurface.Api.Datastore;
import datasurface.ProducerServiceGrpc;

public class ProducerServiceClient {
    private final ProducerServiceGrpc.ProducerServiceBlockingStub blockingStub;

    public ProducerServiceClient(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();

        blockingStub = ProducerServiceGrpc.newBlockingStub(channel);
    }

    public Datastore getDatastore(DataStoreRequest request) {
        return blockingStub.getDatastore(request);
    }
}
