/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sail.mq.proxy.grpc.v2;

import apache.sailmq.v2.AckMessageRequest;
import apache.sailmq.v2.AckMessageResponse;
import apache.sailmq.v2.ChangeInvisibleDurationRequest;
import apache.sailmq.v2.ChangeInvisibleDurationResponse;
import apache.sailmq.v2.EndTransactionRequest;
import apache.sailmq.v2.EndTransactionResponse;
import apache.sailmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.sailmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.sailmq.v2.HeartbeatRequest;
import apache.sailmq.v2.HeartbeatResponse;
import apache.sailmq.v2.NotifyClientTerminationRequest;
import apache.sailmq.v2.NotifyClientTerminationResponse;
import apache.sailmq.v2.QueryAssignmentRequest;
import apache.sailmq.v2.QueryAssignmentResponse;
import apache.sailmq.v2.QueryRouteRequest;
import apache.sailmq.v2.QueryRouteResponse;
import apache.sailmq.v2.ReceiveMessageRequest;
import apache.sailmq.v2.ReceiveMessageResponse;
import apache.sailmq.v2.SendMessageRequest;
import apache.sailmq.v2.SendMessageResponse;
import apache.sailmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.common.utils.StartAndShutdown;

public interface GrpcMessingActivity extends StartAndShutdown {

    CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request);

    CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request);

    CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request);

    CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx, QueryAssignmentRequest request);

    void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver);

    CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request);

    CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(ProxyContext ctx,
        ForwardMessageToDeadLetterQueueRequest request);

    CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx, EndTransactionRequest request);

    CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request);

    CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request);

    ContextStreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver);
}
