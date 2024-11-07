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
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.common.utils.AbstractStartAndShutdown;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.sail.mq.proxy.grpc.v2.client.ClientActivity;
import org.sail.mq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.sail.mq.proxy.grpc.v2.consumer.AckMessageActivity;
import org.sail.mq.proxy.grpc.v2.consumer.ChangeInvisibleDurationActivity;
import org.sail.mq.proxy.grpc.v2.consumer.ReceiveMessageActivity;
import org.sail.mq.proxy.grpc.v2.producer.ForwardMessageToDLQActivity;
import org.sail.mq.proxy.grpc.v2.producer.SendMessageActivity;
import org.sail.mq.proxy.grpc.v2.route.RouteActivity;
import org.sail.mq.proxy.grpc.v2.transaction.EndTransactionActivity;
import org.sail.mq.proxy.processor.MessagingProcessor;

public class DefaultGrpcMessingActivity extends AbstractStartAndShutdown implements GrpcMessingActivity {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected GrpcClientSettingsManager grpcClientSettingsManager;
    protected GrpcChannelManager grpcChannelManager;
    protected ReceiveMessageActivity receiveMessageActivity;
    protected AckMessageActivity ackMessageActivity;
    protected ChangeInvisibleDurationActivity changeInvisibleDurationActivity;
    protected SendMessageActivity sendMessageActivity;
    protected ForwardMessageToDLQActivity forwardMessageToDLQActivity;
    protected EndTransactionActivity endTransactionActivity;
    protected RouteActivity routeActivity;
    protected ClientActivity clientActivity;

    protected DefaultGrpcMessingActivity(MessagingProcessor messagingProcessor) {
        this.init(messagingProcessor);
    }

    protected void init(MessagingProcessor messagingProcessor) {
        this.grpcClientSettingsManager = new GrpcClientSettingsManager(messagingProcessor);
        this.grpcChannelManager = new GrpcChannelManager(messagingProcessor.getProxyRelayService(), this.grpcClientSettingsManager);

        this.receiveMessageActivity = new ReceiveMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.ackMessageActivity = new AckMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.changeInvisibleDurationActivity = new ChangeInvisibleDurationActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.sendMessageActivity = new SendMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.forwardMessageToDLQActivity = new ForwardMessageToDLQActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.endTransactionActivity = new EndTransactionActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.routeActivity = new RouteActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.clientActivity = new ClientActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);

        this.appendStartAndShutdown(this.grpcClientSettingsManager);
    }

    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        return this.routeActivity.queryRoute(ctx, request);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request) {
        return this.clientActivity.heartbeat(ctx, request);
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request) {
        return this.sendMessageActivity.sendMessage(ctx, request);
    }

    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request) {
        return this.routeActivity.queryAssignment(ctx, request);
    }

    @Override
    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        this.receiveMessageActivity.receiveMessage(ctx, request, responseObserver);
    }

    @Override
    public CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request) {
        return this.ackMessageActivity.ackMessage(ctx, request);
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(ProxyContext ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return this.forwardMessageToDLQActivity.forwardMessageToDeadLetterQueue(ctx, request);
    }

    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx, EndTransactionRequest request) {
        return this.endTransactionActivity.endTransaction(ctx, request);
    }

    @Override
    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request) {
        return this.clientActivity.notifyClientTermination(ctx, request);
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request) {
        return this.changeInvisibleDurationActivity.changeInvisibleDuration(ctx, request);
    }

    @Override
    public ContextStreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        return this.clientActivity.telemetry(responseObserver);
    }
}
