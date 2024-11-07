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
package org.sail.mq.proxy.service.message;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.sail.mq.client.consumer.AckResult;
import org.sail.mq.client.consumer.PopResult;
import org.sail.mq.client.consumer.PullResult;
import org.sail.mq.client.producer.SendResult;
import org.sail.mq.common.consumer.ReceiptHandle;
import org.sail.mq.common.message.Message;
import org.sail.mq.common.message.MessageQueue;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.service.route.AddressableMessageQueue;
import org.sail.mq.remoting.protocol.RemotingCommand;
import org.sail.mq.remoting.protocol.body.LockBatchRequestBody;
import org.sail.mq.remoting.protocol.body.UnlockBatchRequestBody;
import org.sail.mq.remoting.protocol.header.AckMessageRequestHeader;
import org.sail.mq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.sail.mq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.sail.mq.remoting.protocol.header.EndTransactionRequestHeader;
import org.sail.mq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.sail.mq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.sail.mq.remoting.protocol.header.PopMessageRequestHeader;
import org.sail.mq.remoting.protocol.header.PullMessageRequestHeader;
import org.sail.mq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.sail.mq.remoting.protocol.header.SendMessageRequestHeader;
import org.sail.mq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;

public interface MessageService {

    CompletableFuture<List<SendResult>> sendMessage(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        List<Message> msgList,
        SendMessageRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<RemotingCommand> sendMessageBack(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        ConsumerSendMsgBackRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<Void> endTransactionOneway(
        ProxyContext ctx,
        String brokerName,
        EndTransactionRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<PopResult> popMessage(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        PopMessageRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<AckResult> changeInvisibleTime(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        ChangeInvisibleTimeRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<AckResult> ackMessage(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        AckMessageRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<AckResult> batchAckMessage(
        ProxyContext ctx,
        List<ReceiptHandleMessage> handleList,
        String consumerGroup,
        String topic,
        long timeoutMillis
    );

    CompletableFuture<PullResult> pullMessage(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        PullMessageRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<Long> queryConsumerOffset(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        QueryConsumerOffsetRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<Void> updateConsumerOffset(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        UpdateConsumerOffsetRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<Void> updateConsumerOffsetAsync(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        UpdateConsumerOffsetRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<Set<MessageQueue>> lockBatchMQ(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        LockBatchRequestBody requestBody,
        long timeoutMillis
    );

    CompletableFuture<Void> unlockBatchMQ(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        UnlockBatchRequestBody requestBody,
        long timeoutMillis
    );

    CompletableFuture<Long> getMaxOffset(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        GetMaxOffsetRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<Long> getMinOffset(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        GetMinOffsetRequestHeader requestHeader,
        long timeoutMillis
    );

    CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis);

    CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis);
}
