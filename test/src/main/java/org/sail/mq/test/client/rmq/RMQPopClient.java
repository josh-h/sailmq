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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.sail.mq.test.client.rmq;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.sail.mq.client.ClientConfig;
import org.sail.mq.client.consumer.AckCallback;
import org.sail.mq.client.consumer.AckResult;
import org.sail.mq.client.consumer.PopCallback;
import org.sail.mq.client.consumer.PopResult;
import org.sail.mq.client.impl.ClientRemotingProcessor;
import org.sail.mq.client.impl.mqclient.MQClientAPIExt;
import org.sail.mq.common.message.MessageQueue;
import org.sail.mq.remoting.netty.NettyClientConfig;
import org.sail.mq.remoting.protocol.header.AckMessageRequestHeader;
import org.sail.mq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.sail.mq.remoting.protocol.header.ExtraInfoUtil;
import org.sail.mq.remoting.protocol.header.NotificationRequestHeader;
import org.sail.mq.remoting.protocol.header.PopMessageRequestHeader;
import org.sail.mq.test.clientinterface.MQConsumer;
import org.sail.mq.test.util.RandomUtil;

public class RMQPopClient implements MQConsumer {

    private static final long DEFAULT_TIMEOUT = 3000;

    private MQClientAPIExt mqClientAPI;

    @Override
    public void create() {
        create(false);
    }

    @Override
    public void create(boolean useTLS) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setInstanceName(RandomUtil.getStringByUUID());

        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyClientConfig.setUseTLS(useTLS);
        this.mqClientAPI = new MQClientAPIExt(
            clientConfig, nettyClientConfig, new ClientRemotingProcessor(null), null);
    }

    @Override
    public void start() {
        this.mqClientAPI.start();
    }

    @Override
    public void shutdown() {
        this.mqClientAPI.shutdown();
    }

    public CompletableFuture<PopResult> popMessageAsync(String brokerAddr, MessageQueue mq, long invisibleTime,
        int maxNums, String consumerGroup, long timeout, boolean poll, int initMode, boolean order,
        String expressionType, String expression) {
        return popMessageAsync(brokerAddr, mq, invisibleTime, maxNums, consumerGroup, timeout, poll, initMode, order, expressionType, expression, null);
    }

    public CompletableFuture<PopResult> popMessageAsync(String brokerAddr, MessageQueue mq, long invisibleTime,
        int maxNums, String consumerGroup, long timeout, boolean poll, int initMode, boolean order,
        String expressionType, String expression, String attemptId) {
        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(mq.getTopic());
        requestHeader.setQueueId(mq.getQueueId());
        requestHeader.setMaxMsgNums(maxNums);
        requestHeader.setInvisibleTime(invisibleTime);
        requestHeader.setInitMode(initMode);
        requestHeader.setExpType(expressionType);
        requestHeader.setExp(expression);
        requestHeader.setOrder(order);
        requestHeader.setAttemptId(attemptId);
        if (poll) {
            requestHeader.setPollTime(timeout);
            requestHeader.setBornTime(System.currentTimeMillis());
            timeout += 10 * 1000;
        }
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.popMessageAsync(mq.getBrokerName(), brokerAddr, requestHeader, timeout, new PopCallback() {
                @Override
                public void onSuccess(PopResult popResult) {
                    future.complete(popResult);
                }

                @Override
                public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckResult> ackMessageAsync(
        String brokerAddr, String topic, String consumerGroup, String extraInfo) {

        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
        requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
        requestHeader.setQueueId(ExtraInfoUtil.getQueueId(extraInfoStrs));
        requestHeader.setOffset(ExtraInfoUtil.getQueueOffset(extraInfoStrs));
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setExtraInfo(extraInfo);
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.ackMessageAsync(brokerAddr, DEFAULT_TIMEOUT, new AckCallback() {
                @Override
                public void onSuccess(AckResult ackResult) {
                    future.complete(ackResult);
                }

                @Override
                public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            }, requestHeader);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckResult> batchAckMessageAsync(String brokerAddr, String topic, String consumerGroup,
        List<String> extraInfoList) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.batchAckMessageAsync(brokerAddr, DEFAULT_TIMEOUT, new AckCallback() {
                @Override
                public void onSuccess(AckResult ackResult) {
                    future.complete(ackResult);
                }

                @Override
                public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            }, topic, consumerGroup, extraInfoList);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckResult> changeInvisibleTimeAsync(String brokerAddr, String brokerName, String topic,
        String consumerGroup, String extraInfo, long invisibleTime) {
        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
        requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
        requestHeader.setQueueId(ExtraInfoUtil.getQueueId(extraInfoStrs));
        requestHeader.setOffset(ExtraInfoUtil.getQueueOffset(extraInfoStrs));
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setExtraInfo(extraInfo);
        requestHeader.setInvisibleTime(invisibleTime);

        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.changeInvisibleTimeAsync(brokerName, brokerAddr, requestHeader, DEFAULT_TIMEOUT, new AckCallback() {
                @Override
                public void onSuccess(AckResult ackResult) {
                    future.complete(ackResult);
                }

                @Override
                public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<Boolean> notification(String brokerAddr, String topic,
        String consumerGroup, int queueId, long pollTime, long bornTime, long timeoutMillis) {
        return notification(brokerAddr, topic, consumerGroup, queueId, null, null, pollTime, bornTime, timeoutMillis);
    }

    public CompletableFuture<Boolean> notification(String brokerAddr, String topic,
        String consumerGroup, int queueId, Boolean order, String attemptId, long pollTime, long bornTime, long timeoutMillis) {
        NotificationRequestHeader requestHeader = new NotificationRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setPollTime(pollTime);
        requestHeader.setBornTime(bornTime);
        requestHeader.setOrder(order);
        requestHeader.setAttemptId(attemptId);
        return this.mqClientAPI.notification(brokerAddr, requestHeader, timeoutMillis);
    }
}
