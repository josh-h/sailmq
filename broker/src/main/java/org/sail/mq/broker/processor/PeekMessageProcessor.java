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
package org.sail.mq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.sail.mq.broker.BrokerController;
import org.sail.mq.broker.metrics.BrokerMetricsManager;
import org.sail.mq.broker.pagecache.ManyMessageTransfer;
import org.sail.mq.common.BrokerConfig;
import org.sail.mq.common.KeyBuilder;
import org.sail.mq.common.MixAll;
import org.sail.mq.common.TopicConfig;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.common.constant.PermName;
import org.sail.mq.common.help.FAQUrl;
import org.sail.mq.common.message.MessageDecoder;
import org.sail.mq.common.topic.TopicValidator;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.remoting.common.RemotingHelper;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.metrics.RemotingMetricsManager;
import org.sail.mq.remoting.netty.NettyRequestProcessor;
import org.sail.mq.remoting.protocol.RemotingCommand;
import org.sail.mq.remoting.protocol.ResponseCode;
import org.sail.mq.remoting.protocol.header.PeekMessageRequestHeader;
import org.sail.mq.remoting.protocol.header.PopMessageResponseHeader;
import org.sail.mq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.sail.mq.store.GetMessageResult;
import org.sail.mq.store.GetMessageStatus;
import org.sail.mq.store.SelectMappedBufferResult;
import org.sail.mq.store.exception.ConsumeQueueException;

import static org.sail.mq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.sail.mq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.sail.mq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.sail.mq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.sail.mq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.sail.mq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

public class PeekMessageProcessor implements NettyRequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private Random random = new Random(System.currentTimeMillis());

    public PeekMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend)
        throws RemotingCommandException {
        final long beginTimeMills = this.brokerController.getMessageStore().now();
        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
        final PeekMessageRequestHeader requestHeader =
            (PeekMessageRequestHeader) request.decodeCommandCustomHeader(PeekMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] peeking message is forbidden", this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            LOG.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return response;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            LOG.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }
        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }
        int randomQ = random.nextInt(100);
        int reviveQid = randomQ % this.brokerController.getBrokerConfig().getReviveQueueNum();
        GetMessageResult getMessageResult = new GetMessageResult(requestHeader.getMaxMsgNums());
        boolean needRetry = randomQ % 5 == 0;
        long popTime = System.currentTimeMillis();
        long restNum = 0;
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        if (needRetry) {
            TopicConfig retryTopicConfig = this.brokerController.getTopicConfigManager()
                .selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2()));
            if (retryTopicConfig != null) {
                for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
                    int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
                    restNum = peekMsgFromQueue(true, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime);
                }
            }
        }
        if (requestHeader.getQueueId() < 0) {
            // read all queue
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
                restNum = peekMsgFromQueue(false, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime);
            }
        } else {
            int queueId = requestHeader.getQueueId();
            restNum = peekMsgFromQueue(false, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime);
        }
        // if not full , fetch retry again
        if (!needRetry && getMessageResult.getMessageMapedList().size() < requestHeader.getMaxMsgNums()) {
            TopicConfig retryTopicConfig = this.brokerController.getTopicConfigManager()
                .selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2()));
            if (retryTopicConfig != null) {
                for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
                    int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
                    restNum = peekMsgFromQueue(true, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime);
                }
            }
        }
        if (!getMessageResult.getMessageBufferList().isEmpty()) {
            response.setCode(ResponseCode.SUCCESS);
            getMessageResult.setStatus(GetMessageStatus.FOUND);
        } else {
            response.setCode(ResponseCode.PULL_NOT_FOUND);
            getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);

        }
        responseHeader.setRestNum(restNum);
        response.setRemark(getMessageResult.getStatus().name());
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:

                this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                    getMessageResult.getMessageCount());

                this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                    getMessageResult.getBufferTotalSize());

                this.brokerController.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), getMessageResult.getMessageCount());

                if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                    final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
                    this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getQueueId(),
                        (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                    response.setBody(r);
                } else {
                    final GetMessageResult tmpGetMessageResult = getMessageResult;
                    try {
                        FileRegion fileRegion =
                            new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                        RemotingCommand finalResponse = response;
                        channel.writeAndFlush(fileRegion)
                            .addListener((ChannelFutureListener) future -> {
                                tmpGetMessageResult.release();
                                Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                                    .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                                    .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(finalResponse.getCode()))
                                    .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                                    .build();
                                RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                if (!future.isSuccess()) {
                                    LOG.error("Fail to transfer messages from page cache to {}", channel.remoteAddress(), future.cause());
                                }
                            });
                    } catch (Throwable e) {
                        LOG.error("Error occurred when transferring messages from page cache", e);
                        getMessageResult.release();
                    }

                    response = null;
                }
                break;
            default:
                assert false;
        }
        return response;
    }

    private long peekMsgFromQueue(boolean isRetry, GetMessageResult getMessageResult,
        PeekMessageRequestHeader requestHeader, int queueId, long restNum, int reviveQid, Channel channel,
        long popTime) throws RemotingCommandException {
        String topic = isRetry ?
            KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerController.getBrokerConfig().isEnableRetryTopicV2())
            : requestHeader.getTopic();
        GetMessageResult getMessageTmpResult;
        long offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId);
        try {
            restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
        } catch (ConsumeQueueException e) {
            LOG.error("Failed to get max offset in queue. topic={}, queue-id={}", topic, queueId, e);
            throw new RemotingCommandException("Failed to get max offset in queue", e);
        }
        if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
            return restNum;
        }
        getMessageTmpResult = this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), topic, queueId, offset,
            requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), null);
        // maybe store offset is not correct.
        if (GetMessageStatus.OFFSET_TOO_SMALL.equals(getMessageTmpResult.getStatus()) || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(getMessageTmpResult.getStatus())) {
            offset = getMessageTmpResult.getNextBeginOffset();
            getMessageTmpResult = this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), topic, queueId, offset,
                requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), null);
        }
        if (getMessageTmpResult != null) {
            if (!getMessageTmpResult.getMessageMapedList().isEmpty() && !isRetry) {
                Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                    .put(LABEL_TOPIC, requestHeader.getTopic())
                    .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
                    .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MixAll.isSysConsumerGroup(requestHeader.getConsumerGroup()))
                    .build();
                BrokerMetricsManager.messagesOutTotal.add(getMessageResult.getMessageCount(), attributes);
                BrokerMetricsManager.throughputOutTotal.add(getMessageResult.getBufferTotalSize(), attributes);
            }

            for (SelectMappedBufferResult mappedBuffer : getMessageTmpResult.getMessageMapedList()) {
                getMessageResult.addMessage(mappedBuffer);
            }
        }
        return restNum;
    }

    private long getPopOffset(String topic, String cid, int queueId) {
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(cid, topic, queueId);
        if (offset < 0) {
            offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
        }
        long bufferOffset = this.brokerController.getPopMessageProcessor().getPopBufferMergeService()
            .getLatestOffset(topic, cid, queueId);
        if (bufferOffset < 0) {
            return offset;
        } else {
            return bufferOffset > offset ? bufferOffset : offset;
        }
    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                storeTimestamp = bb.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

}
