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
import io.netty.channel.FileRegion;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.sail.mq.broker.BrokerController;
import org.sail.mq.broker.longpolling.PullRequest;
import org.sail.mq.broker.metrics.BrokerMetricsManager;
import org.sail.mq.broker.pagecache.ManyMessageTransfer;
import org.sail.mq.broker.plugin.PullMessageResultHandler;
import org.sail.mq.common.AbortProcessException;
import org.sail.mq.common.MixAll;
import org.sail.mq.common.TopicConfig;
import org.sail.mq.common.TopicFilterType;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.common.message.MessageDecoder;
import org.sail.mq.common.message.MessageExtBrokerInner;
import org.sail.mq.common.message.MessageQueue;
import org.sail.mq.common.sysflag.MessageSysFlag;
import org.sail.mq.common.sysflag.PullSysFlag;
import org.sail.mq.common.topic.TopicValidator;
import org.sail.mq.common.utils.NetworkUtil;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.remoting.common.RemotingHelper;
import org.sail.mq.remoting.metrics.RemotingMetricsManager;
import org.sail.mq.remoting.protocol.RemotingCommand;
import org.sail.mq.remoting.protocol.ResponseCode;
import org.sail.mq.remoting.protocol.header.PullMessageRequestHeader;
import org.sail.mq.remoting.protocol.header.PullMessageResponseHeader;
import org.sail.mq.remoting.protocol.heartbeat.SubscriptionData;
import org.sail.mq.remoting.protocol.statictopic.TopicQueueMappingContext;
import org.sail.mq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.sail.mq.remoting.protocol.topic.OffsetMovedEvent;
import org.sail.mq.store.GetMessageResult;
import org.sail.mq.store.MessageFilter;
import org.sail.mq.store.PutMessageResult;
import org.sail.mq.store.config.BrokerRole;

import static org.sail.mq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.sail.mq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.sail.mq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.sail.mq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.sail.mq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.sail.mq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

public class DefaultPullMessageResultHandler implements PullMessageResultHandler {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected final BrokerController brokerController;

    public DefaultPullMessageResultHandler(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand handle(final GetMessageResult getMessageResult,
        final RemotingCommand request,
        final PullMessageRequestHeader requestHeader,
        final Channel channel,
        final SubscriptionData subscriptionData,
        final SubscriptionGroupConfig subscriptionGroupConfig,
        final boolean brokerAllowSuspend,
        final MessageFilter messageFilter,
        RemotingCommand response,
        TopicQueueMappingContext mappingContext,
        long beginTimeMills) {
        PullMessageProcessor processor = brokerController.getPullMessageProcessor();
        final String clientAddress = RemotingHelper.parseChannelRemoteAddr(channel);
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        processor.composeResponseHeader(requestHeader, getMessageResult, topicConfig.getTopicSysFlag(),
            subscriptionGroupConfig, response, clientAddress);
        try {
            processor.executeConsumeMessageHookBefore(request, requestHeader, getMessageResult, brokerAllowSuspend, response.getCode());
        } catch (AbortProcessException e) {
            response.setCode(e.getResponseCode());
            response.setRemark(e.getErrorMessage());
            return response;
        }

        //rewrite the response for the static topic
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        RemotingCommand rewriteResult = processor.rewriteResponseForStaticTopic(requestHeader, responseHeader, mappingContext, response.getCode());
        if (rewriteResult != null) {
            response = rewriteResult;
        }

        processor.updateBroadcastPulledOffset(requestHeader.getTopic(), requestHeader.getConsumerGroup(),
            requestHeader.getQueueId(), requestHeader, channel, response, getMessageResult.getNextBeginOffset());
        processor.tryCommitOffset(brokerAllowSuspend, requestHeader, getMessageResult.getNextBeginOffset(),
            clientAddress);

        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                    getMessageResult.getMessageCount());

                this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                    getMessageResult.getBufferTotalSize());

                this.brokerController.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), getMessageResult.getMessageCount());

                if (!BrokerMetricsManager.isRetryOrDlqTopic(requestHeader.getTopic())) {
                    Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                        .put(LABEL_TOPIC, requestHeader.getTopic())
                        .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MixAll.isSysConsumerGroup(requestHeader.getConsumerGroup()))
                        .build();
                    BrokerMetricsManager.messagesOutTotal.add(getMessageResult.getMessageCount(), attributes);
                    BrokerMetricsManager.throughputOutTotal.add(getMessageResult.getBufferTotalSize(), attributes);
                }

                if (!channelIsWritable(channel, requestHeader)) {
                    getMessageResult.release();
                    //ignore pull request
                    return null;
                }

                if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                    final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
                    this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getQueueId(),
                        (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                    response.setBody(r);
                    return response;
                } else {
                    try {
                        FileRegion fileRegion =
                            new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                        RemotingCommand finalResponse = response;
                        channel.writeAndFlush(fileRegion)
                            .addListener((ChannelFutureListener) future -> {
                                getMessageResult.release();
                                Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                                    .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                                    .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(finalResponse.getCode()))
                                    .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                                    .build();
                                RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                if (!future.isSuccess()) {
                                    log.error("Fail to transfer messages from page cache to {}", channel.remoteAddress(), future.cause());
                                }
                            });
                    } catch (Throwable e) {
                        log.error("Error occurred when transferring messages from page cache", e);
                        getMessageResult.release();
                    }
                    return null;
                }
            case ResponseCode.PULL_NOT_FOUND:
                final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
                final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

                if (brokerAllowSuspend && hasSuspendFlag) {
                    long pollingTimeMills = suspendTimeoutMillisLong;
                    if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                        pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                    }

                    String topic = requestHeader.getTopic();
                    long offset = requestHeader.getQueueOffset();
                    int queueId = requestHeader.getQueueId();
                    PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills,
                        this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);
                    this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);
                    return null;
                }
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                if (this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
                    || this.brokerController.getMessageStoreConfig().isOffsetCheckInSlave()) {
                    MessageQueue mq = new MessageQueue();
                    mq.setTopic(requestHeader.getTopic());
                    mq.setQueueId(requestHeader.getQueueId());
                    mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

                    OffsetMovedEvent event = new OffsetMovedEvent();
                    event.setConsumerGroup(requestHeader.getConsumerGroup());
                    event.setMessageQueue(mq);
                    event.setOffsetRequest(requestHeader.getQueueOffset());
                    event.setOffsetNew(getMessageResult.getNextBeginOffset());
                    log.warn(
                        "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, suggestBrokerId={}",
                        requestHeader.getTopic(), requestHeader.getConsumerGroup(), event.getOffsetRequest(), event.getOffsetNew(),
                        responseHeader.getSuggestWhichBrokerId());
                } else {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    log.warn("PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, requestOffset={}, suggestBrokerId={}",
                        requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueOffset(),
                        responseHeader.getSuggestWhichBrokerId());
                }

                break;
            default:
                log.warn("[BUG] impossible result code of get message: {}", response.getCode());
                assert false;
        }

        return response;
    }

    private boolean channelIsWritable(Channel channel, PullMessageRequestHeader requestHeader) {
        if (this.brokerController.getBrokerConfig().isEnableNetWorkFlowControl()) {
            if (!channel.isWritable()) {
                log.warn("channel {} not writable ,cid {}", channel.remoteAddress(), requestHeader.getConsumerGroup());
                return false;
            }

        }
        return true;
    }

    protected byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group,
        final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                int sysFlag = bb.getInt(MessageDecoder.SYSFLAG_POSITION);
//                bornhost has the IPv4 ip if the MessageSysFlag.BORNHOST_V6_FLAG bit of sysFlag is 0
//                IPv4 host = ip(4 byte) + port(4 byte); IPv6 host = ip(16 byte) + port(4 byte)
                int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                int msgStoreTimePos = 4 // 1 TOTALSIZE
                    + 4 // 2 MAGICCODE
                    + 4 // 3 BODYCRC
                    + 4 // 4 QUEUEID
                    + 4 // 5 FLAG
                    + 8 // 6 QUEUEOFFSET
                    + 8 // 7 PHYSICALOFFSET
                    + 4 // 8 SYSFLAG
                    + 8 // 9 BORNTIMESTAMP
                    + bornhostLength; // 10 BORNHOST
                storeTimestamp = bb.getLong(msgStoreTimePos);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    protected void generateOffsetMovedEvent(final OffsetMovedEvent event) {
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT);
            msgInner.setTags(event.getConsumerGroup());
            msgInner.setDelayTimeLevel(0);
            msgInner.setKeys(event.getConsumerGroup());
            msgInner.setBody(event.encode());
            msgInner.setFlag(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG, msgInner.getTags()));

            msgInner.setQueueId(0);
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(NetworkUtil.string2SocketAddress(this.brokerController.getBrokerAddr()));
            msgInner.setStoreHost(msgInner.getBornHost());

            msgInner.setReconsumeTimes(0);

            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        } catch (Exception e) {
            log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        }
    }
}