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

package org.sail.mq.broker.failover;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.sail.mq.broker.BrokerController;
import org.sail.mq.broker.transaction.queue.TransactionalMessageUtil;
import org.sail.mq.client.consumer.PullStatus;
import org.sail.mq.client.exception.MQBrokerException;
import org.sail.mq.client.impl.producer.TopicPublishInfo;
import org.sail.mq.client.producer.SendResult;
import org.sail.mq.client.producer.SendStatus;
import org.sail.mq.common.MixAll;
import org.sail.mq.common.ThreadFactoryImpl;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.common.message.MessageConst;
import org.sail.mq.common.message.MessageDecoder;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.common.message.MessageExtBrokerInner;
import org.sail.mq.common.message.MessageQueue;
import org.sail.mq.common.utils.ThreadUtils;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.remoting.exception.RemotingException;
import org.sail.mq.store.GetMessageResult;
import org.sail.mq.store.GetMessageStatus;
import org.sail.mq.store.MessageStore;
import org.sail.mq.store.PutMessageResult;
import org.sail.mq.store.PutMessageStatus;
import org.sail.mq.tieredstore.TieredMessageStore;

public class EscapeBridge {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long SEND_TIMEOUT = 3000L;
    private static final long DEFAULT_PULL_TIMEOUT_MILLIS = 1000 * 10L;
    private final String innerProducerGroupName;
    private final String innerConsumerGroupName;

    private final BrokerController brokerController;

    private ExecutorService defaultAsyncSenderExecutor;

    public EscapeBridge(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.innerProducerGroupName = "InnerProducerGroup_" + brokerController.getBrokerConfig().getBrokerName() + "_" + brokerController.getBrokerConfig().getBrokerId();
        this.innerConsumerGroupName = "InnerConsumerGroup_" + brokerController.getBrokerConfig().getBrokerName() + "_" + brokerController.getBrokerConfig().getBrokerId();
    }

    public void start() throws Exception {
        if (brokerController.getBrokerConfig().isEnableSlaveActingMaster() && brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            final BlockingQueue<Runnable> asyncSenderThreadPoolQueue = new LinkedBlockingQueue<>(50000);
            this.defaultAsyncSenderExecutor = ThreadUtils.newThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                asyncSenderThreadPoolQueue,
                new ThreadFactoryImpl("AsyncEscapeBridgeExecutor_", this.brokerController.getBrokerIdentity())
            );
            LOG.info("init executor for escaping messages asynchronously success.");
        }
    }

    public void shutdown() {
        if (null != this.defaultAsyncSenderExecutor) {
            this.defaultAsyncSenderExecutor.shutdown();
        }
    }

    public PutMessageResult putMessage(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().putMessage(messageExt);
        } else if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {

            try {
                messageExt.setWaitStoreMsgOK(false);
                final SendResult sendResult = putMessageToRemoteBroker(messageExt, null);
                return transformSendResult2PutResult(sendResult);
            } catch (Exception e) {
                LOG.error("sendMessageInFailover to remote failed", e);
                return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
            }
        } else {
            LOG.warn("Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.brokerController.getBrokerConfig().isEnableSlaveActingMaster(), this.brokerController.getBrokerConfig().isEnableRemoteEscape());
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
    }

    public SendResult putMessageToRemoteBroker(MessageExtBrokerInner messageExt, String brokerNameToSend) {
        if (this.brokerController.getBrokerConfig().getBrokerName().equals(brokerNameToSend)) { // not remote broker
            return null;
        }
        final boolean isTransHalfMessage = TransactionalMessageUtil.buildHalfTopic().equals(messageExt.getTopic());
        MessageExtBrokerInner messageToPut = messageExt;
        if (isTransHalfMessage) {
            messageToPut = TransactionalMessageUtil.buildTransactionalMessageFromHalfMessage(messageExt);
        }
        final TopicPublishInfo topicPublishInfo = this.brokerController.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageToPut.getTopic());
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            LOG.warn("putMessageToRemoteBroker: no route info of topic {} when escaping message, msgId={}",
                messageToPut.getTopic(), messageToPut.getMsgId());
            return null;
        }

        final MessageQueue mqSelected;
        if (StringUtils.isEmpty(brokerNameToSend)) {
            mqSelected = topicPublishInfo.selectOneMessageQueue(this.brokerController.getBrokerConfig().getBrokerName());
            messageToPut.setQueueId(mqSelected.getQueueId());
            brokerNameToSend = mqSelected.getBrokerName();
            if (this.brokerController.getBrokerConfig().getBrokerName().equals(brokerNameToSend)) {
                LOG.warn("putMessageToRemoteBroker failed, remote broker not found. Topic: {}, MsgId: {}, Broker: {}",
                    messageExt.getTopic(), messageExt.getMsgId(), brokerNameToSend);
                return null;
            }
        } else {
            mqSelected = new MessageQueue(messageExt.getTopic(), brokerNameToSend, messageExt.getQueueId());
        }

        final String brokerAddrToSend = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
        if (null == brokerAddrToSend) {
            LOG.warn("putMessageToRemoteBroker failed, remote broker address not found. Topic: {}, MsgId: {}, Broker: {}",
                messageExt.getTopic(), messageExt.getMsgId(), brokerNameToSend);
            return null;
        }

        final long beginTimestamp = System.currentTimeMillis();
        try {
            final SendResult sendResult = this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBroker(
                brokerAddrToSend, brokerNameToSend,
                messageToPut, this.getProducerGroup(messageToPut), SEND_TIMEOUT);
            if (null != sendResult && SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                return sendResult;
            } else {
                LOG.error("Escaping failed! cost {}ms, Topic: {}, MsgId: {}, Broker: {}",
                    System.currentTimeMillis() - beginTimestamp, messageExt.getTopic(),
                    messageExt.getMsgId(), brokerNameToSend);
            }
        } catch (RemotingException | MQBrokerException e) {
            LOG.error(String.format("putMessageToRemoteBroker exception, MsgId: %s, RT: %sms, Broker: %s",
                messageToPut.getMsgId(), System.currentTimeMillis() - beginTimestamp, mqSelected), e);
        } catch (InterruptedException e) {
            LOG.error(String.format("putMessageToRemoteBroker interrupted, MsgId: %s, RT: %sms, Broker: %s",
                messageToPut.getMsgId(), System.currentTimeMillis() - beginTimestamp, mqSelected), e);
            Thread.currentThread().interrupt();
        }

        return null;
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().asyncPutMessage(messageExt);
        } else if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            try {
                messageExt.setWaitStoreMsgOK(false);

                final TopicPublishInfo topicPublishInfo = this.brokerController.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageExt.getTopic());
                final String producerGroup = getProducerGroup(messageExt);

                final MessageQueue mqSelected = topicPublishInfo.selectOneMessageQueue();
                messageExt.setQueueId(mqSelected.getQueueId());

                final String brokerNameToSend = mqSelected.getBrokerName();
                final String brokerAddrToSend = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
                final CompletableFuture<SendResult> future = this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBrokerAsync(brokerAddrToSend,
                    brokerNameToSend, messageExt,
                    producerGroup, SEND_TIMEOUT);

                return future.exceptionally(throwable -> null)
                    .thenApplyAsync(this::transformSendResult2PutResult, this.defaultAsyncSenderExecutor)
                    .exceptionally(throwable -> transformSendResult2PutResult(null));

            } catch (Exception e) {
                LOG.error("sendMessageInFailover to remote failed", e);
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true));
            }
        } else {
            LOG.warn("Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.brokerController.getBrokerConfig().isEnableSlaveActingMaster(), this.brokerController.getBrokerConfig().isEnableRemoteEscape());
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }
    }

    private String getProducerGroup(MessageExtBrokerInner messageExt) {
        if (null == messageExt) {
            return this.innerProducerGroupName;
        }
        String producerGroup = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = this.innerProducerGroupName;
        }
        return producerGroup;
    }

    public PutMessageResult putMessageToSpecificQueue(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().putMessage(messageExt);
        }
        try {
            return asyncRemotePutMessageToSpecificQueue(messageExt).get(SEND_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.error("Put message to specific queue error", e);
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null, true);
        }
    }

    public CompletableFuture<PutMessageResult> asyncPutMessageToSpecificQueue(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().asyncPutMessage(messageExt);
        }
        return asyncRemotePutMessageToSpecificQueue(messageExt);
    }

    public CompletableFuture<PutMessageResult> asyncRemotePutMessageToSpecificQueue(MessageExtBrokerInner messageExt) {
        if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            try {
                messageExt.setWaitStoreMsgOK(false);

                final TopicPublishInfo topicPublishInfo = this.brokerController.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageExt.getTopic());
                List<MessageQueue> mqs = topicPublishInfo.getMessageQueueList();

                if (null == mqs || mqs.isEmpty()) {
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true));
                }

                String id = messageExt.getTopic() + messageExt.getStoreHost();
                final int index = Math.floorMod(id.hashCode(), mqs.size());

                MessageQueue mq = mqs.get(index);
                messageExt.setQueueId(mq.getQueueId());

                String brokerNameToSend = mq.getBrokerName();
                String brokerAddrToSend = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
                return this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBrokerAsync(
                    brokerAddrToSend, brokerNameToSend,
                    messageExt, this.getProducerGroup(messageExt), SEND_TIMEOUT).thenCompose(sendResult -> CompletableFuture.completedFuture(transformSendResult2PutResult(sendResult)));
            } catch (Exception e) {
                LOG.error("sendMessageInFailover to remote failed", e);
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true));
            }
        } else {
            LOG.warn("Put message to specific queue failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.brokerController.getBrokerConfig().isEnableSlaveActingMaster(), this.brokerController.getBrokerConfig().isEnableRemoteEscape());
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }
    }

    private PutMessageResult transformSendResult2PutResult(SendResult sendResult) {
        if (sendResult == null) {
            return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
        switch (sendResult.getSendStatus()) {
            case SEND_OK:
                return new PutMessageResult(PutMessageStatus.PUT_OK, null, true);
            case SLAVE_NOT_AVAILABLE:
                return new PutMessageResult(PutMessageStatus.SLAVE_NOT_AVAILABLE, null, true);
            case FLUSH_DISK_TIMEOUT:
                return new PutMessageResult(PutMessageStatus.FLUSH_DISK_TIMEOUT, null, true);
            case FLUSH_SLAVE_TIMEOUT:
                return new PutMessageResult(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, null, true);
            default:
                return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
    }

    public Triple<MessageExt, String, Boolean> getMessage(String topic, long offset, int queueId, String brokerName,
        boolean deCompressBody) {
        return getMessageAsync(topic, offset, queueId, brokerName, deCompressBody).join();
    }

    // Triple<MessageExt, info, needRetry>, check info and retry if and only if MessageExt is null
    public CompletableFuture<Triple<MessageExt, String, Boolean>> getMessageAsync(String topic, long offset,
        int queueId, String brokerName, boolean deCompressBody) {
        MessageStore messageStore = brokerController.getMessageStoreByBrokerName(brokerName);
        if (messageStore != null) {
            return messageStore.getMessageAsync(innerConsumerGroupName, topic, queueId, offset, 1, null)
                .thenApply(result -> {
                    if (result == null) {
                        LOG.warn("getMessageResult is null , innerConsumerGroupName {}, topic {}, offset {}, queueId {}", innerConsumerGroupName, topic, offset, queueId);
                        return Triple.of(null, "getMessageResult is null", false); // local store, so no retry
                    }
                    List<MessageExt> list = decodeMsgList(result, deCompressBody);
                    if (list == null || list.isEmpty()) {
                        // OFFSET_FOUND_NULL returned by TieredMessageStore indicates exception occurred
                        boolean needRetry = GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
                            && messageStore instanceof TieredMessageStore;
                        LOG.warn("Can not get msg , topic {}, offset {}, queueId {}, needRetry {}, result is {}",
                            topic, offset, queueId, needRetry, result);
                        return Triple.of(null, "Can not get msg", needRetry);
                    }
                    return Triple.of(list.get(0), "", false);
                });
        } else {
            return getMessageFromRemoteAsync(topic, offset, queueId, brokerName);
        }
    }

    protected List<MessageExt> decodeMsgList(GetMessageResult getMessageResult, boolean deCompressBody) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList != null) {
                for (int i = 0; i < messageBufferList.size(); i++) {
                    ByteBuffer bb = messageBufferList.get(i);
                    if (bb == null) {
                        LOG.error("bb is null {}", getMessageResult);
                        continue;
                    }
                    MessageExt msgExt = MessageDecoder.decode(bb, true, deCompressBody);
                    if (msgExt == null) {
                        LOG.error("decode msgExt is null {}", getMessageResult);
                        continue;
                    }
                    // use CQ offset, not offset in Message
                    msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                    foundList.add(msgExt);
                }
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    protected Triple<MessageExt, String, Boolean> getMessageFromRemote(String topic, long offset, int queueId,
        String brokerName) {
        return getMessageFromRemoteAsync(topic, offset, queueId, brokerName).join();
    }

    // Triple<MessageExt, info, needRetry>, check info and retry if and only if MessageExt is null
    protected CompletableFuture<Triple<MessageExt, String, Boolean>> getMessageFromRemoteAsync(String topic,
        long offset, int queueId, String brokerName) {
        try {
            String brokerAddr = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, false);
            if (null == brokerAddr) {
                this.brokerController.getTopicRouteInfoManager().updateTopicRouteInfoFromNameServer(topic, true, false);
                brokerAddr = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, false);

                if (null == brokerAddr) {
                    LOG.warn("can't find broker address for topic {}, {}", topic, brokerName);
                    return CompletableFuture.completedFuture(Triple.of(null, "brokerAddress not found", true)); // maybe offline temporarily, so need retry
                }
            }

            return this.brokerController.getBrokerOuterAPI().pullMessageFromSpecificBrokerAsync(brokerName,
                    brokerAddr, this.innerConsumerGroupName, topic, queueId, offset, 1, DEFAULT_PULL_TIMEOUT_MILLIS)
                .thenApply(pullResult -> {
                    if (pullResult.getLeft() != null
                        && PullStatus.FOUND.equals(pullResult.getLeft().getPullStatus())
                        && CollectionUtils.isNotEmpty(pullResult.getLeft().getMsgFoundList())) {
                        return Triple.of(pullResult.getLeft().getMsgFoundList().get(0), "", false);
                    }
                    return Triple.of(null, pullResult.getMiddle(), pullResult.getRight());
                });
        } catch (Exception e) {
            LOG.error("Get message from remote failed. {}, {}, {}, {}", topic, offset, queueId, brokerName, e);
        }

        return CompletableFuture.completedFuture(Triple.of(null, "Get message from remote failed", true)); // need retry
    }
}