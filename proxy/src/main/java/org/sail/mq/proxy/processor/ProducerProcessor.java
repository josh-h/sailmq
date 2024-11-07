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
package org.sail.mq.proxy.processor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.sail.mq.client.producer.SendResult;
import org.sail.mq.client.producer.SendStatus;
import org.sail.mq.common.MixAll;
import org.sail.mq.common.attribute.TopicMessageType;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.common.consumer.ReceiptHandle;
import org.sail.mq.common.message.Message;
import org.sail.mq.common.message.MessageAccessor;
import org.sail.mq.common.message.MessageClientIDSetter;
import org.sail.mq.common.message.MessageConst;
import org.sail.mq.common.message.MessageDecoder;
import org.sail.mq.common.message.MessageId;
import org.sail.mq.common.sysflag.MessageSysFlag;
import org.sail.mq.common.topic.TopicValidator;
import org.sail.mq.common.utils.FutureUtils;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.common.ProxyException;
import org.sail.mq.proxy.common.ProxyExceptionCode;
import org.sail.mq.proxy.config.ConfigurationManager;
import org.sail.mq.proxy.processor.validator.DefaultTopicMessageTypeValidator;
import org.sail.mq.proxy.processor.validator.TopicMessageTypeValidator;
import org.sail.mq.proxy.service.ServiceManager;
import org.sail.mq.proxy.service.route.AddressableMessageQueue;
import org.sail.mq.remoting.protocol.NamespaceUtil;
import org.sail.mq.remoting.protocol.RemotingCommand;
import org.sail.mq.remoting.protocol.ResponseCode;
import org.sail.mq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.sail.mq.remoting.protocol.header.SendMessageRequestHeader;

public class ProducerProcessor extends AbstractProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final ExecutorService executor;
    private final TopicMessageTypeValidator topicMessageTypeValidator;

    public ProducerProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager, ExecutorService executor) {
        super(messagingProcessor, serviceManager);
        this.executor = executor;
        this.topicMessageTypeValidator = new DefaultTopicMessageTypeValidator();
    }

    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, QueueSelector queueSelector,
        String producerGroup, int sysFlag, List<Message> messageList, long timeoutMillis) {
        CompletableFuture<List<SendResult>> future = new CompletableFuture<>();
        long beginTimestampFirst = System.currentTimeMillis();
        AddressableMessageQueue messageQueue = null;
        try {
            Message message = messageList.get(0);
            String topic = message.getTopic();
            if (ConfigurationManager.getProxyConfig().isEnableTopicMessageTypeCheck()) {
                if (topicMessageTypeValidator != null) {
                    // Do not check retry or dlq topic
                    if (!NamespaceUtil.isRetryTopic(topic) && !NamespaceUtil.isDLQTopic(topic)) {
                        TopicMessageType topicMessageType = serviceManager.getMetadataService().getTopicMessageType(ctx, topic);
                        TopicMessageType messageType = TopicMessageType.parseFromMessageProperty(message.getProperties());
                        topicMessageTypeValidator.validate(topicMessageType, messageType);
                    }
                }
            }
            messageQueue = queueSelector.select(ctx,
                this.serviceManager.getTopicRouteService().getCurrentMessageQueueView(ctx, topic));
            if (messageQueue == null) {
                throw new ProxyException(ProxyExceptionCode.FORBIDDEN, "no writable queue");
            }

            for (Message msg : messageList) {
                MessageClientIDSetter.setUniqID(msg);
            }
            SendMessageRequestHeader requestHeader = buildSendMessageRequestHeader(messageList, producerGroup, sysFlag, messageQueue.getQueueId());

            AddressableMessageQueue finalMessageQueue = messageQueue;
            future = this.serviceManager.getMessageService().sendMessage(
                ctx,
                messageQueue,
                messageList,
                requestHeader,
                timeoutMillis)
                .thenApplyAsync(sendResultList -> {
                    for (SendResult sendResult : sendResultList) {
                        int tranType = MessageSysFlag.getTransactionValue(requestHeader.getSysFlag());
                        if (SendStatus.SEND_OK.equals(sendResult.getSendStatus()) &&
                            tranType == MessageSysFlag.TRANSACTION_PREPARED_TYPE &&
                            StringUtils.isNotBlank(sendResult.getTransactionId())) {
                            fillTransactionData(ctx, producerGroup, finalMessageQueue, sendResult, messageList);
                        }
                    }
                    return sendResultList;
                }, this.executor)
                    .whenComplete((result, exception) -> {
                        long endTimestamp = System.currentTimeMillis();
                        if (exception != null) {
                            this.serviceManager.getTopicRouteService().updateFaultItem(finalMessageQueue.getBrokerName(), endTimestamp - beginTimestampFirst, true, false);
                        } else {
                            this.serviceManager.getTopicRouteService().updateFaultItem(finalMessageQueue.getBrokerName(),endTimestamp - beginTimestampFirst, false, true);
                        }
                    });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    protected void fillTransactionData(ProxyContext ctx, String producerGroup, AddressableMessageQueue messageQueue, SendResult sendResult, List<Message> messageList) {
        try {
            MessageId id;
            if (sendResult.getOffsetMsgId() != null) {
                id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
            } else {
                id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
            }
            this.serviceManager.getTransactionService().addTransactionDataByBrokerName(
                ctx,
                messageQueue.getBrokerName(),
                messageList.get(0).getTopic(),
                producerGroup,
                sendResult.getQueueOffset(),
                id.getOffset(),
                sendResult.getTransactionId(),
                messageList.get(0)
            );
        } catch (Throwable t) {
            log.warn("fillTransactionData failed. messageQueue: {}, sendResult: {}", messageQueue, sendResult, t);
        }
    }

    protected SendMessageRequestHeader buildSendMessageRequestHeader(List<Message> messageList,
        String producerGroup, int sysFlag, int queueId) {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();

        Message message = messageList.get(0);

        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setTopic(message.getTopic());
        requestHeader.setDefaultTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);
        requestHeader.setDefaultTopicQueueNums(4);
        requestHeader.setQueueId(queueId);
        requestHeader.setSysFlag(sysFlag);
        /*
        In SailMQ 4.0, org.apache.sailmq.remoting.protocol.header.SendMessageRequestHeader.bornTimestamp
        represents the timestamp when the message was born. In SailMQ 5.0, the bornTimestamp of the message
        is a message attribute, that is, the timestamp when message was constructed, and there is no
        bornTimestamp in the SendMessageRequest of SailMQ 5.0.
        Note: When using grpc sendMessage to send multiple messages, the bornTimestamp in the requestHeader
        is set to the bornTimestamp of the first message, which may not be accurate. When a bornTimestamp is
        required, the bornTimestamp of the message property should be used.
        * */
        try {
            requestHeader.setBornTimestamp(Long.parseLong(message.getProperty(MessageConst.PROPERTY_BORN_TIMESTAMP)));
        } catch (Exception e) {
            log.warn("parse born time error, with value:{}", message.getProperty(MessageConst.PROPERTY_BORN_TIMESTAMP));
            requestHeader.setBornTimestamp(System.currentTimeMillis());
        }
        requestHeader.setFlag(message.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
        requestHeader.setReconsumeTimes(0);
        if (messageList.size() > 1) {
            requestHeader.setBatch(true);
        }
        if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            String reconsumeTimes = MessageAccessor.getReconsumeTime(message);
            if (reconsumeTimes != null) {
                requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                MessageAccessor.clearProperty(message, MessageConst.PROPERTY_RECONSUME_TIME);
            }

            String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(message);
            if (maxReconsumeTimes != null) {
                requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                MessageAccessor.clearProperty(message, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
            }
        }

        return requestHeader;
    }

    public CompletableFuture<RemotingCommand> forwardMessageToDeadLetterQueue(ProxyContext ctx, ReceiptHandle handle,
        String messageId, String groupName, String topicName, long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            if (handle.getCommitLogOffset() < 0) {
                throw new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "commit log offset is empty");
            }

            ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
            consumerSendMsgBackRequestHeader.setOffset(handle.getCommitLogOffset());
            consumerSendMsgBackRequestHeader.setGroup(groupName);
            consumerSendMsgBackRequestHeader.setDelayLevel(-1);
            consumerSendMsgBackRequestHeader.setOriginMsgId(messageId);
            consumerSendMsgBackRequestHeader.setOriginTopic(handle.getRealTopic(topicName, groupName));
            consumerSendMsgBackRequestHeader.setMaxReconsumeTimes(0);

            future = this.serviceManager.getMessageService().sendMessageBack(
                ctx,
                handle,
                messageId,
                consumerSendMsgBackRequestHeader,
                timeoutMillis
            ).whenCompleteAsync((remotingCommand, t) -> {
                if (t == null && remotingCommand.getCode() == ResponseCode.SUCCESS) {
                    this.messagingProcessor.ackMessage(ctx, handle, messageId,
                        groupName, topicName, timeoutMillis);
                }
            }, this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

}
