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
package io.openmessaging.sailmq.consumer;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.PullConsumer;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.sailmq.config.ClientConfig;
import io.openmessaging.sailmq.domain.ConsumeRequest;
import io.openmessaging.sailmq.utils.BeanUtils;
import io.openmessaging.sailmq.utils.OMSUtil;
import org.sail.mq.client.consumer.DefaultMQPullConsumer;
import org.sail.mq.client.consumer.MQPullConsumer;
import org.sail.mq.client.consumer.MQPullConsumerScheduleService;
import org.sail.mq.client.consumer.PullResult;
import org.sail.mq.client.consumer.PullTaskCallback;
import org.sail.mq.client.consumer.PullTaskContext;
import org.sail.mq.client.exception.MQClientException;
import org.sail.mq.client.impl.consumer.ProcessQueue;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.common.message.MessageQueue;
import org.sail.mq.remoting.protocol.LanguageCode;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;

public class PullConsumerImpl implements PullConsumer {
    private static final Logger log = LoggerFactory.getLogger(PullConsumerImpl.class);

    private final DefaultMQPullConsumer sailmqPullConsumer;
    private final KeyValue properties;
    private boolean started = false;
    private final MQPullConsumerScheduleService pullConsumerScheduleService;
    private final LocalMessageCache localMessageCache;
    private final ClientConfig clientConfig;

    public PullConsumerImpl(final KeyValue properties) {
        this.properties = properties;
        this.clientConfig = BeanUtils.populate(properties, ClientConfig.class);

        String consumerGroup = clientConfig.getConsumerId();
        if (null == consumerGroup || consumerGroup.isEmpty()) {
            throw new OMSRuntimeException("-1", "Consumer Group is necessary for SailMQ, please set it.");
        }
        pullConsumerScheduleService = new MQPullConsumerScheduleService(consumerGroup);

        this.sailmqPullConsumer = pullConsumerScheduleService.getDefaultMQPullConsumer();

        if ("true".equalsIgnoreCase(System.getenv("OMS_RMQ_DIRECT_NAME_SRV"))) {
            String accessPoints = clientConfig.getAccessPoints();
            if (accessPoints == null || accessPoints.isEmpty()) {
                throw new OMSRuntimeException("-1", "OMS AccessPoints is null or empty.");
            }
            this.sailmqPullConsumer.setNamesrvAddr(accessPoints.replace(',', ';'));
        }

        this.sailmqPullConsumer.setConsumerGroup(consumerGroup);

        int maxReDeliveryTimes = clientConfig.getRmqMaxRedeliveryTimes();
        this.sailmqPullConsumer.setMaxReconsumeTimes(maxReDeliveryTimes);

        String consumerId = OMSUtil.buildInstanceName();
        this.sailmqPullConsumer.setInstanceName(consumerId);
        properties.put(OMSBuiltinKeys.CONSUMER_ID, consumerId);

        this.sailmqPullConsumer.setLanguage(LanguageCode.OMS);

        this.localMessageCache = new LocalMessageCache(this.sailmqPullConsumer, clientConfig);
    }

    @Override
    public KeyValue attributes() {
        return properties;
    }

    @Override
    public PullConsumer attachQueue(String queueName) {
        registerPullTaskCallback(queueName);
        return this;
    }

    @Override
    public PullConsumer attachQueue(String queueName, KeyValue attributes) {
        registerPullTaskCallback(queueName);
        return this;
    }

    @Override
    public PullConsumer detachQueue(String queueName) {
        this.sailmqPullConsumer.getRegisterTopics().remove(queueName);
        return this;
    }

    @Override
    public Message receive() {
        MessageExt rmqMsg = localMessageCache.poll();
        return rmqMsg == null ? null : OMSUtil.msgConvert(rmqMsg);
    }

    @Override
    public Message receive(final KeyValue properties) {
        MessageExt rmqMsg = localMessageCache.poll(properties);
        return rmqMsg == null ? null : OMSUtil.msgConvert(rmqMsg);
    }

    @Override
    public void ack(final String messageId) {
        localMessageCache.ack(messageId);
    }

    @Override
    public void ack(final String messageId, final KeyValue properties) {
        localMessageCache.ack(messageId);
    }

    @Override
    public synchronized void startup() {
        if (!started) {
            try {
                this.pullConsumerScheduleService.start();
                this.localMessageCache.startup();
            } catch (MQClientException e) {
                throw new OMSRuntimeException("-1", e);
            }
        }
        this.started = true;
    }

    private void registerPullTaskCallback(final String targetQueueName) {
        this.pullConsumerScheduleService.registerPullTaskCallback(targetQueueName, new PullTaskCallback() {
            @Override
            public void doPullTask(final MessageQueue mq, final PullTaskContext context) {
                MQPullConsumer consumer = context.getPullConsumer();
                try {
                    long offset = localMessageCache.nextPullOffset(mq);

                    PullResult pullResult = consumer.pull(mq, "*",
                        offset, localMessageCache.nextPullBatchNums());
                    ProcessQueue pq = sailmqPullConsumer.getDefaultMQPullConsumerImpl().getRebalanceImpl()
                        .getProcessQueueTable().get(mq);
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            if (pq != null) {
                                pq.putMessage(pullResult.getMsgFoundList());
                                for (final MessageExt messageExt : pullResult.getMsgFoundList()) {
                                    localMessageCache.submitConsumeRequest(new ConsumeRequest(messageExt, mq, pq));
                                }
                            }
                            break;
                        default:
                            break;
                    }
                    localMessageCache.updatePullOffset(mq, pullResult.getNextBeginOffset());
                } catch (Exception e) {
                    log.error("An error occurred in pull message process.", e);
                }
            }
        });
    }

    @Override
    public synchronized void shutdown() {
        if (this.started) {
            this.localMessageCache.shutdown();
            this.pullConsumerScheduleService.shutdown();
            this.sailmqPullConsumer.shutdown();
        }
        this.started = false;
    }
}
