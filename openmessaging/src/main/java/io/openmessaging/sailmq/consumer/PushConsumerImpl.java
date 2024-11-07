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

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.consumer.PushConsumer;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.interceptor.ConsumerInterceptor;
import io.openmessaging.sailmq.config.ClientConfig;
import io.openmessaging.sailmq.domain.NonStandardKeys;
import io.openmessaging.sailmq.utils.BeanUtils;
import io.openmessaging.sailmq.utils.OMSUtil;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.sail.mq.client.consumer.DefaultMQPushConsumer;
import org.sail.mq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.sail.mq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.sail.mq.client.consumer.listener.MessageListenerConcurrently;
import org.sail.mq.client.exception.MQClientException;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.remoting.protocol.LanguageCode;

public class PushConsumerImpl implements PushConsumer {
    private final DefaultMQPushConsumer sailmqPushConsumer;
    private final KeyValue properties;
    private boolean started = false;
    private final Map<String, MessageListener> subscribeTable = new ConcurrentHashMap<>();
    private final ClientConfig clientConfig;

    public PushConsumerImpl(final KeyValue properties) {
        this.sailmqPushConsumer = new DefaultMQPushConsumer();
        this.properties = properties;
        this.clientConfig = BeanUtils.populate(properties, ClientConfig.class);

        if ("true".equalsIgnoreCase(System.getenv("OMS_RMQ_DIRECT_NAME_SRV"))) {
            String accessPoints = clientConfig.getAccessPoints();
            if (accessPoints == null || accessPoints.isEmpty()) {
                throw new OMSRuntimeException("-1", "OMS AccessPoints is null or empty.");
            }
            this.sailmqPushConsumer.setNamesrvAddr(accessPoints.replace(',', ';'));
        }

        String consumerGroup = clientConfig.getConsumerId();
        if (null == consumerGroup || consumerGroup.isEmpty()) {
            throw new OMSRuntimeException("-1", "Consumer Group is necessary for SailMQ, please set it.");
        }
        this.sailmqPushConsumer.setConsumerGroup(consumerGroup);
        this.sailmqPushConsumer.setMaxReconsumeTimes(clientConfig.getRmqMaxRedeliveryTimes());
        this.sailmqPushConsumer.setConsumeTimeout(clientConfig.getRmqMessageConsumeTimeout());
        this.sailmqPushConsumer.setConsumeThreadMax(clientConfig.getRmqMaxConsumeThreadNums());
        this.sailmqPushConsumer.setConsumeThreadMin(clientConfig.getRmqMinConsumeThreadNums());

        String consumerId = OMSUtil.buildInstanceName();
        this.sailmqPushConsumer.setInstanceName(consumerId);
        properties.put(OMSBuiltinKeys.CONSUMER_ID, consumerId);
        this.sailmqPushConsumer.setLanguage(LanguageCode.OMS);

        this.sailmqPushConsumer.registerMessageListener(new MessageListenerImpl());
    }

    @Override
    public KeyValue attributes() {
        return properties;
    }

    @Override
    public void resume() {
        this.sailmqPushConsumer.resume();
    }

    @Override
    public void suspend() {
        this.sailmqPushConsumer.suspend();
    }

    @Override
    public void suspend(long timeout) {

    }

    @Override
    public boolean isSuspended() {
        return this.sailmqPushConsumer.isPause();
    }

    @Override
    public PushConsumer attachQueue(final String queueName, final MessageListener listener) {
        this.subscribeTable.put(queueName, listener);
        try {
            this.sailmqPushConsumer.subscribe(queueName, "*");
        } catch (MQClientException e) {
            throw new OMSRuntimeException("-1", String.format("SailMQ push consumer can't attach to %s.", queueName));
        }
        return this;
    }

    @Override
    public PushConsumer attachQueue(String queueName, MessageListener listener, KeyValue attributes) {
        return this.attachQueue(queueName, listener);
    }

    @Override
    public PushConsumer detachQueue(String queueName) {
        this.subscribeTable.remove(queueName);
        try {
            this.sailmqPushConsumer.unsubscribe(queueName);
        } catch (Exception e) {
            throw new OMSRuntimeException("-1", String.format("SailMQ push consumer fails to unsubscribe topic: %s", queueName));
        }
        return null;
    }

    @Override
    public void addInterceptor(ConsumerInterceptor interceptor) {

    }

    @Override
    public void removeInterceptor(ConsumerInterceptor interceptor) {

    }

    @Override
    public synchronized void startup() {
        if (!started) {
            try {
                this.sailmqPushConsumer.start();
            } catch (MQClientException e) {
                throw new OMSRuntimeException("-1", e);
            }
        }
        this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.started) {
            this.sailmqPushConsumer.shutdown();
        }
        this.started = false;
    }

    class MessageListenerImpl implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> rmqMsgList,
            ConsumeConcurrentlyContext contextRMQ) {
            MessageExt rmqMsg = rmqMsgList.get(0);
            BytesMessage omsMsg = OMSUtil.msgConvert(rmqMsg);

            MessageListener listener = PushConsumerImpl.this.subscribeTable.get(rmqMsg.getTopic());

            if (listener == null) {
                throw new OMSRuntimeException("-1",
                    String.format("The topic/queue %s isn't attached to this consumer", rmqMsg.getTopic()));
            }

            final KeyValue contextProperties = OMS.newKeyValue();
            final CountDownLatch sync = new CountDownLatch(1);

            contextProperties.put(NonStandardKeys.MESSAGE_CONSUME_STATUS, ConsumeConcurrentlyStatus.RECONSUME_LATER.name());

            MessageListener.Context context = new MessageListener.Context() {
                @Override
                public KeyValue attributes() {
                    return contextProperties;
                }

                @Override
                public void ack() {
                    sync.countDown();
                    contextProperties.put(NonStandardKeys.MESSAGE_CONSUME_STATUS,
                        ConsumeConcurrentlyStatus.CONSUME_SUCCESS.name());
                }
            };
            long begin = System.currentTimeMillis();
            listener.onReceived(omsMsg, context);
            long costs = System.currentTimeMillis() - begin;
            long timeoutMills = clientConfig.getRmqMessageConsumeTimeout() * 60 * 1000;
            try {
                sync.await(Math.max(0, timeoutMills - costs), TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignore) {
            }

            return ConsumeConcurrentlyStatus.valueOf(contextProperties.getString(NonStandardKeys.MESSAGE_CONSUME_STATUS));
        }
    }
}
