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
package io.openmessaging.sailmq.producer;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.ServiceLifecycle;
import io.openmessaging.exception.OMSMessageFormatException;
import io.openmessaging.exception.OMSNotSupportedException;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.exception.OMSTimeOutException;
import io.openmessaging.sailmq.config.ClientConfig;
import io.openmessaging.sailmq.domain.BytesMessageImpl;
import io.openmessaging.sailmq.utils.BeanUtils;
import org.sail.mq.client.exception.MQBrokerException;
import org.sail.mq.client.exception.MQClientException;
import org.sail.mq.client.producer.DefaultMQProducer;
import org.sail.mq.remoting.exception.RemotingConnectException;
import org.sail.mq.remoting.exception.RemotingTimeoutException;
import org.sail.mq.remoting.protocol.LanguageCode;
import org.sail.mq.remoting.protocol.ResponseCode;

import static io.openmessaging.sailmq.utils.OMSUtil.buildInstanceName;

abstract class AbstractOMSProducer implements ServiceLifecycle, MessageFactory {
    final KeyValue properties;
    final DefaultMQProducer sailmqProducer;
    private boolean started = false;
    private final ClientConfig clientConfig;

    AbstractOMSProducer(final KeyValue properties) {
        this.properties = properties;
        this.sailmqProducer = new DefaultMQProducer();
        this.clientConfig = BeanUtils.populate(properties, ClientConfig.class);

        if ("true".equalsIgnoreCase(System.getenv("OMS_RMQ_DIRECT_NAME_SRV"))) {
            String accessPoints = clientConfig.getAccessPoints();
            if (accessPoints == null || accessPoints.isEmpty()) {
                throw new OMSRuntimeException("-1", "OMS AccessPoints is null or empty.");
            }

            this.sailmqProducer.setNamesrvAddr(accessPoints.replace(',', ';'));
        }

        this.sailmqProducer.setProducerGroup(clientConfig.getRmqProducerGroup());

        String producerId = buildInstanceName();
        this.sailmqProducer.setSendMsgTimeout(clientConfig.getOperationTimeout());
        this.sailmqProducer.setInstanceName(producerId);
        this.sailmqProducer.setMaxMessageSize(1024 * 1024 * 4);
        this.sailmqProducer.setLanguage(LanguageCode.OMS);
        properties.put(OMSBuiltinKeys.PRODUCER_ID, producerId);
    }

    @Override
    public synchronized void startup() {
        if (!started) {
            try {
                this.sailmqProducer.start();
            } catch (MQClientException e) {
                throw new OMSRuntimeException("-1", e);
            }
        }
        this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.started) {
            this.sailmqProducer.shutdown();
        }
        this.started = false;
    }

    OMSRuntimeException checkProducerException(String topic, String msgId, Throwable e) {
        if (e instanceof MQClientException) {
            if (e.getCause() != null) {
                if (e.getCause() instanceof RemotingTimeoutException) {
                    return new OMSTimeOutException("-1", String.format("Send message to broker timeout, %dms, Topic=%s, msgId=%s",
                        this.sailmqProducer.getSendMsgTimeout(), topic, msgId), e);
                } else if (e.getCause() instanceof MQBrokerException || e.getCause() instanceof RemotingConnectException) {
                    if (e.getCause() instanceof MQBrokerException) {
                        MQBrokerException brokerException = (MQBrokerException) e.getCause();
                        return new OMSRuntimeException("-1", String.format("Received a broker exception, Topic=%s, msgId=%s, %s",
                            topic, msgId, brokerException.getErrorMessage()), e);
                    }

                    if (e.getCause() instanceof RemotingConnectException) {
                        RemotingConnectException connectException = (RemotingConnectException)e.getCause();
                        return new OMSRuntimeException("-1",
                            String.format("Network connection experiences failures. Topic=%s, msgId=%s, %s",
                                topic, msgId, connectException.getMessage()),
                            e);
                    }
                }
            }
            // Exception thrown by local.
            else {
                MQClientException clientException = (MQClientException) e;
                if (-1 == clientException.getResponseCode()) {
                    return new OMSRuntimeException("-1", String.format("Topic does not exist, Topic=%s, msgId=%s",
                        topic, msgId), e);
                } else if (ResponseCode.MESSAGE_ILLEGAL == clientException.getResponseCode()) {
                    return new OMSMessageFormatException("-1", String.format("A illegal message for SailMQ, Topic=%s, msgId=%s",
                        topic, msgId), e);
                }
            }
        }
        return new OMSRuntimeException("-1", "Send message to SailMQ broker failed.", e);
    }

    protected void checkMessageType(Message message) {
        if (!(message instanceof BytesMessage)) {
            throw new OMSNotSupportedException("-1", "Only BytesMessage is supported.");
        }
    }

    @Override
    public BytesMessage createBytesMessage(String queue, byte[] body) {
        BytesMessage message = new BytesMessageImpl();
        message.setBody(body);
        message.sysHeaders().put(Message.BuiltinKeys.DESTINATION, queue);
        return message;
    }
}
