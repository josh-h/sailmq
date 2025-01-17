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
import io.openmessaging.Promise;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.interceptor.ProducerInterceptor;
import io.openmessaging.producer.BatchMessageSender;
import io.openmessaging.producer.LocalTransactionExecutor;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import io.openmessaging.sailmq.promise.DefaultPromise;
import io.openmessaging.sailmq.utils.OMSUtil;
import org.sail.mq.client.producer.SendCallback;
import org.sail.mq.client.producer.SendStatus;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;

import static io.openmessaging.sailmq.utils.OMSUtil.msgConvert;

public class ProducerImpl extends AbstractOMSProducer implements Producer {

    private static final Logger log = LoggerFactory.getLogger(ProducerImpl.class);

    public ProducerImpl(final KeyValue properties) {
        super(properties);
    }

    @Override
    public KeyValue attributes() {
        return properties;
    }

    @Override
    public SendResult send(final Message message) {
        return send(message, this.sailmqProducer.getSendMsgTimeout());
    }

    @Override
    public SendResult send(final Message message, final KeyValue properties) {
        long timeout = properties.containsKey(Message.BuiltinKeys.TIMEOUT)
            ? properties.getInt(Message.BuiltinKeys.TIMEOUT) : this.sailmqProducer.getSendMsgTimeout();
        return send(message, timeout);
    }

    @Override
    public SendResult send(Message message, LocalTransactionExecutor branchExecutor, KeyValue attributes) {
        return null;
    }

    private SendResult send(final Message message, long timeout) {
        checkMessageType(message);
        org.sail.mq.common.message.Message rmqMessage = msgConvert((BytesMessage) message);
        try {
            org.sail.mq.client.producer.SendResult rmqResult = this.sailmqProducer.send(rmqMessage, timeout);
            if (!rmqResult.getSendStatus().equals(SendStatus.SEND_OK)) {
                log.error(String.format("Send message to SailMQ failed, %s", message));
                throw new OMSRuntimeException("-1", "Send message to SailMQ broker failed.");
            }
            message.sysHeaders().put(Message.BuiltinKeys.MESSAGE_ID, rmqResult.getMsgId());
            return OMSUtil.sendResultConvert(rmqResult);
        } catch (Exception e) {
            log.error(String.format("Send message to SailMQ failed, %s", message), e);
            throw checkProducerException(rmqMessage.getTopic(), message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID), e);
        }
    }

    @Override
    public Promise<SendResult> sendAsync(final Message message) {
        return sendAsync(message, this.sailmqProducer.getSendMsgTimeout());
    }

    @Override
    public Promise<SendResult> sendAsync(final Message message, final KeyValue properties) {
        long timeout = properties.containsKey(Message.BuiltinKeys.TIMEOUT)
            ? properties.getInt(Message.BuiltinKeys.TIMEOUT) : this.sailmqProducer.getSendMsgTimeout();
        return sendAsync(message, timeout);
    }

    private Promise<SendResult> sendAsync(final Message message, long timeout) {
        checkMessageType(message);
        org.sail.mq.common.message.Message rmqMessage = msgConvert((BytesMessage) message);
        final Promise<SendResult> promise = new DefaultPromise<>();
        try {
            this.sailmqProducer.send(rmqMessage, new SendCallback() {
                @Override
                public void onSuccess(final org.sail.mq.client.producer.SendResult rmqResult) {
                    message.sysHeaders().put(Message.BuiltinKeys.MESSAGE_ID, rmqResult.getMsgId());
                    promise.set(OMSUtil.sendResultConvert(rmqResult));
                }

                @Override
                public void onException(final Throwable e) {
                    promise.setFailure(e);
                }
            }, timeout);
        } catch (Exception e) {
            promise.setFailure(e);
        }
        return promise;
    }

    @Override
    public void sendOneway(final Message message) {
        checkMessageType(message);
        org.sail.mq.common.message.Message rmqMessage = msgConvert((BytesMessage) message);
        try {
            this.sailmqProducer.sendOneway(rmqMessage);
        } catch (Exception ignore) { //Ignore the oneway exception.
        }
    }

    @Override
    public void sendOneway(final Message message, final KeyValue properties) {
        sendOneway(message);
    }

    @Override
    public BatchMessageSender createBatchMessageSender() {
        return null;
    }

    @Override
    public void addInterceptor(ProducerInterceptor interceptor) {

    }

    @Override
    public void removeInterceptor(ProducerInterceptor interceptor) {

    }
}
