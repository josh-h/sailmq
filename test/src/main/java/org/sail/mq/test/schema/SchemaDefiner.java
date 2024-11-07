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
package org.sail.mq.test.schema;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.sail.mq.client.ClientConfig;
import org.sail.mq.client.consumer.AllocateMessageQueueStrategy;
import org.sail.mq.client.consumer.DefaultLitePullConsumer;
import org.sail.mq.client.consumer.DefaultMQPullConsumer;
import org.sail.mq.client.consumer.DefaultMQPushConsumer;
import org.sail.mq.client.consumer.PullCallback;
import org.sail.mq.client.consumer.PullResult;
import org.sail.mq.client.consumer.PullStatus;
import org.sail.mq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.sail.mq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.sail.mq.client.consumer.listener.ConsumeOrderlyContext;
import org.sail.mq.client.consumer.listener.ConsumeOrderlyStatus;
import org.sail.mq.client.consumer.listener.MessageListener;
import org.sail.mq.client.consumer.listener.MessageListenerConcurrently;
import org.sail.mq.client.consumer.listener.MessageListenerOrderly;
import org.sail.mq.client.hook.*;
import org.sail.mq.client.producer.DefaultMQProducer;
import org.sail.mq.client.producer.MessageQueueSelector;
import org.sail.mq.client.producer.SendCallback;
import org.sail.mq.client.producer.SendResult;
import org.sail.mq.client.producer.SendStatus;
import org.sail.mq.common.message.Message;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.common.message.MessageQueue;
import org.sail.mq.remoting.CommandCustomHeader;
import org.sail.mq.remoting.RPCHook;
import org.sail.mq.remoting.protocol.RequestCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.reflections.Reflections;

public class SchemaDefiner {
    public static final Map<Class<?>, Set<String>> IGNORED_FIELDS = new HashMap<>();
    //Use name as the key instead of X.class directly. X.class is not equal to field.getType().
    public static final Set<String> FIELD_CLASS_NAMES = new HashSet<>();
    public static final List<Class<?>> API_CLASS_LIST = new ArrayList<>();
    public static final List<Class<?>> PROTOCOL_CLASS_LIST = new ArrayList<>();

    public static void doLoad() {
        {
            IGNORED_FIELDS.put(ClientConfig.class, Sets.newHashSet("namesrvAddr", "clientIP", "clientCallbackExecutorThreads"));
            IGNORED_FIELDS.put(DefaultLitePullConsumer.class, Sets.newHashSet("consumeTimestamp"));
            IGNORED_FIELDS.put(DefaultMQPushConsumer.class, Sets.newHashSet("consumeTimestamp"));
            FIELD_CLASS_NAMES.add(String.class.getName());
            FIELD_CLASS_NAMES.add(Long.class.getName());
            FIELD_CLASS_NAMES.add(Integer.class.getName());
            FIELD_CLASS_NAMES.add(Short.class.getName());
            FIELD_CLASS_NAMES.add(Byte.class.getName());
            FIELD_CLASS_NAMES.add(Double.class.getName());
            FIELD_CLASS_NAMES.add(Float.class.getName());
            FIELD_CLASS_NAMES.add(Boolean.class.getName());
        }
        {
            //basic
            API_CLASS_LIST.add(DefaultMQPushConsumer.class);
            API_CLASS_LIST.add(DefaultMQProducer.class);
            API_CLASS_LIST.add(DefaultMQPullConsumer.class);
            API_CLASS_LIST.add(DefaultLitePullConsumer.class);
            API_CLASS_LIST.add(DefaultMQAdminExt.class);

            //argument
            API_CLASS_LIST.add(Message.class);
            API_CLASS_LIST.add(MessageQueue.class);
            API_CLASS_LIST.add(SendCallback.class);
            API_CLASS_LIST.add(PullCallback.class);
            API_CLASS_LIST.add(MessageQueueSelector.class);
            API_CLASS_LIST.add(AllocateMessageQueueStrategy.class);
            //result
            API_CLASS_LIST.add(MessageExt.class);
            API_CLASS_LIST.add(SendResult.class);
            API_CLASS_LIST.add(SendStatus.class);
            API_CLASS_LIST.add(PullResult.class);
            API_CLASS_LIST.add(PullStatus.class);
            //listener and context
            API_CLASS_LIST.add(MessageListener.class);
            API_CLASS_LIST.add(MessageListenerConcurrently.class);
            API_CLASS_LIST.add(ConsumeConcurrentlyContext.class);
            API_CLASS_LIST.add(ConsumeConcurrentlyStatus.class);
            API_CLASS_LIST.add(MessageListenerOrderly.class);
            API_CLASS_LIST.add(ConsumeOrderlyContext.class);
            API_CLASS_LIST.add(ConsumeOrderlyStatus.class);
            //hook and context
            API_CLASS_LIST.add(RPCHook.class);
            API_CLASS_LIST.add(FilterMessageHook.class);
            API_CLASS_LIST.add(SendMessageHook.class);
            API_CLASS_LIST.add(CheckForbiddenHook.class);
            API_CLASS_LIST.add(ConsumeMessageHook.class);
            API_CLASS_LIST.add(EndTransactionHook.class);
            API_CLASS_LIST.add(FilterMessageContext.class);
            API_CLASS_LIST.add(SendMessageContext.class);
            API_CLASS_LIST.add(ConsumeMessageContext.class);
            API_CLASS_LIST.add(ConsumeMessageContext.class);
            API_CLASS_LIST.add(EndTransactionContext.class);

        }
        {
            PROTOCOL_CLASS_LIST.add(RequestCode.class);
            Reflections reflections = new Reflections("org.apache.sailmq");
            for (Class<?> protocolClass: reflections.getSubTypesOf(CommandCustomHeader.class)) {
                PROTOCOL_CLASS_LIST.add(protocolClass);
            }
        }

    }

}
