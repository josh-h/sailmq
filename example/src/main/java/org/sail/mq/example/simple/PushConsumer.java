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
package org.sail.mq.example.simple;

import java.util.List;
import org.sail.mq.client.consumer.DefaultMQPushConsumer;
import org.sail.mq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.sail.mq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.sail.mq.client.consumer.listener.MessageListenerConcurrently;
import org.sail.mq.client.exception.MQClientException;
import org.sail.mq.common.consumer.ConsumeFromWhere;
import org.sail.mq.common.message.MessageExt;

public class PushConsumer {
    public static final String TOPIC = "TopicTest";
    public static final String CONSUMER_GROUP = "CID_JODIE_1";
    public static final String NAMESRV_ADDR = "127.0.0.1:9876";
    public static void main(String[] args) throws InterruptedException, MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
//        consumer.setNamesrvAddr(NAMESRV_ADDR);

        consumer.subscribe(TOPIC, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //wrong time format 2017_0422_221800
        consumer.setConsumeTimestamp("20181109221800");
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}