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
package org.sail.mq.example.lmq;

import org.sail.mq.client.exception.MQClientException;
import org.sail.mq.client.producer.DefaultMQProducer;
import org.sail.mq.client.producer.SendResult;
import org.sail.mq.common.MixAll;
import org.sail.mq.common.message.Message;
import org.sail.mq.common.message.MessageConst;
import org.sail.mq.remoting.common.RemotingHelper;

public class LMQProducer {
    public static final String PRODUCER_GROUP = "ProducerGroupName";

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";

    public static final String TOPIC = "TopicLMQParent";

    public static final String TAG = "TagA";

    public static final String LMQ_TOPIC_1 = MixAll.LMQ_PREFIX + "123";

    public static final String LMQ_TOPIC_2 = MixAll.LMQ_PREFIX + "456";

    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        producer.start();
        for (int i = 0; i < 128; i++) {
            try {
                Message msg = new Message(TOPIC, TAG, ("Hello SailMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                msg.setKeys("Key" + i);
                msg.putUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH /* "INNER_MULTI_DISPATCH" */,
                    String.join(MixAll.LMQ_DISPATCH_SEPARATOR, LMQ_TOPIC_1, LMQ_TOPIC_2) /* "%LMQ%123,%LMQ%456" */);
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.shutdown();
    }
}
