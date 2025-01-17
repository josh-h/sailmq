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
package org.sail.mq.broker.transaction.queue;

import org.sail.mq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.sail.mq.common.TopicConfig;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.common.constant.PermName;
import org.sail.mq.common.message.MessageAccessor;
import org.sail.mq.common.message.MessageConst;
import org.sail.mq.common.message.MessageDecoder;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.common.message.MessageExtBrokerInner;
import org.sail.mq.store.PutMessageResult;
import org.sail.mq.store.PutMessageStatus;

import java.util.concurrent.ThreadLocalRandom;

public class DefaultTransactionalMessageCheckListener extends AbstractTransactionalMessageCheckListener {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    public DefaultTransactionalMessageCheckListener() {
        super();
    }

    @Override
    public void resolveDiscardMsg(MessageExt msgExt) {
        log.error("MsgExt:{} has been checked too many times, so discard it by moving it to system topic TRANS_CHECK_MAXTIME_TOPIC", msgExt);

        try {
            MessageExtBrokerInner brokerInner = toMessageExtBrokerInner(msgExt);
            PutMessageResult putMessageResult = this.getBrokerController().getMessageStore().putMessage(brokerInner);
            if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                log.info("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC OK. Restored in queueOffset={}, " +
                    "commitLogOffset={}, real topic={}", msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
                // discarded, then the num of half-messages minus 1
                this.getBrokerController().getTransactionalMessageService().getTransactionMetrics().addAndGet(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC), -1);
            } else {
                log.error("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC failed, real topic={}, msgId={}", msgExt.getTopic(), msgExt.getMsgId());
            }
        } catch (Exception e) {
            log.warn("Put checked-too-many-time message to TRANS_CHECK_MAXTIME_TOPIC error. {}", e);
        }

    }

    private MessageExtBrokerInner toMessageExtBrokerInner(MessageExt msgExt) {
        TopicConfig topicConfig = this.getBrokerController().getTopicConfigManager().createTopicOfTranCheckMaxTime(TCMT_QUEUE_NUMS, PermName.PERM_READ | PermName.PERM_WRITE);
        int queueId = ThreadLocalRandom.current().nextInt(99999999) % TCMT_QUEUE_NUMS;
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setTopic(topicConfig.getTopicName());
        inner.setBody(msgExt.getBody());
        inner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(inner, msgExt.getProperties());
        inner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        inner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgExt.getTags()));
        inner.setQueueId(queueId);
        inner.setSysFlag(msgExt.getSysFlag());
        inner.setBornHost(msgExt.getBornHost());
        inner.setBornTimestamp(msgExt.getBornTimestamp());
        inner.setStoreHost(msgExt.getStoreHost());
        inner.setReconsumeTimes(msgExt.getReconsumeTimes());
        inner.setMsgId(msgExt.getMsgId());
        inner.setWaitStoreMsgOK(false);
        return inner;
    }
}
