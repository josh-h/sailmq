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

package org.sail.mq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import org.sail.mq.common.action.Action;
import org.sail.mq.common.action.SailMQAction;
import org.sail.mq.common.resource.ResourceType;
import org.sail.mq.common.resource.SailMQResource;
import org.sail.mq.remoting.annotation.CFNotNull;
import org.sail.mq.remoting.annotation.CFNullable;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.protocol.RequestCode;
import org.sail.mq.remoting.rpc.TopicRequestHeader;

@SailMQAction(value = RequestCode.CONSUME_MESSAGE_DIRECTLY, action = Action.SUB)
public class ConsumeMessageDirectlyResultRequestHeader extends TopicRequestHeader {
    @CFNotNull
    @SailMQResource(ResourceType.GROUP)
    private String consumerGroup;
    @CFNullable
    private String clientId;
    @CFNullable
    private String msgId;
    @CFNullable
    private String brokerName;
    @CFNullable
    @SailMQResource(ResourceType.TOPIC)
    private String topic;
    @CFNullable
    private Integer topicSysFlag;
    @CFNullable
    private Integer groupSysFlag;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(Integer topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public Integer getGroupSysFlag() {
        return groupSysFlag;
    }

    public void setGroupSysFlag(Integer groupSysFlag) {
        this.groupSysFlag = groupSysFlag;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("consumerGroup", consumerGroup)
            .add("clientId", clientId)
            .add("msgId", msgId)
            .add("brokerName", brokerName)
            .add("topic", topic)
            .add("topicSysFlag", topicSysFlag)
            .add("groupSysFlag", groupSysFlag)
            .toString();
    }
}
