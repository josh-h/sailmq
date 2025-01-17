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
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.protocol.RequestCode;
import org.sail.mq.remoting.rpc.TopicQueueRequestHeader;

@SailMQAction(value = RequestCode.NOTIFICATION, action = Action.SUB)
public class NotificationRequestHeader extends TopicQueueRequestHeader {
    @CFNotNull
    @SailMQResource(ResourceType.GROUP)
    private String consumerGroup;
    @CFNotNull
    @SailMQResource(ResourceType.TOPIC)
    private String topic;
    @CFNotNull
    private int queueId;
    @CFNotNull
    private long pollTime;
    @CFNotNull
    private long bornTime;

    private Boolean order = Boolean.FALSE;
    private String attemptId;

    @CFNotNull
    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public long getPollTime() {
        return pollTime;
    }

    public void setPollTime(long pollTime) {
        this.pollTime = pollTime;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public long getBornTime() {
        return bornTime;
    }

    public void setBornTime(long bornTime) {
        this.bornTime = bornTime;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        if (queueId < 0) {
            return -1;
        }
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Boolean getOrder() {
        return order;
    }

    public void setOrder(Boolean order) {
        this.order = order;
    }

    public String getAttemptId() {
        return attemptId;
    }

    public void setAttemptId(String attemptId) {
        this.attemptId = attemptId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("consumerGroup", consumerGroup)
            .add("topic", topic)
            .add("queueId", queueId)
            .add("pollTime", pollTime)
            .add("bornTime", bornTime)
            .add("order", order)
            .add("attemptId", attemptId)
            .toString();
    }
}
