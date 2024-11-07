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
import org.sail.mq.common.action.RocketMQAction;
import org.sail.mq.common.resource.ResourceType;
import org.sail.mq.common.resource.RocketMQResource;
import org.sail.mq.remoting.annotation.CFNotNull;
import org.sail.mq.remoting.annotation.CFNullable;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.protocol.RequestCode;
import org.sail.mq.remoting.rpc.RpcRequestHeader;

@RocketMQAction(value = RequestCode.CONSUMER_SEND_MSG_BACK, action = Action.SUB)
public class ConsumerSendMsgBackRequestHeader extends RpcRequestHeader {
    @CFNotNull
    private Long offset;
    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String group;
    @CFNotNull
    private Integer delayLevel;
    private String originMsgId;
    @RocketMQResource(ResourceType.TOPIC)
    private String originTopic;
    @CFNullable
    private boolean unitMode = false;
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Integer getDelayLevel() {
        return delayLevel;
    }

    public void setDelayLevel(Integer delayLevel) {
        this.delayLevel = delayLevel;
    }

    public String getOriginMsgId() {
        return originMsgId;
    }

    public void setOriginMsgId(String originMsgId) {
        this.originMsgId = originMsgId;
    }

    public String getOriginTopic() {
        return originTopic;
    }

    public void setOriginTopic(String originTopic) {
        this.originTopic = originTopic;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("offset", offset)
            .add("group", group)
            .add("delayLevel", delayLevel)
            .add("originMsgId", originMsgId)
            .add("originTopic", originTopic)
            .add("unitMode", unitMode)
            .add("maxReconsumeTimes", maxReconsumeTimes)
            .toString();
    }
}
