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

/**
 * $Id: SearchOffsetRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.sail.mq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import org.sail.mq.common.BoundaryType;
import org.sail.mq.common.action.Action;
import org.sail.mq.common.action.SailMQAction;
import org.sail.mq.common.resource.ResourceType;
import org.sail.mq.common.resource.SailMQResource;
import org.sail.mq.remoting.annotation.CFNotNull;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.protocol.RequestCode;
import org.sail.mq.remoting.rpc.TopicQueueRequestHeader;

@SailMQAction(value = RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, action = Action.GET)
public class SearchOffsetRequestHeader extends TopicQueueRequestHeader {
    @CFNotNull
    @SailMQResource(ResourceType.TOPIC)
    private String topic;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private Long timestamp;

    private BoundaryType boundaryType;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public Integer getQueueId() {
        return queueId;
    }

    @Override
    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public BoundaryType getBoundaryType() {
        // default return LOWER
        return boundaryType == null ? BoundaryType.LOWER : boundaryType;
    }

    public void setBoundaryType(BoundaryType boundaryType) {
        this.boundaryType = boundaryType;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("topic", topic)
            .add("queueId", queueId)
            .add("timestamp", timestamp)
            .add("boundaryType", boundaryType.getName())
            .toString();
    }
}