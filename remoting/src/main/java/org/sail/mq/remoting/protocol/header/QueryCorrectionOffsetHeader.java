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
 * $Id: GetMinOffsetRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.sail.mq.remoting.protocol.header;

import org.sail.mq.common.action.Action;
import org.sail.mq.common.action.SailMQAction;
import org.sail.mq.common.resource.ResourceType;
import org.sail.mq.common.resource.SailMQResource;
import org.sail.mq.remoting.annotation.CFNotNull;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.rpc.TopicRequestHeader;
import org.sail.mq.remoting.protocol.RequestCode;

@SailMQAction(value = RequestCode.QUERY_CORRECTION_OFFSET, action = Action.GET)
public class QueryCorrectionOffsetHeader extends TopicRequestHeader {
    @SailMQResource(value = ResourceType.GROUP, splitter = ",")
    private String filterGroups;
    @CFNotNull
    @SailMQResource(ResourceType.GROUP)
    private String compareGroup;
    @CFNotNull
    @SailMQResource(ResourceType.TOPIC)
    private String topic;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getFilterGroups() {
        return filterGroups;
    }

    public void setFilterGroups(String filterGroups) {
        this.filterGroups = filterGroups;
    }

    public String getCompareGroup() {
        return compareGroup;
    }

    public void setCompareGroup(String compareGroup) {
        this.compareGroup = compareGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
