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

import org.sail.mq.common.action.Action;
import org.sail.mq.common.action.RocketMQAction;
import org.sail.mq.common.resource.ResourceType;
import org.sail.mq.common.resource.RocketMQResource;
import org.sail.mq.remoting.CommandCustomHeader;
import org.sail.mq.remoting.annotation.CFNotNull;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.GET_BROKER_CLUSTER_ACL_INFO, resource = ResourceType.CLUSTER, action = Action.GET)
public class GetBrokerAclConfigResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private String version;

    private String allAclFileVersion;

    @CFNotNull
    private String brokerName;

    @CFNotNull
    private String brokerAddr;

    @CFNotNull
    @RocketMQResource(ResourceType.CLUSTER)
    private String clusterName;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getAllAclFileVersion() {
        return allAclFileVersion;
    }

    public void setAllAclFileVersion(String allAclFileVersion) {
        this.allAclFileVersion = allAclFileVersion;
    }
}
