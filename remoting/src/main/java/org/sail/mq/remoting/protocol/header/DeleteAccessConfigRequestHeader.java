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
import org.sail.mq.common.action.SailMQAction;
import org.sail.mq.common.resource.ResourceType;
import org.sail.mq.remoting.CommandCustomHeader;
import org.sail.mq.remoting.annotation.CFNotNull;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.protocol.RequestCode;

@SailMQAction(value = RequestCode.DELETE_ACL_CONFIG, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class DeleteAccessConfigRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String accessKey;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }
}