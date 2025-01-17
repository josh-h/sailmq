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

package org.sail.mq.proxy.processor;

import java.util.concurrent.CompletableFuture;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.service.ServiceManager;
import org.sail.mq.remoting.protocol.RemotingCommand;

public class RequestBrokerProcessor extends AbstractProcessor {

    public RequestBrokerProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager) {
        super(messagingProcessor, serviceManager);
    }

    CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request, long timeoutMillis) {
        return serviceManager.getMessageService().request(ctx, brokerName, request, timeoutMillis);
    }

    CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request, long timeoutMillis) {
        return serviceManager.getMessageService().requestOneway(ctx, brokerName, request, timeoutMillis);
    }
}
