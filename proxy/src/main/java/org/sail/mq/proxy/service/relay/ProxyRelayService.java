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
package org.sail.mq.proxy.service.relay;

import java.util.concurrent.CompletableFuture;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.service.transaction.TransactionData;
import org.sail.mq.remoting.protocol.RemotingCommand;
import org.sail.mq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.sail.mq.remoting.protocol.body.ConsumerRunningInfo;
import org.sail.mq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.sail.mq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.sail.mq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;

public interface ProxyRelayService {

    CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> processGetConsumerRunningInfo(
        ProxyContext context,
        RemotingCommand command,
        GetConsumerRunningInfoRequestHeader header
    );

    CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> processConsumeMessageDirectly(
        ProxyContext context,
        RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header
    );

    RelayData<TransactionData, Void> processCheckTransactionState(
        ProxyContext context,
        RemotingCommand command,
        CheckTransactionStateRequestHeader header,
        MessageExt messageExt
    );
}
