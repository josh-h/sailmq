/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sail.mq.proxy.service.relay;

import java.util.concurrent.CompletableFuture;
import org.sail.mq.common.message.MessageConst;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.common.ProxyException;
import org.sail.mq.proxy.common.ProxyExceptionCode;
import org.sail.mq.proxy.common.utils.ProxyUtils;
import org.sail.mq.proxy.service.transaction.TransactionData;
import org.sail.mq.proxy.service.transaction.TransactionService;
import org.sail.mq.remoting.protocol.RemotingCommand;
import org.sail.mq.remoting.protocol.header.CheckTransactionStateRequestHeader;

public abstract class AbstractProxyRelayService implements ProxyRelayService {

    protected final TransactionService transactionService;

    public AbstractProxyRelayService(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Override
    public RelayData<TransactionData, Void> processCheckTransactionState(ProxyContext context,
        RemotingCommand command, CheckTransactionStateRequestHeader header, MessageExt messageExt) {
        CompletableFuture<ProxyRelayResult<Void>> future = new CompletableFuture<>();
        String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        TransactionData transactionData = transactionService.addTransactionDataByBrokerAddr(
            context,
            command.getExtFields().get(ProxyUtils.BROKER_ADDR),
            messageExt.getTopic(),
            group,
            header.getTranStateTableOffset(),
            header.getCommitLogOffset(),
            header.getTransactionId(),
            messageExt);
        if (transactionData == null) {
            throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR,
                String.format("add transaction data failed. request:%s, message:%s", command, messageExt));
        }
        future.exceptionally(throwable -> {
            this.transactionService.onSendCheckTransactionStateFailed(context, group, transactionData);
            return null;
        });
        return new RelayData<>(transactionData, future);
    }
}
