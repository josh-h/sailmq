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
package org.sail.mq.proxy.service.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import org.sail.mq.broker.client.ProducerManager;
import org.sail.mq.client.impl.ClientRemotingProcessor;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.common.message.MessageConst;
import org.sail.mq.common.message.MessageDecoder;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.common.utils.NetworkUtil;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.proxy.common.utils.ProxyUtils;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.protocol.RemotingCommand;
import org.sail.mq.remoting.protocol.RequestCode;
import org.sail.mq.remoting.protocol.header.CheckTransactionStateRequestHeader;

public class ProxyClientRemotingProcessor extends ClientRemotingProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final ProducerManager producerManager;

    public ProxyClientRemotingProcessor(ProducerManager producerManager) {
        super(null);
        this.producerManager = producerManager;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        if (request.getCode() == RequestCode.CHECK_TRANSACTION_STATE) {
            return this.checkTransactionState(ctx, request);
        }
        return null;
    }

    @Override
    public RemotingCommand checkTransactionState(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer, true, false, false);
        if (messageExt != null) {
            final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                CheckTransactionStateRequestHeader requestHeader =
                    (CheckTransactionStateRequestHeader) request.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
                request.writeCustomHeader(requestHeader);
                request.addExtField(ProxyUtils.BROKER_ADDR, NetworkUtil.socketAddress2String(ctx.channel().remoteAddress()));
                Channel channel = this.producerManager.getAvailableChannel(group);
                if (channel != null) {
                    channel.writeAndFlush(request);
                } else {
                    log.warn("check transaction failed, channel is empty. groupId={}, requestHeader:{}", group, requestHeader);
                }
            }
        }
        return null;
    }
}
