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
package org.sail.mq.proxy.remoting.pipeline;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.sail.mq.common.MQVersion;
import org.sail.mq.common.utils.NetworkUtil;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.processor.channel.ChannelProtocolType;
import org.sail.mq.remoting.common.RemotingHelper;
import org.sail.mq.remoting.netty.AttributeKeys;
import org.sail.mq.remoting.protocol.LanguageCode;
import org.sail.mq.remoting.protocol.RemotingCommand;

public class ContextInitPipeline implements RequestPipeline {

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws Exception {
        Channel channel = ctx.channel();
        LanguageCode languageCode = RemotingHelper.getAttributeValue(AttributeKeys.LANGUAGE_CODE_KEY, channel);
        String clientId = RemotingHelper.getAttributeValue(AttributeKeys.CLIENT_ID_KEY, channel);
        Integer version = RemotingHelper.getAttributeValue(AttributeKeys.VERSION_KEY, channel);
        context.setAction(RemotingHelper.getRequestCodeDesc(request.getCode()))
            .setProtocolType(ChannelProtocolType.REMOTING.getName())
            .setChannel(channel)
            .setLocalAddress(NetworkUtil.socketAddress2String(ctx.channel().localAddress()))
            .setRemoteAddress(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        if (languageCode != null) {
            context.setLanguage(languageCode.name());
        }
        if (clientId != null) {
            context.setClientID(clientId);
        }
        if (version != null) {
            context.setClientVersion(MQVersion.getVersionDesc(version));
        }
    }
}
