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

package org.sail.mq.namesrv.processor;

import com.alibaba.fastjson.serializer.SerializerFeature;
import io.netty.channel.ChannelHandlerContext;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.sail.mq.common.MQVersion;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.common.help.FAQUrl;
import org.sail.mq.common.namesrv.NamesrvUtil;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.namesrv.NamesrvController;
import org.sail.mq.remoting.exception.RemotingCommandException;
import org.sail.mq.remoting.netty.NettyRequestProcessor;
import org.sail.mq.remoting.protocol.RemotingCommand;
import org.sail.mq.remoting.protocol.ResponseCode;
import org.sail.mq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.sail.mq.remoting.protocol.route.TopicRouteData;

public class ClientRequestProcessor implements NettyRequestProcessor {

    private static Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    protected NamesrvController namesrvController;
    private long startupTimeMillis;

    private AtomicBoolean needCheckNamesrvReady = new AtomicBoolean(true);

    public ClientRequestProcessor(final NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
        this.startupTimeMillis = System.currentTimeMillis();
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        final RemotingCommand request) throws Exception {
        return this.getRouteInfoByTopic(ctx, request);
    }

    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
            (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        boolean namesrvReady = needCheckNamesrvReady.get() && System.currentTimeMillis() - startupTimeMillis >= TimeUnit.SECONDS.toMillis(namesrvController.getNamesrvConfig().getWaitSecondsForService());

        if (namesrvController.getNamesrvConfig().isNeedWaitForService() && !namesrvReady) {
            log.warn("name server not ready. request code {} ", request.getCode());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("name server not ready");
            return response;
        }

        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());

        if (topicRouteData != null) {
            //topic route info register success ,so disable namesrvReady check
            if (needCheckNamesrvReady.get()) {
                needCheckNamesrvReady.set(false);
            }

            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                String orderTopicConf =
                    this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
                        requestHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }

            byte[] content;
            Boolean standardJsonOnly = Optional.ofNullable(requestHeader.getAcceptStandardJsonOnly()).orElse(false);
            if (request.getVersion() >= MQVersion.Version.V4_9_4.ordinal() || standardJsonOnly) {
                content = topicRouteData.encode(SerializerFeature.BrowserCompatible,
                    SerializerFeature.QuoteFieldNames, SerializerFeature.SkipTransientField,
                    SerializerFeature.MapSortField);
            } else {
                content = topicRouteData.encode();
            }

            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
            + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
