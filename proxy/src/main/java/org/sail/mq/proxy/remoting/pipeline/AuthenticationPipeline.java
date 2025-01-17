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

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import org.sail.mq.acl.AccessResource;
import org.sail.mq.acl.AccessValidator;
import org.sail.mq.auth.authentication.AuthenticationEvaluator;
import org.sail.mq.auth.authentication.context.AuthenticationContext;
import org.sail.mq.auth.authentication.exception.AuthenticationException;
import org.sail.mq.auth.authentication.factory.AuthenticationFactory;
import org.sail.mq.auth.config.AuthConfig;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.config.ConfigurationManager;
import org.sail.mq.proxy.config.ProxyConfig;
import org.sail.mq.proxy.processor.MessagingProcessor;
import org.sail.mq.remoting.protocol.RemotingCommand;

public class AuthenticationPipeline implements RequestPipeline {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final List<AccessValidator> accessValidatorList;
    private final AuthConfig authConfig;
    private final AuthenticationEvaluator authenticationEvaluator;

    public AuthenticationPipeline(List<AccessValidator> accessValidatorList, AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.accessValidatorList = accessValidatorList;
        this.authConfig = authConfig;
        this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws Exception {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        if (config.isEnableACL()) {
            for (AccessValidator accessValidator : accessValidatorList) {
                AccessResource accessResource = accessValidator.parse(request, context.getRemoteAddress());
                accessValidator.validate(accessResource);
            }
        }

        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        try {
            AuthenticationContext authenticationContext = newContext(ctx, request, context);
            authenticationEvaluator.evaluate(authenticationContext);
        } catch (AuthenticationException ex) {
            throw ex;
        } catch (Throwable ex) {
            LOGGER.error("authenticate failed, request:{}", request, ex);
            throw ex;
        }
    }

    protected AuthenticationContext newContext(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) {
        return AuthenticationFactory.newContext(authConfig, ctx, request);
    }
}
