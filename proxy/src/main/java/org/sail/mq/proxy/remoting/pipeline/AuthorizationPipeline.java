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
import org.sail.mq.auth.authentication.exception.AuthenticationException;
import org.sail.mq.auth.authorization.AuthorizationEvaluator;
import org.sail.mq.auth.authorization.context.AuthorizationContext;
import org.sail.mq.auth.authorization.exception.AuthorizationException;
import org.sail.mq.auth.authorization.factory.AuthorizationFactory;
import org.sail.mq.auth.config.AuthConfig;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.processor.MessagingProcessor;
import org.sail.mq.remoting.protocol.RemotingCommand;

public class AuthorizationPipeline implements RequestPipeline {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final AuthConfig authConfig;
    private final AuthorizationEvaluator authorizationEvaluator;

    public AuthorizationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authorizationEvaluator = AuthorizationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws Exception {
        if (!authConfig.isAuthorizationEnabled()) {
            return;
        }
        try {
            List<AuthorizationContext> contexts = newContexts(request, ctx, context);
            authorizationEvaluator.evaluate(contexts);
        } catch (AuthorizationException | AuthenticationException ex) {
            throw ex;
        }  catch (Throwable ex) {
            LOGGER.error("authorize failed, request:{}", request, ex);
            throw ex;
        }
    }

    protected List<AuthorizationContext> newContexts(RemotingCommand request, ChannelHandlerContext ctx, ProxyContext context) {
        return AuthorizationFactory.newContexts(authConfig, ctx, request);
    }
}
