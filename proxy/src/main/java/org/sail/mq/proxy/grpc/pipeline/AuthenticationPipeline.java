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
package org.sail.mq.proxy.grpc.pipeline;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.Metadata;
import org.apache.commons.lang3.StringUtils;
import org.sail.mq.auth.authentication.AuthenticationEvaluator;
import org.sail.mq.auth.authentication.context.AuthenticationContext;
import org.sail.mq.auth.authentication.context.DefaultAuthenticationContext;
import org.sail.mq.auth.authentication.exception.AuthenticationException;
import org.sail.mq.auth.authentication.factory.AuthenticationFactory;
import org.sail.mq.auth.config.AuthConfig;
import org.sail.mq.common.constant.GrpcConstants;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.common.utils.GrpcUtils;
import org.sail.mq.proxy.processor.MessagingProcessor;

public class AuthenticationPipeline implements RequestPipeline {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final AuthConfig authConfig;
    private final AuthenticationEvaluator authenticationEvaluator;

    public AuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        try {
            Metadata metadata = GrpcConstants.METADATA.get(Context.current());
            AuthenticationContext authenticationContext = newContext(context, metadata, request);
            authenticationEvaluator.evaluate(authenticationContext);
        } catch (AuthenticationException ex) {
            throw ex;
        } catch (Throwable ex) {
            LOGGER.error("authenticate failed, request:{}", request, ex);
            throw ex;
        }
    }

    /**
     * Create Context, for extension
     *
     * @param context for extension
     * @param headers gRPC headers
     * @param request
     * @return
     */
    protected AuthenticationContext newContext(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        AuthenticationContext result = AuthenticationFactory.newContext(authConfig, headers, request);
        if (result instanceof DefaultAuthenticationContext) {
            DefaultAuthenticationContext defaultAuthenticationContext = (DefaultAuthenticationContext) result;
            if (StringUtils.isNotBlank(defaultAuthenticationContext.getUsername())) {
                GrpcUtils.putHeaderIfNotExist(headers, GrpcConstants.AUTHORIZATION_AK, defaultAuthenticationContext.getUsername());
            }
        }
        return result;
    }
}
