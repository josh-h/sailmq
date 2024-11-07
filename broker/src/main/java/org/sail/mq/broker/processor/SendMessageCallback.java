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

package org.sail.mq.broker.processor;

import org.sail.mq.broker.mqtrace.SendMessageContext;
import org.sail.mq.remoting.protocol.RemotingCommand;

public interface SendMessageCallback {
    /**
     * On send complete.
     *
     * @param ctx send context
     * @param response send response
     */
    void onComplete(SendMessageContext ctx, RemotingCommand response);
}
