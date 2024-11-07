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
package org.sail.mq.client.trace.hook;

import java.util.ArrayList;
import org.sail.mq.client.hook.SendMessageContext;
import org.sail.mq.client.hook.SendMessageHook;
import org.sail.mq.client.producer.SendStatus;
import org.sail.mq.client.trace.AsyncTraceDispatcher;
import org.sail.mq.client.trace.TraceBean;
import org.sail.mq.client.trace.TraceContext;
import org.sail.mq.client.trace.TraceDispatcher;
import org.sail.mq.client.trace.TraceType;
import org.sail.mq.remoting.protocol.NamespaceUtil;

public class SendMessageTraceHookImpl implements SendMessageHook {

    private TraceDispatcher localDispatcher;

    public SendMessageTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "SendMessageTraceHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
            return;
        }
        //build the context content of TraceContext
        TraceContext traceContext = new TraceContext();
        traceContext.setTraceBeans(new ArrayList<>(1));
        context.setMqTraceContext(traceContext);
        traceContext.setTraceType(TraceType.Pub);
        traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));
        //build the data bean object of message trace
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
        traceBean.setTags(context.getMessage().getTags());
        traceBean.setKeys(context.getMessage().getKeys());
        traceBean.setStoreHost(context.getBrokerAddr());
        traceBean.setBodyLength(context.getMessage().getBody().length);
        traceBean.setMsgType(context.getMsgType());
        traceContext.getTraceBeans().add(traceBean);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())
            || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }

        if (context.getSendResult().getRegionId() == null
            || !context.getSendResult().isTraceOn()) {
            // if switch is false,skip it
            return;
        }

        TraceContext traceContext = (TraceContext) context.getMqTraceContext();
        TraceBean traceBean = traceContext.getTraceBeans().get(0);
        int costTime = (int) ((System.currentTimeMillis() - traceContext.getTimeStamp()) / traceContext.getTraceBeans().size());
        traceContext.setCostTime(costTime);
        if (context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK)) {
            traceContext.setSuccess(true);
        } else {
            traceContext.setSuccess(false);
        }
        traceContext.setRegionId(context.getSendResult().getRegionId());
        traceBean.setMsgId(context.getSendResult().getMsgId());
        traceBean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
        traceBean.setStoreTime(traceContext.getTimeStamp() + costTime / 2);
        localDispatcher.append(traceContext);
    }
}
