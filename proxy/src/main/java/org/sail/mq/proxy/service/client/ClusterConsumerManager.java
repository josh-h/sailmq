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

import java.util.Set;
import org.sail.mq.broker.client.ClientChannelInfo;
import org.sail.mq.broker.client.ConsumerIdsChangeListener;
import org.sail.mq.broker.client.ConsumerManager;
import org.sail.mq.common.consumer.ConsumeFromWhere;
import org.sail.mq.common.utils.StartAndShutdown;
import org.sail.mq.proxy.service.admin.AdminService;
import org.sail.mq.client.impl.mqclient.MQClientAPIFactory;
import org.sail.mq.proxy.service.route.TopicRouteService;
import org.sail.mq.proxy.service.sysmessage.HeartbeatSyncer;
import org.sail.mq.remoting.RPCHook;
import org.sail.mq.remoting.protocol.heartbeat.ConsumeType;
import org.sail.mq.remoting.protocol.heartbeat.MessageModel;
import org.sail.mq.remoting.protocol.heartbeat.SubscriptionData;

public class ClusterConsumerManager extends ConsumerManager implements StartAndShutdown {

    protected HeartbeatSyncer heartbeatSyncer;

    public ClusterConsumerManager(TopicRouteService topicRouteService, AdminService adminService,
                                  MQClientAPIFactory mqClientAPIFactory, ConsumerIdsChangeListener consumerIdsChangeListener, long channelExpiredTimeout, RPCHook rpcHook) {
        super(consumerIdsChangeListener, channelExpiredTimeout);
        this.heartbeatSyncer = new HeartbeatSyncer(topicRouteService, adminService, this, mqClientAPIFactory, rpcHook);
    }

    @Override
    public boolean registerConsumer(String group, ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable, boolean updateSubscription) {
        this.heartbeatSyncer.onConsumerRegister(group, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList);
        return super.registerConsumer(group, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList,
            isNotifyConsumerIdsChangedEnable, updateSubscription);
    }

    @Override
    public void unregisterConsumer(String group, ClientChannelInfo clientChannelInfo,
        boolean isNotifyConsumerIdsChangedEnable) {
        this.heartbeatSyncer.onConsumerUnRegister(group, clientChannelInfo);
        super.unregisterConsumer(group, clientChannelInfo, isNotifyConsumerIdsChangedEnable);
    }

    @Override
    public void shutdown() throws Exception {
        this.heartbeatSyncer.shutdown();
    }

    @Override
    public void start() throws Exception {
        this.heartbeatSyncer.start();
    }
}
