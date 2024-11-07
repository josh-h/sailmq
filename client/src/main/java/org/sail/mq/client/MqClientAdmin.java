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

package org.sail.mq.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.common.message.MessageQueue;
import org.sail.mq.remoting.protocol.admin.ConsumeStats;
import org.sail.mq.remoting.protocol.admin.TopicStatsTable;
import org.sail.mq.remoting.protocol.body.ClusterInfo;
import org.sail.mq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.sail.mq.remoting.protocol.body.ConsumerConnection;
import org.sail.mq.remoting.protocol.body.ConsumerRunningInfo;
import org.sail.mq.remoting.protocol.body.GroupList;
import org.sail.mq.remoting.protocol.body.QueueTimeSpan;
import org.sail.mq.remoting.protocol.body.TopicList;
import org.sail.mq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.sail.mq.remoting.protocol.header.CreateTopicRequestHeader;
import org.sail.mq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.sail.mq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.sail.mq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.sail.mq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.sail.mq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.sail.mq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.sail.mq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.sail.mq.remoting.protocol.header.QueryMessageRequestHeader;
import org.sail.mq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.sail.mq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.sail.mq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.sail.mq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.sail.mq.remoting.protocol.header.ViewMessageRequestHeader;
import org.sail.mq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.sail.mq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.sail.mq.remoting.protocol.heartbeat.SubscriptionData;
import org.sail.mq.remoting.protocol.subscription.SubscriptionGroupConfig;

public interface MqClientAdmin {
    CompletableFuture<List<MessageExt>> queryMessage(String address, boolean uniqueKeyFlag, boolean decompressBody,
        QueryMessageRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<TopicStatsTable> getTopicStatsInfo(String address,
        GetTopicStatsInfoRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<List<QueueTimeSpan>> queryConsumeTimeSpan(String address,
        QueryConsumeTimeSpanRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<Void> updateOrCreateTopic(String address, CreateTopicRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Void> updateOrCreateSubscriptionGroup(String address, SubscriptionGroupConfig config,
        long timeoutMillis);

    CompletableFuture<Void> deleteTopicInBroker(String address, DeleteTopicRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Void> deleteTopicInNameserver(String address, DeleteTopicFromNamesrvRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Void> deleteKvConfig(String address, DeleteKVConfigRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Void> deleteSubscriptionGroup(String address, DeleteSubscriptionGroupRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Map<MessageQueue, Long>> invokeBrokerToResetOffset(String address,
        ResetOffsetRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<MessageExt> viewMessage(String address, ViewMessageRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<ClusterInfo> getBrokerClusterInfo(String address, long timeoutMillis);

    CompletableFuture<ConsumerConnection> getConsumerConnectionList(String address,
        GetConsumerConnectionListRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<TopicList> queryTopicsByConsumer(String address,
        QueryTopicsByConsumerRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<SubscriptionData> querySubscriptionByConsumer(String address,
        QuerySubscriptionByConsumerRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumeStats> getConsumeStats(String address, GetConsumeStatsRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<GroupList> queryTopicConsumeByWho(String address,
        QueryTopicConsumeByWhoRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumerRunningInfo> getConsumerRunningInfo(String address,
        GetConsumerRunningInfoRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumeMessageDirectlyResult> consumeMessageDirectly(String address,
        ConsumeMessageDirectlyResultRequestHeader requestHeader, long timeoutMillis);
}
