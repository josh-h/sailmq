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
package org.apache.rocketmq.tools.command.message;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.sail.mq.client.ClientConfig;
import org.sail.mq.client.QueryResult;
import org.sail.mq.client.exception.MQBrokerException;
import org.sail.mq.client.exception.MQClientException;
import org.sail.mq.client.impl.MQAdminImpl;
import org.sail.mq.client.impl.MQClientAPIImpl;
import org.sail.mq.client.impl.MQClientManager;
import org.sail.mq.client.impl.factory.MQClientInstance;
import org.sail.mq.common.MixAll;
import org.sail.mq.common.message.MessageExt;
import org.sail.mq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryMsgByUniqueKeySubCommandTest {

    private static QueryMsgByUniqueKeySubCommand cmd = new QueryMsgByUniqueKeySubCommand();

    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());

    private static MQClientAPIImpl mQClientAPIImpl;
    private static MQAdminImpl mQAdminImpl;

    @Before
    public void before() throws NoSuchFieldException, IllegalAccessException, InterruptedException, RemotingException, MQClientException, MQBrokerException {

        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        mQAdminImpl = mock(MQAdminImpl.class);

        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);

        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);

        field = MQClientInstance.class.getDeclaredField("mQAdminImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQAdminImpl);

        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);

        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setConsumeResult(CMResult.CR_SUCCESS);
        result.setRemark("customRemark_122333444");
        when(mQClientAPIImpl.consumeMessageDirectly(anyString(), anyString(), anyString(), anyString(), anyString(), anyLong())).thenReturn(result);

        MessageExt retMsgExt = new MessageExt();
        retMsgExt.setMsgId("0A3A54F7BF7D18B4AAC28A3FA2CF0000");
        retMsgExt.setBody("this is message ext body".getBytes());
        retMsgExt.setTopic("testTopic");
        retMsgExt.setTags("testTags");
        retMsgExt.setStoreHost(new InetSocketAddress("127.0.0.1", 8899));
        retMsgExt.setBornHost(new InetSocketAddress("127.0.0.1", 7788));
        retMsgExt.setQueueId(1);
        retMsgExt.setQueueOffset(12L);
        retMsgExt.setCommitLogOffset(123);
        retMsgExt.setReconsumeTimes(2);
        retMsgExt.setBornTimestamp(System.currentTimeMillis());
        retMsgExt.setStoreTimestamp(System.currentTimeMillis());
        when(mQAdminImpl.viewMessage(anyString(), anyString())).thenReturn(retMsgExt);

        when(mQAdminImpl.queryMessageByUniqKey(anyString(), anyString())).thenReturn(retMsgExt);

        QueryResult queryResult = new QueryResult(0, Lists.newArrayList(retMsgExt));
        when(mQAdminImpl.queryMessageByUniqKey(anyString(), anyString(), anyString(), anyInt(), anyLong(), anyLong())).thenReturn(queryResult);

        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, "127.0.0.1:9876");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);

        GroupList groupList = new GroupList();
        HashSet<String> groupSets = new HashSet<>();
        groupSets.add("testGroup");
        groupList.setGroupList(groupSets);
        when(mQClientAPIImpl.queryTopicConsumeByWho(anyString(), anyString(), anyLong())).thenReturn(groupList);

        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setConsumeTps(100 * 10000);
        HashMap<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();
        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setBrokerName("messageQueue BrokerName testing");
        messageQueue.setTopic("messageQueue topic");
        messageQueue.setQueueId(1);
        OffsetWrapper offsetWrapper = new OffsetWrapper();
        offsetWrapper.setBrokerOffset(100);
        offsetWrapper.setConsumerOffset(200);
        offsetWrapper.setLastTimestamp(System.currentTimeMillis());
        offsetTable.put(messageQueue, offsetWrapper);
        consumeStats.setOffsetTable(offsetTable);
        when(mQClientAPIImpl.getConsumeStats(anyString(), anyString(), (String) isNull(), anyLong())).thenReturn(consumeStats);

        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<>();
        brokerAddrTable.put("key", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        HashMap<String, Set<String>> clusterAddrTable = new HashMap<>();
        Set<String> addrSet = new HashSet<>();
        addrSet.add("127.0.0.1:9876");
        clusterAddrTable.put("key", addrSet);
        clusterInfo.setClusterAddrTable(clusterAddrTable);
        when(mQClientAPIImpl.getBrokerClusterInfo(anyLong())).thenReturn(clusterInfo);

        field = QueryMsgByUniqueKeySubCommand.class.getDeclaredField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(cmd, defaultMQAdminExt);

    }

    @Test
    public void testExecuteConsumeActively() throws SubCommandException, InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {

        ConsumerConnection consumerConnection = new ConsumerConnection();
        consumerConnection.setConsumeType(ConsumeType.CONSUME_ACTIVELY);
        HashSet<Connection> connectionSet = new HashSet<>();
        Connection conn = new Connection();
        conn.setClientId("clientIdTest");
        conn.setClientAddr("clientAddrTest");
        conn.setLanguage(LanguageCode.JAVA);
        conn.setVersion(1);
        connectionSet.add(conn);
        consumerConnection.setConnectionSet(connectionSet);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);

        Options options = ServerUtil.buildCommandlineOptions(new Options());

        String[] args = new String[] {"-t myTopicTest", "-i msgId", "-c DefaultCluster"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args,
            cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);

    }

    @Test
    public void testExecuteConsumePassively() throws SubCommandException, InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {

        ConsumerConnection consumerConnection = new ConsumerConnection();
        consumerConnection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        HashSet<Connection> connectionSet = new HashSet<>();
        Connection conn = new Connection();
        conn.setClientId("clientIdTestStr");
        conn.setClientAddr("clientAddrTestStr");
        conn.setLanguage(LanguageCode.JAVA);
        conn.setVersion(2);
        connectionSet.add(conn);
        consumerConnection.setConnectionSet(connectionSet);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);

        Options options = ServerUtil.buildCommandlineOptions(new Options());

        String[] args = new String[] {"-t myTopicTest", "-i 7F000001000004D20000000000000066", "-c DefaultCluster"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args,
            cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);

    }

    @Test
    public void testExecuteWithConsumerGroupAndClientId() throws SubCommandException {

        Options options = ServerUtil.buildCommandlineOptions(new Options());

        String[] args = new String[] {"-t myTopicTest", "-i 0A3A54F7BF7D18B4AAC28A3FA2CF0000", "-g producerGroupName", "-d clientId", "-c DefaultCluster"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args,
            cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);
    }

    @Test
    public void testExecute() throws SubCommandException {

        System.setProperty("sailmq.namesrv.addr", "127.0.0.1:9876");

        String[] args = new String[]{"-t myTopicTest", "-i 0A3A54F7BF7D18B4AAC28A3FA2CF0000", "-c DefaultCluster"};
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args,
            cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);

        args = new String[] {"-t myTopicTest", "-i 0A3A54F7BF7D18B4AAC28A3FA2CF0000", "-g producerGroupName", "-d clientId", "-c DefaultCluster"};
        commandLine = ServerUtil.parseCmdLine("mqadmin ", args, cmd.buildCommandlineOptions(options),
            new DefaultParser());
        cmd.execute(commandLine, options, null);

    }
}
