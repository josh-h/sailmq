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
package org.sail.mq.proxy.service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.sail.mq.broker.BrokerController;
import org.sail.mq.broker.client.ConsumerManager;
import org.sail.mq.broker.client.ProducerManager;
import org.sail.mq.client.common.NameserverAccessConfig;
import org.sail.mq.client.impl.mqclient.DoNothingClientRemotingProcessor;
import org.sail.mq.client.impl.mqclient.MQClientAPIFactory;
import org.sail.mq.common.ThreadFactoryImpl;
import org.sail.mq.common.utils.AbstractStartAndShutdown;
import org.sail.mq.common.utils.StartAndShutdown;
import org.sail.mq.common.utils.ThreadUtils;
import org.sail.mq.proxy.config.ConfigurationManager;
import org.sail.mq.proxy.config.ProxyConfig;
import org.sail.mq.proxy.service.admin.AdminService;
import org.sail.mq.proxy.service.admin.DefaultAdminService;
import org.sail.mq.proxy.service.channel.ChannelManager;
import org.sail.mq.proxy.service.message.LocalMessageService;
import org.sail.mq.proxy.service.message.MessageService;
import org.sail.mq.proxy.service.metadata.LocalMetadataService;
import org.sail.mq.proxy.service.metadata.MetadataService;
import org.sail.mq.proxy.service.relay.LocalProxyRelayService;
import org.sail.mq.proxy.service.relay.ProxyRelayService;
import org.sail.mq.proxy.service.route.LocalTopicRouteService;
import org.sail.mq.proxy.service.route.TopicRouteService;
import org.sail.mq.proxy.service.transaction.LocalTransactionService;
import org.sail.mq.proxy.service.transaction.TransactionService;
import org.sail.mq.remoting.RPCHook;

public class LocalServiceManager extends AbstractStartAndShutdown implements ServiceManager {

    private final BrokerController brokerController;
    private final TopicRouteService topicRouteService;
    private final MessageService messageService;
    private final TransactionService transactionService;
    private final ProxyRelayService proxyRelayService;
    private final MetadataService metadataService;
    private final AdminService adminService;

    private final MQClientAPIFactory mqClientAPIFactory;
    private final ChannelManager channelManager;

    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(
        new ThreadFactoryImpl("LocalServiceManagerScheduledThread"));

    public LocalServiceManager(BrokerController brokerController, RPCHook rpcHook) {
        this.brokerController = brokerController;
        this.channelManager = new ChannelManager();
        this.messageService = new LocalMessageService(brokerController, channelManager, rpcHook);
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        NameserverAccessConfig nameserverAccessConfig = new NameserverAccessConfig(proxyConfig.getNamesrvAddr(),
            proxyConfig.getNamesrvDomain(), proxyConfig.getNamesrvDomainSubgroup());
        this.mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "LocalMQClient_",
            1,
            new DoNothingClientRemotingProcessor(null),
            rpcHook,
            scheduledExecutorService
        );
        this.topicRouteService = new LocalTopicRouteService(brokerController, mqClientAPIFactory);
        this.transactionService = new LocalTransactionService(brokerController.getBrokerConfig());
        this.proxyRelayService = new LocalProxyRelayService(brokerController, this.transactionService);
        this.metadataService = new LocalMetadataService(brokerController);
        this.adminService = new DefaultAdminService(this.mqClientAPIFactory);
        this.init();
    }

    protected void init() {
        this.appendStartAndShutdown(this.mqClientAPIFactory);
        this.appendStartAndShutdown(this.topicRouteService);
        this.appendStartAndShutdown(new LocalServiceManagerStartAndShutdown());
    }

    @Override
    public MessageService getMessageService() {
        return this.messageService;
    }

    @Override
    public TopicRouteService getTopicRouteService() {
        return this.topicRouteService;
    }

    @Override
    public ProducerManager getProducerManager() {
        return this.brokerController.getProducerManager();
    }

    @Override
    public ConsumerManager getConsumerManager() {
        return this.brokerController.getConsumerManager();
    }

    @Override
    public TransactionService getTransactionService() {
        return this.transactionService;
    }

    @Override
    public ProxyRelayService getProxyRelayService() {
        return this.proxyRelayService;
    }

    @Override
    public MetadataService getMetadataService() {
        return this.metadataService;
    }

    @Override
    public AdminService getAdminService() {
        return this.adminService;
    }

    private class LocalServiceManagerStartAndShutdown implements StartAndShutdown {
        @Override
        public void start() throws Exception {
            LocalServiceManager.this.scheduledExecutorService.scheduleWithFixedDelay(channelManager::scanAndCleanChannels, 5, 5, TimeUnit.MINUTES);
        }

        @Override
        public void shutdown() throws Exception {
            LocalServiceManager.this.scheduledExecutorService.shutdown();
        }
    }
}
