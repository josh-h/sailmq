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
import org.sail.mq.broker.client.ClientChannelInfo;
import org.sail.mq.broker.client.ConsumerGroupEvent;
import org.sail.mq.broker.client.ConsumerIdsChangeListener;
import org.sail.mq.broker.client.ConsumerManager;
import org.sail.mq.broker.client.ProducerChangeListener;
import org.sail.mq.broker.client.ProducerGroupEvent;
import org.sail.mq.broker.client.ProducerManager;
import org.sail.mq.client.common.NameserverAccessConfig;
import org.sail.mq.client.impl.mqclient.DoNothingClientRemotingProcessor;
import org.sail.mq.client.impl.mqclient.MQClientAPIFactory;
import org.sail.mq.common.constant.LoggerName;
import org.sail.mq.common.utils.AbstractStartAndShutdown;
import org.sail.mq.common.utils.ThreadUtils;
import org.sail.mq.logging.org.slf4j.Logger;
import org.sail.mq.logging.org.slf4j.LoggerFactory;
import org.sail.mq.proxy.common.ProxyContext;
import org.sail.mq.proxy.config.ConfigurationManager;
import org.sail.mq.proxy.config.ProxyConfig;
import org.sail.mq.proxy.service.admin.AdminService;
import org.sail.mq.proxy.service.admin.DefaultAdminService;
import org.sail.mq.proxy.service.client.ClusterConsumerManager;
import org.sail.mq.proxy.service.client.ProxyClientRemotingProcessor;
import org.sail.mq.proxy.service.message.ClusterMessageService;
import org.sail.mq.proxy.service.message.MessageService;
import org.sail.mq.proxy.service.metadata.ClusterMetadataService;
import org.sail.mq.proxy.service.metadata.MetadataService;
import org.sail.mq.proxy.service.relay.ClusterProxyRelayService;
import org.sail.mq.proxy.service.relay.ProxyRelayService;
import org.sail.mq.proxy.service.route.ClusterTopicRouteService;
import org.sail.mq.proxy.service.route.TopicRouteService;
import org.sail.mq.proxy.service.transaction.ClusterTransactionService;
import org.sail.mq.proxy.service.transaction.TransactionService;
import org.sail.mq.remoting.RPCHook;

public class ClusterServiceManager extends AbstractStartAndShutdown implements ServiceManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected ClusterTransactionService clusterTransactionService;
    protected ProducerManager producerManager;
    protected ClusterConsumerManager consumerManager;
    protected TopicRouteService topicRouteService;
    protected MessageService messageService;
    protected ProxyRelayService proxyRelayService;
    protected ClusterMetadataService metadataService;
    protected AdminService adminService;

    protected ScheduledExecutorService scheduledExecutorService;
    protected MQClientAPIFactory messagingClientAPIFactory;
    protected MQClientAPIFactory operationClientAPIFactory;
    protected MQClientAPIFactory transactionClientAPIFactory;

    public ClusterServiceManager(RPCHook rpcHook) {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        NameserverAccessConfig nameserverAccessConfig = new NameserverAccessConfig(proxyConfig.getNamesrvAddr(),
            proxyConfig.getNamesrvDomain(), proxyConfig.getNamesrvDomainSubgroup());
        this.scheduledExecutorService = ThreadUtils.newScheduledThreadPool(3);

        this.messagingClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "ClusterMQClient_",
            proxyConfig.getRocketmqMQClientNum(),
            new DoNothingClientRemotingProcessor(null),
            rpcHook,
            scheduledExecutorService);
        this.operationClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "OperationClient_",
            1,
            new DoNothingClientRemotingProcessor(null),
            rpcHook,
            this.scheduledExecutorService
        );

        this.topicRouteService = new ClusterTopicRouteService(operationClientAPIFactory);
        this.messageService = new ClusterMessageService(this.topicRouteService, this.messagingClientAPIFactory);
        this.metadataService = new ClusterMetadataService(topicRouteService, operationClientAPIFactory);
        this.adminService = new DefaultAdminService(this.operationClientAPIFactory);

        this.producerManager = new ProducerManager();
        this.consumerManager = new ClusterConsumerManager(this.topicRouteService, this.adminService, this.operationClientAPIFactory, new ConsumerIdsChangeListenerImpl(), proxyConfig.getChannelExpiredTimeout(), rpcHook);

        this.transactionClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "ClusterTransaction_",
            1,
            new ProxyClientRemotingProcessor(producerManager),
            rpcHook,
            scheduledExecutorService);
        this.clusterTransactionService = new ClusterTransactionService(this.topicRouteService, this.producerManager,
            this.transactionClientAPIFactory);
        this.proxyRelayService = new ClusterProxyRelayService(this.clusterTransactionService);

        this.init();
    }

    protected void init() {
        this.producerManager.appendProducerChangeListener(new ProducerChangeListenerImpl());

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                producerManager.scanNotActiveChannel();
                consumerManager.scanNotActiveChannel();
            } catch (Throwable e) {
                log.error("Error occurred when scan not active client channels.", e);
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);

        this.appendShutdown(scheduledExecutorService::shutdown);
        this.appendStartAndShutdown(this.messagingClientAPIFactory);
        this.appendStartAndShutdown(this.operationClientAPIFactory);
        this.appendStartAndShutdown(this.transactionClientAPIFactory);
        this.appendStartAndShutdown(this.topicRouteService);
        this.appendStartAndShutdown(this.clusterTransactionService);
        this.appendStartAndShutdown(this.metadataService);
        this.appendStartAndShutdown(this.consumerManager);
    }

    @Override
    public MessageService getMessageService() {
        return this.messageService;
    }

    @Override
    public TopicRouteService getTopicRouteService() {
        return topicRouteService;
    }

    @Override
    public ProducerManager getProducerManager() {
        return this.producerManager;
    }

    @Override
    public ConsumerManager getConsumerManager() {
        return this.consumerManager;
    }

    @Override
    public TransactionService getTransactionService() {
        return this.clusterTransactionService;
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

    protected static class ConsumerIdsChangeListenerImpl implements ConsumerIdsChangeListener {

        @Override
        public void handle(ConsumerGroupEvent event, String group, Object... args) {

        }

        @Override
        public void shutdown() {

        }
    }

    protected class ProducerChangeListenerImpl implements ProducerChangeListener {
        @Override
        public void handle(ProducerGroupEvent event, String group, ClientChannelInfo clientChannelInfo) {
            if (event == ProducerGroupEvent.GROUP_UNREGISTER) {
                getTransactionService().unSubscribeAllTransactionTopic(ProxyContext.createForInner(this.getClass()), group);
            }
        }
    }
}
