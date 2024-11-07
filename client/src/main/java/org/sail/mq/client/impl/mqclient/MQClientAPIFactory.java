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
package org.sail.mq.client.impl.mqclient;

import com.google.common.base.Strings;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.sail.mq.client.ClientConfig;
import org.sail.mq.client.common.NameserverAccessConfig;
import org.sail.mq.client.impl.ClientRemotingProcessor;
import org.sail.mq.common.MixAll;
import org.sail.mq.common.utils.AsyncShutdownHelper;
import org.sail.mq.common.utils.StartAndShutdown;
import org.sail.mq.remoting.RPCHook;
import org.sail.mq.remoting.netty.NettyClientConfig;

public class MQClientAPIFactory implements StartAndShutdown {

    private MQClientAPIExt[] clients;
    private final String namePrefix;
    private final int clientNum;
    private final ClientRemotingProcessor clientRemotingProcessor;
    private final RPCHook rpcHook;
    private final ScheduledExecutorService scheduledExecutorService;
    private final NameserverAccessConfig nameserverAccessConfig;

    public MQClientAPIFactory(NameserverAccessConfig nameserverAccessConfig, String namePrefix, int clientNum,
        ClientRemotingProcessor clientRemotingProcessor,
        RPCHook rpcHook, ScheduledExecutorService scheduledExecutorService) {
        this.nameserverAccessConfig = nameserverAccessConfig;
        this.namePrefix = namePrefix;
        this.clientNum = clientNum;
        this.clientRemotingProcessor = clientRemotingProcessor;
        this.rpcHook = rpcHook;
        this.scheduledExecutorService = scheduledExecutorService;

        this.init();
    }

    protected void init() {
        System.setProperty(ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false");
        if (StringUtils.isEmpty(nameserverAccessConfig.getNamesrvDomain())) {
            if (Strings.isNullOrEmpty(nameserverAccessConfig.getNamesrvAddr())) {
                throw new RuntimeException("The configuration item NamesrvAddr is not configured");
            }
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, nameserverAccessConfig.getNamesrvAddr());
        } else {
            System.setProperty("sailmq.namesrv.domain", nameserverAccessConfig.getNamesrvDomain());
            System.setProperty("sailmq.namesrv.domain.subgroup", nameserverAccessConfig.getNamesrvDomainSubgroup());
        }
    }

    public MQClientAPIExt getClient() {
        if (clients.length == 1) {
            return this.clients[0];
        }
        int index = ThreadLocalRandom.current().nextInt(this.clients.length);
        return this.clients[index];
    }

    @Override
    public void start() throws Exception {
        this.clients = new MQClientAPIExt[this.clientNum];

        for (int i = 0; i < this.clientNum; i++) {
            clients[i] = createAndStart(this.namePrefix + "N_" + i);
        }
    }

    @Override
    public void shutdown() throws Exception {
        AsyncShutdownHelper helper = new AsyncShutdownHelper();
        for (int i = 0; i < this.clientNum; i++) {
            helper.addTarget(clients[i]);
        }
        helper.shutdown().await(Integer.MAX_VALUE, TimeUnit.SECONDS);
    }

    protected MQClientAPIExt createAndStart(String instanceName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setInstanceName(instanceName);
        clientConfig.setDecodeReadBody(true);
        clientConfig.setDecodeDecompressBody(false);

        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyClientConfig.setDisableCallbackExecutor(true);

        MQClientAPIExt mqClientAPIExt = new MQClientAPIExt(clientConfig, nettyClientConfig,
            clientRemotingProcessor,
            rpcHook);

        if (!mqClientAPIExt.updateNameServerAddressList()) {
            mqClientAPIExt.fetchNameServerAddr();
            this.scheduledExecutorService.scheduleAtFixedRate(
                mqClientAPIExt::fetchNameServerAddr,
                Duration.ofSeconds(10).toMillis(),
                Duration.ofMinutes(2).toMillis(),
                TimeUnit.MILLISECONDS
            );
        }
        mqClientAPIExt.start();
        return mqClientAPIExt;
    }
}
