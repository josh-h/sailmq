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

package org.sail.mq.tools.command.broker;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.sail.mq.client.exception.MQBrokerException;
import org.sail.mq.remoting.RPCHook;
import org.sail.mq.remoting.exception.RemotingConnectException;
import org.sail.mq.remoting.exception.RemotingSendRequestException;
import org.sail.mq.remoting.exception.RemotingTimeoutException;
import org.sail.mq.tools.admin.DefaultMQAdminExt;
import org.sail.mq.tools.admin.MQAdminExt;
import org.sail.mq.tools.command.CommandUtil;
import org.sail.mq.tools.command.SubCommand;
import org.sail.mq.tools.command.SubCommandException;

public class GetBrokerConfigCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getBrokerConfig";
    }

    @Override
    public String commandDesc() {
        return "Get broker config by cluster or special broker.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("b", "brokerAddr", true, "get which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "get which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        final RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {

            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();

                getAndPrint(defaultMQAdminExt,
                    String.format("============%s============\n", brokerAddr),
                    brokerAddr);

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                defaultMQAdminExt.start();

                Map<String, List<String>> masterAndSlaveMap
                    = CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExt, clusterName);

                for (String masterAddr : masterAndSlaveMap.keySet()) {

                    if (masterAddr == null) {
                        continue;
                    }

                    getAndPrint(
                        defaultMQAdminExt,
                        String.format("============Master: %s============\n", masterAddr),
                        masterAddr
                    );

                    for (String slaveAddr : masterAndSlaveMap.get(masterAddr)) {

                        if (slaveAddr == null) {
                            continue;
                        }

                        getAndPrint(
                            defaultMQAdminExt,
                            String.format("============My Master: %s=====Slave: %s============\n", masterAddr, slaveAddr),
                            slaveAddr
                        );
                    }
                }
            }

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    protected void getAndPrint(final MQAdminExt defaultMQAdminExt, final String printPrefix, final String addr)
        throws InterruptedException, RemotingConnectException,
        UnsupportedEncodingException, RemotingTimeoutException,
        MQBrokerException, RemotingSendRequestException {

        System.out.print(printPrefix);

        if (addr.equals(CommandUtil.NO_MASTER_PLACEHOLDER)) {
            return;
        }

        Properties properties = defaultMQAdminExt.getBrokerConfig(addr);
        if (properties == null) {
            System.out.printf("Broker[%s] has no config property!\n", addr);
            return;
        }

        for (Entry<Object, Object> entry : properties.entrySet()) {
            System.out.printf("%-50s=  %s\n", entry.getKey(), entry.getValue());
        }

        System.out.printf("%n");
    }
}
