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
package org.sail.mq.tools.command.consumer;

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.sail.mq.common.MixAll;
import org.sail.mq.remoting.RPCHook;
import org.sail.mq.srvutil.ServerUtil;
import org.sail.mq.tools.admin.DefaultMQAdminExt;
import org.sail.mq.tools.command.CommandUtil;
import org.sail.mq.tools.command.SubCommand;
import org.sail.mq.tools.command.SubCommandException;

public class DeleteSubscriptionGroupCommand implements SubCommand {
    @Override
    public String commandName() {
        return "deleteSubGroup";
    }

    @Override
    public String commandDesc() {
        return "Delete subscription group from broker.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "delete subscription group from which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete subscription group from which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupName", true, "subscription group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("r", "removeOffset", true, "remove offset");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // groupName
            String groupName = commandLine.getOptionValue('g').trim();
            boolean cleanOffset = false;
            if (commandLine.hasOption('r')) {
                try {
                    cleanOffset = Boolean.valueOf(commandLine.getOptionValue('r').trim());
                } catch (Exception e) {
                }
            }

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                adminExt.start();

                adminExt.deleteSubscriptionGroup(addr, groupName, cleanOffset);
                System.out.printf("delete subscription group [%s] from broker [%s] success.%n", groupName,
                    addr);

                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                adminExt.start();

                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
                for (String master : masterSet) {
                    adminExt.deleteSubscriptionGroup(master, groupName, cleanOffset);
                    System.out.printf(
                        "delete subscription group [%s] from broker [%s] in cluster [%s] success.%n",
                        groupName, master, clusterName);
                }

                try {
                    adminExt.deleteTopic(MixAll.RETRY_GROUP_TOPIC_PREFIX + groupName, clusterName);
                    adminExt.deleteTopic(MixAll.DLQ_GROUP_TOPIC_PREFIX + groupName, clusterName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }
}
