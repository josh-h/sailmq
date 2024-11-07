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
package org.apache.rocketmq.tools.command.auth;

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.sail.mq.remoting.RPCHook;
import org.sail.mq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateUserSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateUser";
    }

    @Override
    public String commandDesc() {
        return "Update user to cluster.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("c", "clusterName", true, "update user to which cluster");
        optionGroup.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "update user to which broker");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("u", "username", true, "the username of user to update.");
        opt.setRequired(true);
        options.addOption(opt);

        optionGroup = new OptionGroup();
        opt = new Option("p", "password", true, "the password of user to update");
        optionGroup.addOption(opt);

        opt = new Option("t", "userType", true, "the userType of user to update");
        optionGroup.addOption(opt);

        opt = new Option("s", "userStatus", true, "the userStatus of user to update");
        optionGroup.addOption(opt);
        optionGroup.setRequired(true);

        options.addOptionGroup(optionGroup);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String username = StringUtils.trim(commandLine.getOptionValue('u'));
            String password = StringUtils.trim(commandLine.getOptionValue('p'));
            String userType = StringUtils.trim(commandLine.getOptionValue('t'));
            String userStatus = StringUtils.trim(commandLine.getOptionValue('s'));

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                defaultMQAdminExt.updateUser(addr, username, password, userType, userStatus);

                System.out.printf("update user to %s success.%n", addr);
                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();
                Set<String> brokerAddrSet =
                    CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : brokerAddrSet) {
                    defaultMQAdminExt.updateUser(addr, username, password, userType, userStatus);
                    System.out.printf("update user to %s success.%n", addr);
                }
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
