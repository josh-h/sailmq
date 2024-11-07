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
package org.sail.mq.tools.command.export;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Arrays;
import java.util.Properties;

import com.alibaba.fastjson.JSON;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.sail.mq.common.MixAll;
import org.sail.mq.remoting.RPCHook;
import org.sail.mq.tools.admin.DefaultMQAdminExt;
import org.sail.mq.tools.command.CommandUtil;
import org.sail.mq.tools.command.SubCommand;
import org.sail.mq.tools.command.SubCommandException;

public class ExportConfigsCommand implements SubCommand {
    @Override
    public String commandName() {
        return "exportConfigs";
    }

    @Override
    public String commandDesc() {
        return "Export configs.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "choose a cluster to export");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "filePath", true,
            "export configs.json path | default /tmp/sailmq/export");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook)
        throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String clusterName = commandLine.getOptionValue('c').trim();
            String filePath = !commandLine.hasOption('f') ? "/tmp/sailmq/export" : commandLine.getOptionValue('f')
                .trim();

            defaultMQAdminExt.start();
            Map<String, Object> result = new HashMap<>();
            // name servers
            List<String> nameServerAddressList = defaultMQAdminExt.getNameServerAddressList();

            //broker
            int masterBrokerSize = 0;
            int slaveBrokerSize = 0;
            Map<String, Properties> brokerConfigs = new HashMap<>();
            Map<String, List<String>> masterAndSlaveMap
                = CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExt, clusterName);
            for (Entry<String, List<String>> masterAndSlaveEntry : masterAndSlaveMap.entrySet()) {
                Properties masterProperties = defaultMQAdminExt.getBrokerConfig(masterAndSlaveEntry.getKey());
                masterBrokerSize++;
                slaveBrokerSize += masterAndSlaveEntry.getValue().size();

                brokerConfigs.put(masterProperties.getProperty("brokerName"), needBrokerProprties(masterProperties));
            }

            Map<String, Integer> clusterScaleMap = new HashMap<>();
            clusterScaleMap.put("namesrvSize", nameServerAddressList.size());
            clusterScaleMap.put("masterBrokerSize", masterBrokerSize);
            clusterScaleMap.put("slaveBrokerSize", slaveBrokerSize);

            result.put("brokerConfigs", brokerConfigs);
            result.put("clusterScale", clusterScaleMap);

            String path = filePath + "/configs.json";
            MixAll.string2FileNotSafe(JSON.toJSONString(result, true), path);
            System.out.printf("export %s success", path);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }


    private Properties needBrokerProprties(Properties properties) {
        List<String> propertyKeys = Arrays.asList(
                "brokerClusterName",
                "brokerId",
                "brokerName",
                "brokerRole",
                "fileReservedTime",
                "filterServerNums",
                "flushDiskType",
                "maxMessageSize",
                "messageDelayLevel",
                "msgTraceTopicName",
                "slaveReadEnable",
                "traceOn",
                "traceTopicEnable",
                "useTLS",
                "autoCreateTopicEnable",
                "autoCreateSubscriptionGroup"
        );

        Properties newProperties = new Properties();
        propertyKeys.stream()
                .filter(key -> properties.getProperty(key) != null)
                .forEach(key -> newProperties.setProperty(key, properties.getProperty(key)));

        return newProperties;
    }

}