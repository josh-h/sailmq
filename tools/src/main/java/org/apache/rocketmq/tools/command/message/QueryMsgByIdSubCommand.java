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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.sail.mq.client.exception.MQBrokerException;
import org.sail.mq.client.exception.MQClientException;
import org.sail.mq.client.producer.DefaultMQProducer;
import org.sail.mq.client.producer.SendResult;
import org.sail.mq.common.UtilAll;
import org.sail.mq.common.message.MessageClientExt;
import org.sail.mq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class QueryMsgByIdSubCommand implements SubCommand {
    public static void queryById(final DefaultMQAdminExt admin, final String clusterName, final String topic,
        final String msgId, final Charset msgBodyCharset) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException, IOException {
        MessageExt msg = admin.queryMessage(clusterName, topic, msgId);

        printMsg(admin, msg, msgBodyCharset);
    }

    public static void printMsg(final DefaultMQAdminExt admin, final MessageExt msg) throws IOException {
        printMsg(admin, msg, null);
    }

    public static void printMsg(final DefaultMQAdminExt admin, final MessageExt msg,
        final Charset msgBodyCharset) throws IOException {
        if (msg == null) {
            System.out.printf("%nMessage not found!");
            return;
        }

        String bodyTmpFilePath = createBodyFile(msg);
        String msgId = msg.getMsgId();
        if (msg instanceof MessageClientExt) {
            msgId = ((MessageClientExt) msg).getOffsetMsgId();
        }

        System.out.printf("%-20s %s%n",
            "OffsetID:",
            msgId
        );

        System.out.printf("%-20s %s%n",
            "Topic:",
            msg.getTopic()
        );

        System.out.printf("%-20s %s%n",
            "Tags:",
            "[" + msg.getTags() + "]"
        );

        System.out.printf("%-20s %s%n",
            "Keys:",
            "[" + msg.getKeys() + "]"
        );

        System.out.printf("%-20s %d%n",
            "Queue ID:",
            msg.getQueueId()
        );

        System.out.printf("%-20s %d%n",
            "Queue Offset:",
            msg.getQueueOffset()
        );

        System.out.printf("%-20s %d%n",
            "CommitLog Offset:",
            msg.getCommitLogOffset()
        );

        System.out.printf("%-20s %d%n",
            "Reconsume Times:",
            msg.getReconsumeTimes()
        );

        System.out.printf("%-20s %s%n",
            "Born Timestamp:",
            UtilAll.timeMillisToHumanString2(msg.getBornTimestamp())
        );

        System.out.printf("%-20s %s%n",
            "Store Timestamp:",
            UtilAll.timeMillisToHumanString2(msg.getStoreTimestamp())
        );

        System.out.printf("%-20s %s%n",
            "Born Host:",
            RemotingHelper.parseSocketAddressAddr(msg.getBornHost())
        );

        System.out.printf("%-20s %s%n",
            "Store Host:",
            RemotingHelper.parseSocketAddressAddr(msg.getStoreHost())
        );

        System.out.printf("%-20s %d%n",
            "System Flag:",
            msg.getSysFlag()
        );

        System.out.printf("%-20s %s%n",
            "Properties:",
            msg.getProperties() != null ? msg.getProperties().toString() : ""
        );

        System.out.printf("%-20s %s%n",
            "Message Body Path:",
            bodyTmpFilePath
        );

        if (null != msgBodyCharset) {
            System.out.printf("%-20s %s%n", "Message Body:", new String(msg.getBody(), msgBodyCharset));
        }

        try {
            List<MessageTrack> mtdList = admin.messageTrackDetail(msg);
            if (mtdList.isEmpty()) {
                System.out.printf("%n%nWARN: No Consumer");
            } else {
                System.out.printf("%n%n");
                for (MessageTrack mt : mtdList) {
                    System.out.printf("%s%n", mt);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String createBodyFile(MessageExt msg) throws IOException {
        DataOutputStream dos = null;
        try {
            String bodyTmpFilePath = "/tmp/rocketmq/msgbodys";
            File file = new File(bodyTmpFilePath);
            if (!file.exists()) {
                file.mkdirs();
            }
            bodyTmpFilePath = bodyTmpFilePath + "/" + msg.getMsgId();
            dos = new DataOutputStream(new FileOutputStream(bodyTmpFilePath));
            dos.write(msg.getBody());
            return bodyTmpFilePath;
        } finally {
            if (dos != null)
                dos.close();
        }
    }

    @Override
    public String commandName() {
        return "queryMsgById";
    }

    @Override
    public String commandDesc() {
        return "Query Message by Id.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "msgId", true, "Message Id");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "clientId", true, "The consumer's client id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "sendMessage", true, "resend message");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("u", "unitName", true, "unit name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "bodyFormat", true, "print message body by the specified format");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "cluster", true, "Cluster name or lmq parent topic, lmq is used to find the route.");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("ReSendMsgById");
        defaultMQProducer.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String topic = commandLine.getOptionValue('t').trim();

            if (commandLine.hasOption('s')) {
                if (commandLine.hasOption('u')) {
                    String unitName = commandLine.getOptionValue('u').trim();
                    defaultMQProducer.setUnitName(unitName);
                }
                defaultMQProducer.start();
            }

            final String msgIds = commandLine.getOptionValue('i').trim();
            final String[] msgIdArr = StringUtils.split(msgIds, ",");
            String clusterName = commandLine.hasOption('c') ? commandLine.getOptionValue('c').trim() : null;

            if (commandLine.hasOption('g') && commandLine.hasOption('d')) {
                final String consumerGroup = commandLine.getOptionValue('g').trim();
                final String clientId = commandLine.getOptionValue('d').trim();
                for (String msgId : msgIdArr) {
                    if (StringUtils.isNotBlank(msgId)) {
                        pushMsg(defaultMQAdminExt, clusterName, consumerGroup, clientId, topic, msgId.trim());
                    }
                }
            } else if (commandLine.hasOption('s')) {
                boolean resend = Boolean.parseBoolean(commandLine.getOptionValue('s', "false").trim());
                if (resend) {
                    for (String msgId : msgIdArr) {
                        if (StringUtils.isNotBlank(msgId)) {
                            sendMsg(defaultMQAdminExt, clusterName, defaultMQProducer, topic, msgId.trim());
                        }
                    }
                }
            } else {
                Charset msgBodyCharset = null;
                if (commandLine.hasOption('f')) {
                    msgBodyCharset = Charset.forName(commandLine.getOptionValue('f').trim());
                }
                for (String msgId : msgIdArr) {
                    if (StringUtils.isNotBlank(msgId)) {
                        queryById(defaultMQAdminExt, clusterName, topic, msgId.trim(), msgBodyCharset);
                    }
                }

            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQProducer.shutdown();
            defaultMQAdminExt.shutdown();
        }
    }

    private void pushMsg(final DefaultMQAdminExt defaultMQAdminExt, final String clusterName,
        final String consumerGroup, final String clientId,
        final String topic, final String msgId) {
        try {
            ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(consumerGroup, clientId, false, false);
            if (consumerRunningInfo != null && ConsumerRunningInfo.isPushType(consumerRunningInfo)) {
                ConsumeMessageDirectlyResult result =
                    defaultMQAdminExt.consumeMessageDirectly(clusterName, consumerGroup, clientId, topic, msgId);
                System.out.printf("%s", result);
            } else {
                System.out.printf("this %s client is not push consumer ,not support direct push \n", clientId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendMsg(final DefaultMQAdminExt defaultMQAdminExt, final String clusterName,
        final DefaultMQProducer defaultMQProducer,
        final String topic, final String msgId) {
        try {
            MessageExt msg = defaultMQAdminExt.queryMessage(clusterName, topic, msgId);
            if (msg != null) {
                // resend msg by id
                System.out.printf("prepare resend msg. originalMsgId=%s", msgId);
                SendResult result = defaultMQProducer.send(msg);
                System.out.printf("%s", result);
            } else {
                System.out.printf("no message. msgId=%s", msgId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
