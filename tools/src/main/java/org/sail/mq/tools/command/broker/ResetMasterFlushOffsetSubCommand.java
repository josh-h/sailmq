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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.sail.mq.remoting.RPCHook;
import org.sail.mq.tools.admin.DefaultMQAdminExt;
import org.sail.mq.tools.command.SubCommand;
import org.sail.mq.tools.command.SubCommandException;

public class ResetMasterFlushOffsetSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "resetMasterFlushOffset";
    }

    @Override
    public String commandDesc() {
        return "Reset master flush offset in slave.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "which broker to reset");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("o", "offset", true, "the offset to reset at");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            String brokerAddr = commandLine.getOptionValue('b').trim();
            long masterFlushOffset = Long.parseLong(commandLine.getOptionValue('o').trim());

            defaultMQAdminExt.resetMasterFlushOffset(brokerAddr, masterFlushOffset);
            System.out.printf("reset master flush offset to %d success%n", masterFlushOffset);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
