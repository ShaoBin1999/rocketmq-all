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
package com.bsren.rocketmq.tools.command.connection;

import com.bsren.rocketmq.common.MQVersion;
import com.bsren.rocketmq.common.protocol.body.Connection;
import com.bsren.rocketmq.common.protocol.body.ConsumerConnection;
import com.bsren.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.bsren.rocketmq.remoting.RPCHook;
import com.bsren.rocketmq.tools.admin.DefaultMQAdminExt;
import com.bsren.rocketmq.tools.command.SubCommand;
import com.bsren.rocketmq.tools.command.SubCommandException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Iterator;
import java.util.Map.Entry;

public class ConsumerConnectionSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "consumerConnection";
    }

    @Override
    public String commandDesc() {
        return "Query consumer's socket connection, client version and subscription";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String group = commandLine.getOptionValue('g').trim();

            ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(group);

            int i = 1;
            for (Connection conn : cc.getConnectionSet()) {
                System.out.printf("%03d  %-32s %-22s %-8s %s%n",
                    i++,
                    conn.getClientId(),
                    conn.getClientAddr(),
                    conn.getLanguage(),
                    MQVersion.getVersionDesc(conn.getVersion())
                );
            }

            System.out.printf("%nBelow is subscription:");
            Iterator<Entry<String, SubscriptionData>> it = cc.getSubscriptionTable().entrySet().iterator();
            i = 1;
            while (it.hasNext()) {
                Entry<String, SubscriptionData> entry = it.next();
                SubscriptionData sd = entry.getValue();
                System.out.printf("%03d  Topic: %-40s SubExpression: %s%n",
                    i++,
                    sd.getTopic(),
                    sd.getSubString()
                );
            }

            System.out.printf("");
            System.out.printf("ConsumeType: %s%n", cc.getConsumeType());
            System.out.printf("MessageModel: %s%n", cc.getMessageModel());
            System.out.printf("ConsumeFromWhere: %s%n", cc.getConsumeFromWhere());
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
