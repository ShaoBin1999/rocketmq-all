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

package com.bsren.rocketmq.tools.command.broker;

import com.bsren.rocketmq.client.exception.MQBrokerException;
import com.bsren.rocketmq.remoting.RPCHook;
import com.bsren.rocketmq.remoting.exception.RemotingConnectException;
import com.bsren.rocketmq.remoting.exception.RemotingSendRequestException;
import com.bsren.rocketmq.remoting.exception.RemotingTimeoutException;
import com.bsren.rocketmq.tools.admin.DefaultMQAdminExt;
import com.bsren.rocketmq.tools.admin.MQAdminExt;
import com.bsren.rocketmq.tools.command.CommandUtil;
import com.bsren.rocketmq.tools.command.SubCommand;
import com.bsren.rocketmq.tools.command.SubCommandException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GetBrokerConfigCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getBrokerConfig";
    }

    @Override
    public String commandDesc() {
        return "Get broker config by cluster or special broker!";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("b", "brokerAddr", true, "update which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "update which cluster");
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

                    getAndPrint(
                        defaultMQAdminExt,
                        String.format("============Master: %s============\n", masterAddr),
                        masterAddr
                    );
                    for (String slaveAddr : masterAndSlaveMap.get(masterAddr)) {

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

        Properties properties = defaultMQAdminExt.getBrokerConfig(addr);
        if (properties == null) {
            System.out.printf("Broker[%s] has no config property!\n", addr);
            return;
        }

        for (Object key : properties.keySet()) {
            System.out.printf("%-50s=  %s\n", key, properties.get(key));
        }

        System.out.printf("%n");
    }
}
