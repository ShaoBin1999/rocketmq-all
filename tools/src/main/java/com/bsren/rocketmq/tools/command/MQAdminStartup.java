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
package com.bsren.rocketmq.tools.command;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.bsren.rocketmq.common.MQVersion;
import com.bsren.rocketmq.common.MixAll;
import com.bsren.rocketmq.common.ServerUtil;
import com.bsren.rocketmq.remoting.RPCHook;
import com.bsren.rocketmq.remoting.protocol.RemotingCommand;
import com.bsren.rocketmq.tools.command.broker.*;
import com.bsren.rocketmq.tools.command.cluster.CLusterSendMsgRTCommand;
import com.bsren.rocketmq.tools.command.cluster.ClusterListSubCommand;
import com.bsren.rocketmq.tools.command.connection.ConsumerConnectionSubCommand;
import com.bsren.rocketmq.tools.command.connection.ProducerConnectionSubCommand;
import com.bsren.rocketmq.tools.command.consumer.*;
import com.bsren.rocketmq.tools.command.message.*;
import com.bsren.rocketmq.tools.command.namesrv.*;
import com.bsren.rocketmq.tools.command.offset.CloneGroupOffsetCommand;
import com.bsren.rocketmq.tools.command.offset.ResetOffsetByTimeCommand;
import com.bsren.rocketmq.tools.command.queue.QueryConsumeQueueCommand;
import com.bsren.rocketmq.tools.command.stats.StatsAllSubCommand;
import com.bsren.rocketmq.tools.command.topic.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MQAdminStartup {
    protected static List<SubCommand> subCommandList = new ArrayList<SubCommand>();

    public static void main(String[] args) {
        main0(args, null);
    }

    public static void main0(String[] args, RPCHook rpcHook) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        //PackageConflictDetect.detectFastjson();

        initCommand();

        try {
            initLogback();
            switch (args.length) {
                case 0:
                    printHelp();
                    break;
                case 2:
                    if (args[0].equals("help")) {
                        SubCommand cmd = findSubCommand(args[1]);
                        if (cmd != null) {
                            Options options = ServerUtil.buildCommandlineOptions(new Options());
                            options = cmd.buildCommandlineOptions(options);
                            if (options != null) {
                                ServerUtil.printCommandLineHelp("mqadmin " + cmd.commandName(), options);
                            }
                        } else {
                            System.out.printf("The sub command %s not exist.%n", args[1]);
                        }
                        break;
                    }
                case 1:
                default:
                    SubCommand cmd = findSubCommand(args[0]);
                    if (cmd != null) {
                        String[] subargs = parseSubArgs(args);

                        Options options = ServerUtil.buildCommandlineOptions(new Options());
                        final CommandLine commandLine =
                            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options),
                                new PosixParser());
                        if (null == commandLine) {
                            return;
                        }

                        if (commandLine.hasOption('n')) {
                            String namesrvAddr = commandLine.getOptionValue('n');
                            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
                        }

                        cmd.execute(commandLine, options, rpcHook);
                    } else {
                        System.out.printf("The sub command %s not exist.%n", args[0]);
                    }
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void initCommand() {
        initCommand(new UpdateTopicSubCommand());
        initCommand(new DeleteTopicSubCommand());
        initCommand(new UpdateSubGroupSubCommand());
        initCommand(new DeleteSubscriptionGroupCommand());
        initCommand(new UpdateBrokerConfigSubCommand());
        initCommand(new UpdateTopicPermSubCommand());

        initCommand(new TopicRouteSubCommand());
        initCommand(new TopicStatusSubCommand());
        initCommand(new TopicClusterSubCommand());

        initCommand(new BrokerStatusSubCommand());
        initCommand(new QueryMsgByIdSubCommand());
        initCommand(new QueryMsgByKeySubCommand());
        initCommand(new QueryMsgByUniqueKeySubCommand());
        initCommand(new QueryMsgByOffsetSubCommand());
        initCommand(new QueryMsgByUniqueKeySubCommand());
        initCommand(new PrintMessageSubCommand());
        initCommand(new PrintMessageByQueueCommand());
        initCommand(new SendMsgStatusCommand());
        initCommand(new BrokerConsumeStatsSubCommad());

        initCommand(new ProducerConnectionSubCommand());
        initCommand(new ConsumerConnectionSubCommand());
        initCommand(new ConsumerProgressSubCommand());
        initCommand(new ConsumerStatusSubCommand());
        initCommand(new CloneGroupOffsetCommand());

        initCommand(new ClusterListSubCommand());
        initCommand(new TopicListSubCommand());

        initCommand(new UpdateKvConfigCommand());
        initCommand(new DeleteKvConfigCommand());

        initCommand(new WipeWritePermSubCommand());
        initCommand(new ResetOffsetByTimeCommand());

        initCommand(new UpdateOrderConfCommand());
        initCommand(new CleanExpiredCQSubCommand());
        initCommand(new CleanUnusedTopicCommand());

        initCommand(new StartMonitoringSubCommand());
        initCommand(new StatsAllSubCommand());

        initCommand(new AllocateMQSubCommand());

        initCommand(new CheckMsgSendRTCommand());
        initCommand(new CLusterSendMsgRTCommand());

        initCommand(new GetNamesrvConfigCommand());
        initCommand(new UpdateNamesrvConfigCommand());
        initCommand(new GetBrokerConfigCommand());

        initCommand(new QueryConsumeQueueCommand());
    }

    private static void initLogback() throws JoranException {
        String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(rocketmqHome + "/conf/logback_tools.xml");
    }

    private static void printHelp() {
        System.out.printf("The most commonly used mqadmin commands are:%n");

        for (SubCommand cmd : subCommandList) {
            System.out.printf("   %-20s %s%n", cmd.commandName(), cmd.commandDesc());
        }

        System.out.printf("%nSee 'mqadmin help <command>' for more information on a specific command.%n");
    }

    private static SubCommand findSubCommand(final String name) {
        for (SubCommand cmd : subCommandList) {
            if (cmd.commandName().toUpperCase().equals(name.toUpperCase())) {
                return cmd;
            }
        }

        return null;
    }

    private static String[] parseSubArgs(String[] args) {
        if (args.length > 1) {
            String[] result = new String[args.length - 1];
            for (int i = 0; i < args.length - 1; i++) {
                result[i] = args[i + 1];
            }
            return result;
        }
        return null;
    }

    public static void initCommand(SubCommand command) {
        subCommandList.add(command);
    }
}
