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
package com.bsren.rocketmq.tools.command.topic;

import com.bsren.rocketmq.client.exception.MQClientException;
import com.bsren.rocketmq.common.MixAll;
import com.bsren.rocketmq.common.UtilAll;
import com.bsren.rocketmq.common.protocol.body.ClusterInfo;
import com.bsren.rocketmq.common.protocol.body.GroupList;
import com.bsren.rocketmq.common.protocol.body.TopicList;
import com.bsren.rocketmq.common.protocol.route.BrokerData;
import com.bsren.rocketmq.common.protocol.route.TopicRouteData;
import com.bsren.rocketmq.remoting.RPCHook;
import com.bsren.rocketmq.remoting.exception.RemotingException;
import com.bsren.rocketmq.tools.admin.DefaultMQAdminExt;
import com.bsren.rocketmq.tools.command.SubCommand;
import com.bsren.rocketmq.tools.command.SubCommandException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

public class TopicListSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topicList";
    }

    @Override
    public String commandDesc() {
        return "Fetch all topic list from name server";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterModel", false, "clusterModel");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            if (commandLine.hasOption('c')) {
                ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();

                System.out.printf("%-20s  %-48s  %-48s%n",
                    "#Cluster Name",
                    "#Topic",
                    "#Consumer Group"
                );

                TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
                for (String topic : topicList.getTopicList()) {
                    if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                        || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
                        continue;
                    }

                    String clusterName = "";
                    GroupList groupList = new GroupList();

                    try {
                        clusterName =
                            this.findTopicBelongToWhichCluster(topic, clusterInfo, defaultMQAdminExt);
                        groupList = defaultMQAdminExt.queryTopicConsumeByWho(topic);
                    } catch (Exception e) {
                    }

                    if (null == groupList || groupList.getGroupList().isEmpty()) {
                        groupList = new GroupList();
                        groupList.getGroupList().add("");
                    }

                    for (String group : groupList.getGroupList()) {
                        System.out.printf("%-20s  %-48s  %-48s%n",
                            UtilAll.frontStringAtLeast(clusterName, 20),
                            UtilAll.frontStringAtLeast(topic, 48),
                            UtilAll.frontStringAtLeast(group, 48)
                        );
                    }
                }
            } else {
                TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
                for (String topic : topicList.getTopicList()) {
                    System.out.printf("%s%n", topic);
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private String findTopicBelongToWhichCluster(final String topic, final ClusterInfo clusterInfo,
        final DefaultMQAdminExt defaultMQAdminExt) throws RemotingException, MQClientException,
        InterruptedException {
        TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);

        BrokerData brokerData = topicRouteData.getBrokerDatas().get(0);

        String brokerName = brokerData.getBrokerName();

        Iterator<Entry<String, Set<String>>> it = clusterInfo.getClusterAddrTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<String>> next = it.next();
            if (next.getValue().contains(brokerName)) {
                return next.getKey();
            }
        }
        return null;
    }
}
