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

package com.bsren.rocketmq.tools.monitor;

import com.bsren.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.bsren.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.bsren.rocketmq.client.consumer.PullResult;
import com.bsren.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.bsren.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.bsren.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.bsren.rocketmq.client.exception.MQBrokerException;
import com.bsren.rocketmq.client.exception.MQClientException;
import com.bsren.rocketmq.client.log.ClientLogger;
import com.bsren.rocketmq.common.MQVersion;
import com.bsren.rocketmq.common.MixAll;
import com.bsren.rocketmq.common.ThreadFactoryImpl;
import com.bsren.rocketmq.common.admin.ConsumeStats;
import com.bsren.rocketmq.common.admin.OffsetWrapper;
import com.bsren.rocketmq.common.message.MessageExt;
import com.bsren.rocketmq.common.message.MessageQueue;
import com.bsren.rocketmq.common.protocol.body.Connection;
import com.bsren.rocketmq.common.protocol.body.ConsumerConnection;
import com.bsren.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.bsren.rocketmq.common.protocol.body.TopicList;
import com.bsren.rocketmq.common.protocol.topic.OffsetMovedEvent;
import com.bsren.rocketmq.remoting.RPCHook;
import com.bsren.rocketmq.remoting.exception.RemotingException;
import com.bsren.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MonitorService {
    private final Logger log = ClientLogger.getLog();
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("MonitorService"));

    private final MonitorConfig monitorConfig;

    private final MonitorListener monitorListener;

    private final DefaultMQAdminExt defaultMQAdminExt;
    private final DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(
        MixAll.TOOLS_CONSUMER_GROUP);
    private final DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(
        MixAll.MONITOR_CONSUMER_GROUP);

    public MonitorService(MonitorConfig monitorConfig, MonitorListener monitorListener, RPCHook rpcHook) {
        this.monitorConfig = monitorConfig;
        this.monitorListener = monitorListener;

        this.defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        this.defaultMQAdminExt.setInstanceName(instanceName());
        this.defaultMQAdminExt.setNamesrvAddr(monitorConfig.getNamesrvAddr());

        this.defaultMQPullConsumer.setInstanceName(instanceName());
        this.defaultMQPullConsumer.setNamesrvAddr(monitorConfig.getNamesrvAddr());

        this.defaultMQPushConsumer.setInstanceName(instanceName());
        this.defaultMQPushConsumer.setNamesrvAddr(monitorConfig.getNamesrvAddr());
        try {
            this.defaultMQPushConsumer.setConsumeThreadMin(1);
            this.defaultMQPushConsumer.setConsumeThreadMax(1);
            this.defaultMQPushConsumer.subscribe(MixAll.OFFSET_MOVED_EVENT, "*");
            this.defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    try {
                        OffsetMovedEvent ome =
                            OffsetMovedEvent.decode(msgs.get(0).getBody(), OffsetMovedEvent.class);

                        DeleteMsgsEvent deleteMsgsEvent = new DeleteMsgsEvent();
                        deleteMsgsEvent.setOffsetMovedEvent(ome);
                        deleteMsgsEvent.setEventTimestamp(msgs.get(0).getStoreTimestamp());

                        MonitorService.this.monitorListener.reportDeleteMsgsEvent(deleteMsgsEvent);
                    } catch (Exception e) {
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
        } catch (MQClientException e) {
        }
    }

    public static void main(String[] args) throws MQClientException {
        main0(args, null);
    }

    public static void main0(String[] args, RPCHook rpcHook) throws MQClientException {
        final MonitorService monitorService =
            new MonitorService(new MonitorConfig(), new DefaultMonitorListener(), rpcHook);
        monitorService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;

            @Override
            public void run() {
                synchronized (this) {
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        monitorService.shutdown();
                    }
                }
            }
        }, "ShutdownHook"));
    }

    private String instanceName() {
        String name =
            System.currentTimeMillis() + new Random().nextInt() + this.monitorConfig.getNamesrvAddr();

        return "MonitorService_" + name.hashCode();
    }

    public void start() throws MQClientException {
        this.defaultMQPullConsumer.start();
        this.defaultMQAdminExt.start();
        this.defaultMQPushConsumer.start();
        this.startScheduleTask();
    }

    public void shutdown() {
        this.defaultMQPullConsumer.shutdown();
        this.defaultMQAdminExt.shutdown();
        this.defaultMQPushConsumer.shutdown();
    }

    private void startScheduleTask() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    MonitorService.this.doMonitorWork();
                } catch (Exception e) {
                    log.error("doMonitorWork Exception", e);
                }
            }
        }, 1000 * 20, this.monitorConfig.getRoundInterval(), TimeUnit.MILLISECONDS);
    }

    public void doMonitorWork() throws RemotingException, MQClientException, InterruptedException {
        long beginTime = System.currentTimeMillis();
        this.monitorListener.beginRound();

        TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
        for (String topic : topicList.getTopicList()) {
            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                String consumerGroup = topic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());

                try {
                    this.reportUndoneMsgs(consumerGroup);
                } catch (Exception e) {
                    // log.error("reportUndoneMsgs Exception", e);
                }

                try {
                    this.reportConsumerRunningInfo(consumerGroup);
                } catch (Exception e) {
                    // log.error("reportConsumerRunningInfo Exception", e);
                }
            }
        }
        this.monitorListener.endRound();
        long spentTimeMills = System.currentTimeMillis() - beginTime;
        log.info("Execute one round monitor work, spent timemills: {}", spentTimeMills);
    }

    private void reportUndoneMsgs(final String consumerGroup) {
        ConsumeStats cs = null;
        try {
            cs = defaultMQAdminExt.examineConsumeStats(consumerGroup);
        } catch (Exception e) {
            return;
        }

        ConsumerConnection cc = null;
        try {
            cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
        } catch (Exception e) {
            return;
        }

        if (cs != null) {

            HashMap<String/* Topic */, ConsumeStats> csByTopic = new HashMap<String, ConsumeStats>();
            {
                Iterator<Entry<MessageQueue, OffsetWrapper>> it = cs.getOffsetTable().entrySet().iterator();
                while (it.hasNext()) {
                    Entry<MessageQueue, OffsetWrapper> next = it.next();
                    MessageQueue mq = next.getKey();
                    OffsetWrapper ow = next.getValue();
                    ConsumeStats csTmp = csByTopic.get(mq.getTopic());
                    if (null == csTmp) {
                        csTmp = new ConsumeStats();
                        csByTopic.put(mq.getTopic(), csTmp);
                    }

                    csTmp.getOffsetTable().put(mq, ow);
                }
            }

            {
                Iterator<Entry<String, ConsumeStats>> it = csByTopic.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String, ConsumeStats> next = it.next();
                    UndoneMsgs undoneMsgs = new UndoneMsgs();
                    undoneMsgs.setConsumerGroup(consumerGroup);
                    undoneMsgs.setTopic(next.getKey());
                    this.computeUndoneMsgs(undoneMsgs, next.getValue());
                    this.monitorListener.reportUndoneMsgs(undoneMsgs);
                    this.reportFailedMsgs(consumerGroup, next.getKey());
                }
            }
        }
    }

    public void reportConsumerRunningInfo(final String consumerGroup) throws InterruptedException,
            MQBrokerException, RemotingException, MQClientException {
        ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
        TreeMap<String, ConsumerRunningInfo> infoMap = new TreeMap<String, ConsumerRunningInfo>();
        for (Connection c : cc.getConnectionSet()) {
            String clientId = c.getClientId();

            if (c.getVersion() < MQVersion.Version.V3_1_8_SNAPSHOT.ordinal()) {
                continue;
            }

            try {
                ConsumerRunningInfo info =
                    defaultMQAdminExt.getConsumerRunningInfo(consumerGroup, clientId, false);
                infoMap.put(clientId, info);
            } catch (Exception e) {
            }
        }

        if (!infoMap.isEmpty()) {
            this.monitorListener.reportConsumerRunningInfo(infoMap);
        }
    }

    private void computeUndoneMsgs(final UndoneMsgs undoneMsgs, final ConsumeStats consumeStats) {
        long total = 0;
        long singleMax = 0;
        long delayMax = 0;
        for (Entry<MessageQueue, OffsetWrapper> next : consumeStats.getOffsetTable().entrySet()) {
            MessageQueue mq = next.getKey();
            OffsetWrapper ow = next.getValue();
            long diff = ow.getBrokerOffset() - ow.getConsumerOffset();

            if (diff > singleMax) {
                singleMax = diff;
            }

            if (diff > 0) {
                total += diff;
            }

            // Delay
            if (ow.getLastTimestamp() > 0) {
                try {
                    long maxOffset = this.defaultMQPullConsumer.maxOffset(mq);
                    if (maxOffset > 0) {
                        PullResult pull = this.defaultMQPullConsumer.pull(mq, "*", maxOffset - 1, 1);
                        switch (pull.getPullStatus()) {
                            case FOUND:
                                long delay =
                                        pull.getMsgFoundList().get(0).getStoreTimestamp() - ow.getLastTimestamp();
                                if (delay > delayMax) {
                                    delayMax = delay;
                                }
                                break;
                            case NO_MATCHED_MSG:
                            case NO_NEW_MSG:
                            case OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                }
            }
        }

        undoneMsgs.setUndoneMsgsTotal(total);
        undoneMsgs.setUndoneMsgsSingleMQ(singleMax);
        undoneMsgs.setUndoneMsgsDelayTimeMills(delayMax);
    }

    private void reportFailedMsgs(final String consumerGroup, final String topic) {

    }
}
