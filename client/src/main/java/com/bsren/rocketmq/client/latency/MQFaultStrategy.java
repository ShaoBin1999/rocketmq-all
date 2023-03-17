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

package com.bsren.rocketmq.client.latency;

import com.bsren.rocketmq.client.impl.producer.TopicPublishInfo;
import com.bsren.rocketmq.client.log.ClientLogger;
import com.bsren.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;

/**
 * 在RocketMq集群中，queue分布在各个不同的broker服务器中时，
 * 当尝试向其中一个queue发送消息时，如果出现耗时过长或者发送失败的情况，
 * RocketMQ则会尝试重试发送。不妨细想一下，
 * 同样的消息第一次发送失败或耗时过长，可能是网络波动或者相关broker停止导致，
 * 如果短时间再次重试极有可能还是同样的情况。
 * RocketMQ为我们提供了延迟故障自动切换queue的功能，
 * 并且会根据故障次数和失败等级来预判故障时间并自动恢复，
 * 该功能是选配，默认关闭，可以通过如下配置开启。
 * 该功能只有在没有指定queue时生效
 */
public class MQFaultStrategy {
    private final static Logger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 如果开启了延迟
     * 则根据Index获取对应的MessageQueue，如果lastBrokerName为空或者没有匹配的上
     * 否则随机选择一个，
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            try {
                // 这个index，每次选择一个队列，tpInfo中的ThreadLocalIndex都会加1，意味着，每个线程去获取队列，其实都是负载均衡的
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                // 与队列的长度取模，根据最后的pos取一个队列
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    //判断取到的队列的broker是否故障中，如果不是故障中，就返回即可
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                //如果所有的队列都是故障中的话，那么就从故障列表取出一个Broker即可(待会再看实现)
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                //获取这个broker的可写队列数，如果该Broker没有可写的队列，则返回-1
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    //再次选择一次队列，通过与队列的长度取模确定队列的位置
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    //没有可写的队列，直接从故障列表移除
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            //如果故障列表中也没有可写的队列，则直接从tpInfo中获取一个
            return tpInfo.selectOneMessageQueue();
        }
        //没有开启延迟故障，直接从TopicPublishInfo通过取模的方式获取队列即可，如果LastBrokerName不为空，则需要过滤掉brokerName=lastBrokerName的队列
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * @param currentLatency 截至当前延迟
     * @param isolation 是否隔离，该处会直接将隔离状态的延迟时长标记为30秒，也就是说为true时就认为执行时长为30秒
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 以上latencyMax表示执行时长，notAvailableDuration表示broker不可用时长，
     * 他们索引位一一对应，该方法是反向遍历索引位置，假设我当前消息推送时长为600ms，
     * 对应latencyMax下标是2，那么在notAvailableDuration下标也是2，这个broker的不可用时长则是30000ms。
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
