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

/**
 * $Id: QueueData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package com.bsren.rocketmq.common.protocol.route;

/**
 * 自动创建Topic时，默认采用的是Broker模式，会为每个Broker默认创建4个Queue，这个可以再配置文件中配置。
 *
 * 读写队列实际上在物理概念上是同一个队列，
 * 所以是不存在读写队列数据同步的问题，
 * 读写队列是在逻辑上区分的概念，一般情况下读写队列数量是相同的
 * 但是我们可以设置不同，比如创建Topic时，设置我们写队列创建8个，读队列4个，
 * 那么就会有0-7个Queue可写，0-3个队列为可读，
 * 那么Producer会将消息写入到0-7这八个队列中，
 * 但是Consumer只会消费0-3这4个Queue中的消息，4-7中的Queue中的消息是不会被消费的，
 * 假设我们消费者集群中有两个Consumer，Consumer1消费0-3，Consumer2消费4-7，
 * 但是实际上Consumer2是没法消费消费消息的，
 * 那么这样一个设置，很明显是容易出现问题的，但是RocketMQ为什么这么设置呢，原因如下！
 *
 * 通常情况下，我们手动创建Topic的时候读写Queue设置都是一样的，
 * 但是如果我们刚开始高估了我们的系统消息吞吐量，
 * 一开始就设置为16个Queue，但是后期实际上消息只有一半，那么为了提高系统资源利用，
 * 我们可以对Topic下的Queue进行动态缩容，那么这时我们就可以调整读写队列数量，
 * 我们可以先将写队列缩容为8，那么此时生产者发送的消息就自会到达0-7队列中，
 * 二消费者还是消费0-15中的Queue，等待消费者将8-15Queue消费完，
 * 那么此时，就可以将读队列设置为8个，那么此时就可以保证消息不丢失的情况下完成动态缩容
 */
public class QueueData implements Comparable<QueueData> {
    private String brokerName;
    private int readQueueNums;
    private int writeQueueNums;
    private int perm;
    private int topicSynFlag;

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public int getTopicSynFlag() {
        return topicSynFlag;
    }

    public void setTopicSynFlag(int topicSynFlag) {
        this.topicSynFlag = topicSynFlag;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + perm;
        result = prime * result + readQueueNums;
        result = prime * result + writeQueueNums;
        result = prime * result + topicSynFlag;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueueData other = (QueueData) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (perm != other.perm)
            return false;
        if (readQueueNums != other.readQueueNums)
            return false;
        if (writeQueueNums != other.writeQueueNums)
            return false;
        if (topicSynFlag != other.topicSynFlag)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "QueueData [brokerName=" + brokerName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + perm + ", topicSynFlag=" + topicSynFlag
            + "]";
    }

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
