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

package com.bsren.rocketmq.common.protocol.body;

import com.bsren.rocketmq.common.message.MessageQueue;
import com.bsren.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

/**
 * consumerGroup
 * clientId
 * Set<MessageQueue>
 */
public class UnlockBatchRequestBody extends RemotingSerializable {
    private String consumerGroup;
    private String clientId;
    private Set<MessageQueue> mqSet;

    public UnlockBatchRequestBody(String consumerGroup, String clientId,MessageQueue mq) {
        this.consumerGroup = consumerGroup;
        this.clientId = clientId;
        this.mqSet = new HashSet<>();
        this.mqSet.add(mq);
    }

    public UnlockBatchRequestBody(String consumerGroup, String clientId, Set<MessageQueue> mqSet) {
        this.consumerGroup = consumerGroup;
        this.clientId = clientId;
        this.mqSet = mqSet;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Set<MessageQueue> getMqSet() {
        return mqSet;
    }

    public void setMqSet(Set<MessageQueue> mqSet) {
        this.mqSet = mqSet;
    }
}
