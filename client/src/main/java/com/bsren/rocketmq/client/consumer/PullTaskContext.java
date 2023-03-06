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
package com.bsren.rocketmq.client.consumer;

public class PullTaskContext {

    private int pullNextDelayTimeMillis = 200;

    private MQPullConsumer pullConsumer;

    public int getPullNextDelayTimeMillis() {
        return pullNextDelayTimeMillis;
    }

    public void setPullNextDelayTimeMillis(int pullNextDelayTimeMillis) {
        this.pullNextDelayTimeMillis = pullNextDelayTimeMillis;
    }

    public MQPullConsumer getPullConsumer() {
        return pullConsumer;
    }

    public void setPullConsumer(MQPullConsumer pullConsumer) {
        this.pullConsumer = pullConsumer;
    }
}
