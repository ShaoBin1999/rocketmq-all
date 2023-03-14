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

package com.bsren.rocketmq.broker.filter;

import com.bsren.rocketmq.filter.expression.EvaluationContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Evaluation context from message.
 */
public class MessageEvaluationContext implements EvaluationContext {

    private final Map<String, String> properties;

    public MessageEvaluationContext(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Object get(final String name) {
        if (this.properties == null) {
            return null;
        }
        return this.properties.get(name);
    }

    @Override
    public Map<String, Object> keyValues() {
        if (properties == null) {
            return null;
        }

        Map<String, Object> copy = new HashMap<String, Object>(properties.size(), 1);

        for (String key : properties.keySet()) {
            copy.put(key, properties.get(key));
        }

        return copy;
    }
}
