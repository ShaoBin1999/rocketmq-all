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

package com.bsren.rocketmq.filtersrv.filter;

import com.bsren.rocketmq.common.MixAll;
import com.bsren.rocketmq.common.ThreadFactoryImpl;
import com.bsren.rocketmq.common.UtilAll;
import com.bsren.rocketmq.common.constant.LoggerName;
import com.bsren.rocketmq.common.filter.MessageFilter;
import com.bsren.rocketmq.filtersrv.FiltersrvController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.*;

/**
 * 注册filterClass
 * 定时从remotingHost获取class检查crc，如果一致则不变，以host端为准
 */
public class FilterClassManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTERSRV_LOGGER_NAME);

    private final Object compileLock = new Object();
    private final FiltersrvController filtersrvController;

    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FSGetClassScheduledThread"));
    private ConcurrentMap<String/* topic@consumerGroup */, FilterClassInfo> filterClassTable =
            new ConcurrentHashMap<>(128);
    private FilterClassFetchMethod filterClassFetchMethod;

    public FilterClassManager(FiltersrvController filtersrvController) {
        this.filtersrvController = filtersrvController;
        this.filterClassFetchMethod =
            new HttpFilterClassFetchMethod(this.filtersrvController.getFiltersrvConfig().getFilterClassRepertoryUrl());
    }

    private static String buildKey(final String consumerGroup, final String topic) {
        return topic + "@" + consumerGroup;
    }

    public void start() {
        if (!this.filtersrvController.getFiltersrvConfig().isClientUploadFilterClassEnable()) {
            this.scheduledExecutorService.scheduleAtFixedRate(this::fetchClassFromRemoteHost, 1, 1, TimeUnit.MINUTES);
        }
    }

    /**
     * 从host获取class，如果有新的则更新
     */
    private void fetchClassFromRemoteHost() {
        Iterator<Entry<String, FilterClassInfo>> it = this.filterClassTable.entrySet().iterator();
        while (it.hasNext()) {
            try {
                Entry<String, FilterClassInfo> next = it.next();
                FilterClassInfo filterClassInfo = next.getValue();
                String[] topicAndGroup = next.getKey().split("@");
                String responseStr = this.filterClassFetchMethod.fetch(topicAndGroup[0], topicAndGroup[1], filterClassInfo.getClassName());
                byte[] filterSourceBinary = responseStr.getBytes(StandardCharsets.UTF_8);
                int classCRC = UtilAll.crc32(responseStr.getBytes(StandardCharsets.UTF_8));
                if (classCRC != filterClassInfo.getClassCRC()) {
                    String javaSource = new String(filterSourceBinary, MixAll.DEFAULT_CHARSET);
                    Class<?> newClass =
                        DynaCode.compileAndLoadClass(filterClassInfo.getClassName(), javaSource);
                    Object newInstance = newClass.newInstance();
                    filterClassInfo.setMessageFilter((MessageFilter) newInstance);
                    filterClassInfo.setClassCRC(classCRC);

                    log.info("fetch Remote class File OK, {} {}", next.getKey(), filterClassInfo.getClassName());
                }
            } catch (Exception e) {
                log.error("fetchClassFromRemoteHost Exception", e);
            }
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    public boolean registerFilterClass(
            final String consumerGroup,
            final String topic,
            final String className,
            final int classCRC,
            final byte[] filterSourceBinary) {
        final String key = buildKey(consumerGroup, topic);

        boolean registerNew = false;
        FilterClassInfo filterClassInfoPrev = this.filterClassTable.get(key);
        if (null == filterClassInfoPrev) {
            registerNew = true;
        } else {
            if (this.filtersrvController.getFiltersrvConfig().isClientUploadFilterClassEnable()) {
                if (filterClassInfoPrev.getClassCRC() != classCRC && classCRC != 0) {
                    registerNew = true;
                }
            }
        }

        if (registerNew) {
            synchronized (this.compileLock) {
                filterClassInfoPrev = this.filterClassTable.get(key);
                if (null != filterClassInfoPrev && filterClassInfoPrev.getClassCRC() == classCRC) {
                    return true;
                }

                try {
                    FilterClassInfo filterClassInfoNew = new FilterClassInfo();
                    filterClassInfoNew.setClassName(className);
                    filterClassInfoNew.setClassCRC(0);
                    filterClassInfoNew.setMessageFilter(null);
                    if (this.filtersrvController.getFiltersrvConfig().isClientUploadFilterClassEnable()) {
                        String javaSource = new String(filterSourceBinary, MixAll.DEFAULT_CHARSET);
                        Class<?> newClass = DynaCode.compileAndLoadClass(className, javaSource);
                        Object newInstance = newClass.newInstance();
                        filterClassInfoNew.setMessageFilter((MessageFilter) newInstance);
                        filterClassInfoNew.setClassCRC(classCRC);
                    }
                    this.filterClassTable.put(key, filterClassInfoNew);
                } catch (Throwable e) {
                    String info = String.format(
                                "FilterServer, registerFilterClass Exception, consumerGroup: %s topic: %s className: %s",
                                consumerGroup, topic, className);
                    log.error(info, e);
                    return false;
                }
            }
        }

        return true;
    }

    public FilterClassInfo findFilterClass(final String consumerGroup, final String topic) {
        return this.filterClassTable.get(buildKey(consumerGroup, topic));
    }

    public FilterClassFetchMethod getFilterClassFetchMethod() {
        return filterClassFetchMethod;
    }

    public void setFilterClassFetchMethod(FilterClassFetchMethod filterClassFetchMethod) {
        this.filterClassFetchMethod = filterClassFetchMethod;
    }
}
