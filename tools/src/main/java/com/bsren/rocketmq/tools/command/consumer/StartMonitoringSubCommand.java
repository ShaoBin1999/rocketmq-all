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
package com.bsren.rocketmq.tools.command.consumer;

import com.bsren.rocketmq.client.log.ClientLogger;
import com.bsren.rocketmq.remoting.RPCHook;
import com.bsren.rocketmq.tools.command.SubCommand;
import com.bsren.rocketmq.tools.command.SubCommandException;
import com.bsren.rocketmq.tools.monitor.DefaultMonitorListener;
import com.bsren.rocketmq.tools.monitor.MonitorConfig;
import com.bsren.rocketmq.tools.monitor.MonitorService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;

public class StartMonitoringSubCommand implements SubCommand {
    private final Logger log = ClientLogger.getLog();

    @Override
    public String commandName() {
        return "startMonitoring";
    }

    @Override
    public String commandDesc() {
        return "Start Monitoring";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        try {
            MonitorService monitorService =
                new MonitorService(new MonitorConfig(), new DefaultMonitorListener(), rpcHook);

            monitorService.start();
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        }
    }
}
