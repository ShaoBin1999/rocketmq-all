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
package com.bsren.rocketmq.broker.processor;

import com.bsren.rocketmq.broker.BrokerController;
import com.bsren.rocketmq.broker.client.ClientChannelInfo;
import com.bsren.rocketmq.common.MixAll;
import com.bsren.rocketmq.common.RemotingHelper;
import com.bsren.rocketmq.common.constant.LoggerName;
import com.bsren.rocketmq.common.constant.PermName;
import com.bsren.rocketmq.common.filter.ExpressionType;
import com.bsren.rocketmq.common.protocol.RequestCode;
import com.bsren.rocketmq.common.protocol.ResponseCode;
import com.bsren.rocketmq.common.protocol.body.CheckClientRequestBody;
import com.bsren.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import com.bsren.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import com.bsren.rocketmq.common.protocol.heartbeat.ConsumerData;
import com.bsren.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.bsren.rocketmq.common.protocol.heartbeat.ProducerData;
import com.bsren.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.bsren.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.bsren.rocketmq.common.sysflag.TopicSysFlag;
import com.bsren.rocketmq.remoting.exception.RemotingCommandException;
import com.bsren.rocketmq.remoting.netty.NettyRequestProcessor;
import com.bsren.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import com.bsren.rocketmq.filter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientManageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request);
            case RequestCode.UNREGISTER_CLIENT:
                return this.unregisterClient(ctx, request);
            case RequestCode.CHECK_CLIENT_CONFIG:
                return this.checkClientConfig(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 生产端会把订阅的主题发送给broker
     */
    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            ctx.channel(),
            heartbeatData.getClientID(),
            request.getLanguage(),
            request.getVersion()
        );

        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(data.getGroupName());
            boolean isNotifyConsumerIdsChangedEnable = true;
            if (null != subscriptionGroupConfig) {
                isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                int topicSysFlag = 0;
                if (data.isUnitMode()) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                String newTopic = MixAll.getRetryTopic(data.getGroupName());
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                        newTopic,
                        subscriptionGroupConfig.getRetryQueueNums(),
                        PermName.PERM_WRITE | PermName.PERM_READ,
                        topicSysFlag);
            }

            boolean changed = this.brokerController.getConsumerManager().registerConsumer(
                data.getGroupName(),
                clientChannelInfo,
                data.getConsumeType(),
                data.getMessageModel(),
                data.getConsumeFromWhere(),
                data.getSubscriptionDataSet(),
                isNotifyConsumerIdsChangedEnable
            );

            if (changed) {
                log.info("registerConsumer info changed {} {}",
                        data,
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                );
            }
        }

        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(),
                clientChannelInfo);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        final UnregisterClientRequestHeader requestHeader =
            (UnregisterClientRequestHeader) request
                .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            ctx.channel(),
            requestHeader.getClientID(),
            request.getLanguage(),
            request.getVersion());
        {
            final String group = requestHeader.getProducerGroup();
            if (group != null) {
                this.brokerController.getProducerManager().unregisterProducer(group, clientChannelInfo);
            }
        }

        {
            final String group = requestHeader.getConsumerGroup();
            if (group != null) {
                SubscriptionGroupConfig subscriptionGroupConfig =
                    this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
                boolean isNotifyConsumerIdsChangedEnable = true;
                if (null != subscriptionGroupConfig) {
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                }
                this.brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo, isNotifyConsumerIdsChangedEnable);
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 检查clientConfig里 sql表达式是否合适
     */
    public RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        CheckClientRequestBody requestBody = CheckClientRequestBody.decode(request.getBody(),
            CheckClientRequestBody.class);

        if (requestBody != null && requestBody.getSubscriptionData() != null) {
            SubscriptionData subscriptionData = requestBody.getSubscriptionData();

            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {  //如果是tag类型，则直接返回
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }

            //如果用的是sql92但是又不开启propertyFilter则直接报错
            if (!this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
                return response;
            }

            try {
                FilterFactory.INSTANCE.get(subscriptionData.getExpressionType()).compile(subscriptionData.getSubString());
            } catch (Exception e) {
                log.warn("Client {}@{} filter message, but failed to compile expression! sub={}, error={}",
                    requestBody.getClientId(), requestBody.getGroup(), requestBody.getSubscriptionData(), e.getMessage());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark(e.getMessage());
                return response;
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
