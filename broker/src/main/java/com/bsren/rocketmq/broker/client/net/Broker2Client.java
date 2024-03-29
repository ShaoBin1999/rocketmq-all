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
package com.bsren.rocketmq.broker.client.net;

import com.bsren.rocketmq.broker.BrokerController;
import com.bsren.rocketmq.broker.client.ClientChannelInfo;
import com.bsren.rocketmq.broker.client.ConsumerGroupInfo;
import com.bsren.rocketmq.broker.pagecache.OneMessageTransfer;
import com.bsren.rocketmq.common.MQVersion;
import com.bsren.rocketmq.common.RemotingHelper;
import com.bsren.rocketmq.common.TopicConfig;
import com.bsren.rocketmq.common.UtilAll;
import com.bsren.rocketmq.common.constant.LoggerName;
import com.bsren.rocketmq.common.message.MessageQueue;
import com.bsren.rocketmq.common.message.MessageQueueForC;
import com.bsren.rocketmq.common.protocol.RequestCode;
import com.bsren.rocketmq.common.protocol.body.GetConsumerStatusBody;
import com.bsren.rocketmq.common.protocol.body.ResetOffsetBody;
import com.bsren.rocketmq.common.protocol.body.ResetOffsetBodyForC;
import com.bsren.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.bsren.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import com.bsren.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import com.bsren.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import com.bsren.rocketmq.remoting.exception.RemotingSendRequestException;
import com.bsren.rocketmq.remoting.exception.RemotingTimeoutException;
import com.bsren.rocketmq.remoting.protocol.RemotingCommand;
import com.bsren.rocketmq.remoting.protocol.ResponseCode;
import com.bsren.rocketmq.store.SelectMappedBufferResult;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * broker呼叫client
 * 通知consumer的修改
 * 通知offset的修改
 */
public class Broker2Client {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    public Broker2Client(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void checkProducerTransactionState(
        final Channel channel,
        final CheckTransactionStateRequestHeader requestHeader,
        final SelectMappedBufferResult selectMappedBufferResult) {
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
        request.markOnewayRPC();

        try {
            FileRegion fileRegion =
                new OneMessageTransfer(request.encodeHeader(selectMappedBufferResult.getSize()),
                    selectMappedBufferResult);
            channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    selectMappedBufferResult.release();
                    if (!future.isSuccess()) {
                        log.error("invokeProducer failed,", future.cause());
                    }
                }
            });
        } catch (Throwable e) {
            log.error("invokeProducer exception", e);
            selectMappedBufferResult.release();
        }
    }

    public RemotingCommand callClient(final Channel channel, final RemotingCommand request)
            throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        return this.brokerController.getRemotingServer().invokeSync(channel, request, 10000);
    }

    /**
     * RequestCode.NOTIFY_CONSUMER_IDS_CHANGED
     */
    public void notifyConsumerIdsChanged(final Channel channel, final String consumerGroup) {
        if (null == consumerGroup) {
            log.error("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }
        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);
        try {
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e.getMessage());
        }
    }

    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce) {
        return resetOffset(topic, group, timeStamp, isForce, false);
    }

    /**
     * 自己遍历文件，找到timeStamp对应的offset
     * 对该group下的所有channel发送RequestCode.RESET_CONSUMER_CLIENT_OFFSET
     */
    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce, boolean isC) {

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            log.error("[reset-offset] reset offset failed, no topic in this broker. topic={}", topic);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("[reset-offset] reset offset failed, no topic in this broker. topic=" + topic);
            return response;
        }
        Map<MessageQueue, Long> offsetTable = new HashMap<>();

        //todo 这里需要用这个遍历条件吗
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, i);
            if (-1 == consumerOffset) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("THe consumer group <%s> not exist", group));
                return response;
            }

            long timeStampOffset; //timeStamp对应的记录数
            if (timeStamp == -1) {
                timeStampOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            } else {
                timeStampOffset = this.brokerController.getMessageStore().getOffsetInQueueByTime(topic, i, timeStamp);
            }

            if (timeStampOffset < 0) {
                log.warn("reset offset is invalid. topic={}, queueId={}, timeStampOffset={}", topic, i, timeStampOffset);
                timeStampOffset = 0;
            }
            MessageQueue mq = new MessageQueue();
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setTopic(topic);
            mq.setQueueId(i);
            if (isForce || timeStampOffset < consumerOffset) {
                offsetTable.put(mq, timeStampOffset);
            } else {
                offsetTable.put(mq, consumerOffset);
            }
        }

        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timeStamp);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, requestHeader);
        if (isC) {
            // c++ language
            ResetOffsetBodyForC body = new ResetOffsetBodyForC();
            List<MessageQueueForC> offsetList = convertOffsetTable2OffsetList(offsetTable);
            body.setOffsetTable(offsetList);
            request.setBody(body.encode());
        } else {
            // other language
            ResetOffsetBody body = new ResetOffsetBody();
            body.setOffsetTable(offsetTable);  //将offsetTable传过去
            request.setBody(body.encode());
        }

        ConsumerGroupInfo consumerGroupInfo =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(group);

        if (consumerGroupInfo != null && !consumerGroupInfo.getAllChannel().isEmpty()) {
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = consumerGroupInfo.getChannelInfoTable();
            for (Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
                int version = entry.getValue().getVersion();
                if (version >= MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                    try {
                        this.brokerController.getRemotingServer().invokeOneway(entry.getKey(), request, 5000);
                        log.info("[reset-offset] reset offset success. topic={}, group={}, clientId={}",
                            topic, group, entry.getValue().getClientId());
                    } catch (Exception e) {
                        log.error("[reset-offset] reset offset exception. topic={}, group={}",
                            new Object[] {topic, group}, e);
                    }
                } else {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("the client does not support this feature. version="
                        + MQVersion.getVersionDesc(version));
                    log.warn("[reset-offset] the client does not support this feature. version={}",
                        RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                    return response;
                }
            }
        } else {
            String errorInfo =
                String.format("Consumer not online, so can not reset offset, Group: %s Topic: %s Timestamp: %d",
                    requestHeader.getGroup(),
                    requestHeader.getTopic(),
                    requestHeader.getTimestamp());
            log.error(errorInfo);
            response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
            response.setRemark(errorInfo);
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        ResetOffsetBody resBody = new ResetOffsetBody();
        resBody.setOffsetTable(offsetTable);
        response.setBody(resBody.encode());
        return response;
    }

    private List<MessageQueueForC> convertOffsetTable2OffsetList(Map<MessageQueue, Long> table) {
        List<MessageQueueForC> list = new ArrayList<>();
        for (Entry<MessageQueue, Long> entry : table.entrySet()) {
            MessageQueue mq = entry.getKey();
            MessageQueueForC tmp =
                new MessageQueueForC(mq.getTopic(), mq.getBrokerName(), mq.getQueueId(), entry.getValue());
            list.add(tmp);
        }
        return list;
    }


    /**
     * 如果originClientId为空，则请求全部的status
     * 发送RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT
     */

    public RemotingCommand getConsumeStatus(String topic, String group, String originClientId) {
        final RemotingCommand result = RemotingCommand.createResponseCommand(null);

        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, requestHeader);

        Map<String, Map<MessageQueue, Long>> consumerStatusTable = new HashMap<>();
        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = this.brokerController.getConsumerManager().
                getConsumerGroupInfo(group).getChannelInfoTable();
        if (null == channelInfoTable || channelInfoTable.isEmpty()) {
            result.setCode(ResponseCode.SYSTEM_ERROR);
            result.setRemark(String.format("No Any Consumer online in the consumer group: [%s]", group));
            return result;
        }

        for (Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
            int version = entry.getValue().getVersion();
            String clientId = entry.getValue().getClientId();
            if (version < MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                result.setCode(ResponseCode.SYSTEM_ERROR);
                result.setRemark("the client does not support this feature. version="
                    + MQVersion.getVersionDesc(version));
                log.warn("[get-consumer-status] the client does not support this feature. version={}",
                    RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                return result;
            } else if (UtilAll.isBlank(originClientId) || originClientId.equals(clientId)) {
                try {
                    RemotingCommand response = this.brokerController.getRemotingServer().invokeSync(entry.getKey(), request, 5000);
                    assert response != null;
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        if (response.getBody() != null) {
                            GetConsumerStatusBody body = GetConsumerStatusBody.decode(response.getBody(), GetConsumerStatusBody.class);
                            consumerStatusTable.put(clientId, body.getMessageQueueTable());
                            log.info("[get-consumer-status] get consumer status success. topic={}, group={}, channelRemoteAddr={}", topic, group, clientId);
                        }
                    }
                } catch (Exception e) {
                    log.error(
                        "[get-consumer-status] get consumer status exception. topic={}, group={}, offset={}", new Object[] {topic, group}, e);
                }

                if (!UtilAll.isBlank(originClientId) && originClientId.equals(clientId)) {
                    break;
                }
            }
        }

        result.setCode(ResponseCode.SUCCESS);
        GetConsumerStatusBody resBody = new GetConsumerStatusBody();
        resBody.setConsumerTable(consumerStatusTable);
        result.setBody(resBody.encode());
        return result;
    }
}
