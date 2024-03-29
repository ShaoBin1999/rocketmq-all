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
import com.bsren.rocketmq.broker.pagecache.OneMessageTransfer;
import com.bsren.rocketmq.broker.pagecache.QueryMessageTransfer;
import com.bsren.rocketmq.common.MixAll;
import com.bsren.rocketmq.common.constant.LoggerName;
import com.bsren.rocketmq.common.protocol.RequestCode;
import com.bsren.rocketmq.common.protocol.ResponseCode;
import com.bsren.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import com.bsren.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import com.bsren.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import com.bsren.rocketmq.remoting.exception.RemotingCommandException;
import com.bsren.rocketmq.remoting.netty.NettyRequestProcessor;
import com.bsren.rocketmq.remoting.protocol.RemotingCommand;
import com.bsren.rocketmq.store.QueryMessageResult;
import com.bsren.rocketmq.store.SelectMappedBufferResult;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RequestCode.QUERY_MESSAGE
 * RequestCode.VIEW_MESSAGE_BY_ID
 */
public class QueryMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public QueryMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.QUERY_MESSAGE:
                return this.queryMessage(ctx, request);
            case RequestCode.VIEW_MESSAGE_BY_ID:
                return this.viewMessageById(ctx, request);
            default:
                break;
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand queryMessage(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(QueryMessageResponseHeader.class);
        final QueryMessageResponseHeader responseHeader = (QueryMessageResponseHeader) response.getCustomHeader();
        final QueryMessageRequestHeader requestHeader =
                (QueryMessageRequestHeader) request.decodeCommandCustomHeader(QueryMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        String isUniqueKey = request.getExtFields().get(MixAll.UNIQUE_MSG_QUERY_FLAG);
        if (isUniqueKey != null && isUniqueKey.equals("true")) {
            requestHeader.setMaxNum(this.brokerController.getMessageStoreConfig().getDefaultQueryMaxNum());
        }

        final QueryMessageResult queryMessageResult =
            this.brokerController.getMessageStore().queryMessage(requestHeader.getTopic(),
                requestHeader.getKey(), requestHeader.getMaxNum(), requestHeader.getBeginTimestamp(),
                requestHeader.getEndTimestamp());
        assert queryMessageResult != null;

        responseHeader.setIndexLastUpdatePhyoffset(queryMessageResult.getIndexLastUpdatePhyoffset());
        responseHeader.setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp());

        if (queryMessageResult.getBufferTotalSize() > 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            try {
                //todo fileRegion
                FileRegion fileRegion =
                    new QueryMessageTransfer(response.encodeHeader(queryMessageResult.getBufferTotalSize()), queryMessageResult);
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        queryMessageResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer query message by page cache failed, ", future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                queryMessageResult.release();
            }
            return null;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("can not find message, maybe time range not correct");
        return response;
    }

    public RemotingCommand viewMessageById(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ViewMessageRequestHeader requestHeader =
            (ViewMessageRequestHeader) request.decodeCommandCustomHeader(ViewMessageRequestHeader.class);
        response.setOpaque(request.getOpaque());
        final SelectMappedBufferResult selectMappedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(requestHeader.getOffset());
        if (selectMappedBufferResult != null) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            try {
                FileRegion fileRegion =
                    new OneMessageTransfer(response.encodeHeader(selectMappedBufferResult.getSize()), selectMappedBufferResult);
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        selectMappedBufferResult.release();
                        if (!future.isSuccess()) {
                            log.error("Transfer one message from page cache failed, ", future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                selectMappedBufferResult.release();
            }

            return null;
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("can not find message by the offset, " + requestHeader.getOffset());
        }

        return response;
    }
}
