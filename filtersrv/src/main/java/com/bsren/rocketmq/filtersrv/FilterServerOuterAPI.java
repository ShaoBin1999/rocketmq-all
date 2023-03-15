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
package com.bsren.rocketmq.filtersrv;

import com.bsren.rocketmq.client.exception.MQBrokerException;
import com.bsren.rocketmq.common.protocol.RequestCode;
import com.bsren.rocketmq.common.protocol.ResponseCode;
import com.bsren.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerRequestHeader;
import com.bsren.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerResponseHeader;
import com.bsren.rocketmq.remoting.RemotingClient;
import com.bsren.rocketmq.remoting.exception.RemotingCommandException;
import com.bsren.rocketmq.remoting.exception.RemotingConnectException;
import com.bsren.rocketmq.remoting.exception.RemotingSendRequestException;
import com.bsren.rocketmq.remoting.exception.RemotingTimeoutException;
import com.bsren.rocketmq.remoting.netty.NettyClientConfig;
import com.bsren.rocketmq.remoting.netty.NettyRemotingClient;
import com.bsren.rocketmq.remoting.protocol.RemotingCommand;


public class FilterServerOuterAPI {
    private final RemotingClient remotingClient;

    public FilterServerOuterAPI() {
        this.remotingClient = new NettyRemotingClient(new NettyClientConfig());
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
    }

    public RegisterFilterServerResponseHeader registerFilterServerToBroker(final String brokerAddr, final String filterServerAddr)
            throws RemotingCommandException,
            RemotingConnectException,
            RemotingSendRequestException,
            RemotingTimeoutException,
            InterruptedException,
            MQBrokerException
    {
        RegisterFilterServerRequestHeader requestHeader = new RegisterFilterServerRequestHeader();
        requestHeader.setFilterServerAddr(filterServerAddr);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_FILTER_SERVER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);
        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return (RegisterFilterServerResponseHeader) response
                    .decodeCommandCustomHeader(RegisterFilterServerResponseHeader.class);
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }
}
