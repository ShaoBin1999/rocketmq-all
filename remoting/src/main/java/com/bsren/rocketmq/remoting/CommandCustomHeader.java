package com.bsren.rocketmq.remoting;

import com.bsren.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 请求头
 */
public interface CommandCustomHeader {

    void checkFields() throws RemotingCommandException;

}
