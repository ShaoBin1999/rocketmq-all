package com.bsren.rocketmq.client.producer.selector;

import com.bsren.rocketmq.common.message.Message;
import com.bsren.rocketmq.common.message.MessageQueue;

import java.util.List;

public interface MessageQueueSelector {
    MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}
