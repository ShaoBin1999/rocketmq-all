package com.bsren.rocketmq.remoting.protocol;

import com.bsren.rocketmq.remoting.exception.RemotingCommandException;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;

public interface FastCodesHeader {

    default String getAndCheckNotNull(HashMap<String,String> fields,String field){
        String value = fields.get(field);
        if(value==null){
            String headerClass = this.getClass().getSimpleName();
            RemotingCommand.log.error("the custom field {}.{} is null", headerClass, field);
        }
        return value;
    }

    default void writeIfNotNull(ByteBuf out, String key, Object value) {
        if (value != null) {
            RocketMQSerializable.writeStr(out, true, key);
            RocketMQSerializable.writeStr(out, false, value.toString());
        }
    }

    void encode(ByteBuf out);

    void decode(HashMap<String, String> fields) throws RemotingCommandException;

}
