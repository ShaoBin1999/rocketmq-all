package com.bsren.rocketmq.remoting.protocol;

import com.bsren.rocketmq.remoting.exception.RemotingCommandException;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RocketMQSerializable {

    private static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    public static void writeStr(ByteBuf byteBuf,boolean useShortLength,String str){
        int lenIndex = byteBuf.writerIndex();
        if(useShortLength){
            byteBuf.writeShort(0);
        }else {
            byteBuf.writeInt(0);
        }
        int len = byteBuf.writeCharSequence(str,CHARSET_UTF8);
        if(useShortLength){
            byteBuf.setShort(lenIndex,len);
        }else {
            byteBuf.setInt(lenIndex,len);
        }
    }

    public static String readStr(ByteBuf byteBuf,boolean useShortLength,int limit) throws RemotingCommandException{
        int len = useShortLength?byteBuf.readShort():byteBuf.readInt();
        if(len==0){
            return null;
        }
        if(len>limit){
            throw new RemotingCommandException("string length exceed limit:" + limit);
        }
        CharSequence cs = byteBuf.readCharSequence(len,CHARSET_UTF8);
        return cs==null?null:cs.toString();
    }

    /**
     * 返回写入的长度
     */
    public static int rocketMQProtocolEncode(RemotingCommand cmd,ByteBuf out){
        int beginIndex = out.writerIndex();

        out.writeShort(cmd.getCode());
        //2.语言
        out.writeByte(cmd.getLanguage().getCode());
        //3.版本
        out.writeShort(cmd.getVersion());
        //4.序号
        out.writeInt(cmd.getOpaque());
        //5.flag
        out.writeInt(cmd.getFlag());
        //6.remark
        String remark = cmd.getRemark();
        if(remark==null || remark.isEmpty()){
            out.writeInt(0);
        }else {
            writeStr(out,false,remark);
        }
        //7.字段
        int mapLenIndex = out.writerIndex();
        out.writeInt(0);
        if(cmd.getCustomHeader() instanceof FastCodesHeader){
            ((FastCodesHeader)cmd.getCustomHeader()).encode(out);
        }
        HashMap<String, String> map = cmd.getExtFields();
        if(map!=null && !map.isEmpty()){
            map.forEach((k,v)->{
                if(k!=null && v!=null){
                    writeStr(out,true,k);
                    writeStr(out,false,v);
                }
            });
        }
        out.setInt(mapLenIndex, out.writerIndex() - mapLenIndex - 4);
        return out.writerIndex() - beginIndex;
    }

    public static byte[] rocketMQProtocolEncode(RemotingCommand cmd){
        byte[] remarkBytes = null;
        int remarkLen = 0;
        if(cmd.getRemark()!=null && !cmd.getRemark().isEmpty()){
            remarkBytes = cmd.getRemark().getBytes(CHARSET_UTF8);
            remarkLen = remarkBytes.length;
        }

        byte[] extFieldsBytes = null;
        int extLen = 0;
        if(cmd.getExtFields()!=null && !cmd.getExtFields().isEmpty()){
            extFieldsBytes = mapSerialize(cmd.getExtFields());
            extLen = extFieldsBytes.length;
        }
        int totalLen = calTotalLen(remarkLen,extLen);
        ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);
        headerBuffer.putShort((short)cmd.getCode());
        headerBuffer.put(cmd.getLanguage().getCode());
        headerBuffer.putShort((short) cmd.getVersion());
        headerBuffer.putInt(cmd.getOpaque());
        headerBuffer.putInt(cmd.getFlag());
        if(remarkBytes!=null){
            headerBuffer.putInt(remarkLen);
            headerBuffer.put(remarkBytes);
        }else {
            headerBuffer.putInt(0);
        }
        if(extFieldsBytes!=null){
            headerBuffer.putInt(extLen);
            headerBuffer.put(extFieldsBytes);
        }else {
            headerBuffer.putInt(0);
        }
        return headerBuffer.array();
    }

    public static RemotingCommand rocketMQProtocolDecode(final ByteBuf byteBuf,int headerLen) throws RemotingCommandException {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(byteBuf.readShort());
        cmd.setLanguage(LanguageCode.valueOf(byteBuf.readByte()));
        cmd.setVersion(byteBuf.readShort());
        cmd.setOpaque(byteBuf.readInt());
        cmd.setFlag(byteBuf.readInt());
        cmd.setRemark(readStr(byteBuf,false,headerLen));
        int extFieldsLength = byteBuf.readInt();
        if(extFieldsLength>0){
            //todo
            if(extFieldsLength>headerLen){
                throw new RemotingCommandException("RocketMQ protocol decoding failed, extFields length: " + extFieldsLength + ", but header length: " + headerLen);
            }
            cmd.setExtFields(mapDeserialize(byteBuf,extFieldsLength));
        }
        return cmd;
    }

    public static byte[] mapSerialize(HashMap<String,String> map){
        if(map==null || map.isEmpty()){
            return null;
        }
        int totalLen = 0;
        int kvLength;
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, String> entry = it.next();
            if(entry.getKey()!=null && entry.getValue()!=null){
                kvLength = 2+entry.getKey().getBytes(CHARSET_UTF8).length+
                        4+entry.getValue().getBytes(CHARSET_UTF8).length;
                totalLen+=kvLength;
            }
        }
        ByteBuffer content = ByteBuffer.allocate(totalLen);
        byte[] key;
        byte[] val;
        it = map.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, String> entry = it.next();
            if(entry.getKey()!=null && entry.getValue()!=null){
                key = entry.getKey().getBytes(CHARSET_UTF8);
                val = entry.getValue().getBytes(CHARSET_UTF8);
                content.putShort((short) key.length);
                content.put(key);
                content.putInt(val.length);
                content.put(val);
            }
        }
        return content.array();
    }

    public static HashMap<String,String> mapDeserialize(ByteBuf byteBuf,int len) throws RemotingCommandException {
        HashMap<String,String> map = new HashMap<>();
        int endIndex = byteBuf.readerIndex()+len;
        while (byteBuf.readerIndex()<endIndex){
            String k = readStr(byteBuf,true,len);
            String v = readStr(byteBuf,false,len);
            map.put(k,v);
        }
        return map;
    }

    private static int calTotalLen(int remark,int ext){
        int length =
                2+
                1+
                2+
                4+
                4+
                4+remark
                +4+ext;
        return length;
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

}
