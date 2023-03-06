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
package com.bsren.rocketmq.store;

import com.bsren.rocketmq.common.constant.LoggerName;
import com.bsren.rocketmq.store.config.MessageStoreConfig;
import com.bsren.rocketmq.store.util.LibC;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 刷盘是否开启TransientStorePool的区别
 * 不开启TransientStorePool：
 * MappedByteBuffer是直接内存，它暂时存储了message消息，
 * MappedFile.mapp()方法做好MappedByteBuffer对象直接内存和落盘文件的映射关系，
 * 然后flush()方法执行MappedByteBuffer.force()：
 * 强制将ByteBuffer中的任何内容的改变写入到磁盘文件，读写都经过Page Cache。
 *
 * 开启TransientStorePool：
 * TransientStorePool会通过ByteBuffer.allocateDirect调用直接申请对外内存writerBuffer，
 * 消息数据在写入内存的时候是写入预申请的内存中。
 * MappedFile的writerBuffer为直接开辟的内存，
 * 然后MappedFile的初始化操作，做好FileChannel和磁盘文件的映射，
 * commit()方法实质是执行fileChannel.write(writerBuffer)，将writerBuffer的数据写入到FileChannel映射的磁盘文件，
 * flush操作执行FileChannel.force():将映射文件中的数据强制刷新到磁盘。
 * 写入的时候不经过PageCache，因此在消息写入操作上会更快，因此能更少的占用CommitLog.putMessageLock锁，从而能够提升消息处理量。
 * 使用TransientStorePool方案的缺陷主要在于在异常崩溃的情况下回丢失更多的消息。
 *
 * TransientStorePool的作用
 * TransientStorePool 相当于在内存层面做了读写分离，写走内存磁盘，读走pagecache，同时最大程度消除了page cache的锁竞争，降低了毛刺。它还使用了锁机制，避免直接内存被交换到swap分区。
 *
 * 日常FileChannel的写操作会经过Page Cache,但是TransientStorePool开辟了直接内存WriterBuffer，WriterBuffer只负责写入，也是通过FileChannel写入磁盘，读操作由单独的MappedByteBuffer负责，这样实现了读写分离。
 */
public class TransientStorePool {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final int poolSize;
    private final int fileSize;
    private final Deque<ByteBuffer> availableBuffers;
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMapedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int remainBufferNumbs() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
