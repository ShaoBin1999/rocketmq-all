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
package com.bsren.rocketmq.broker.pagecache;

import com.bsren.rocketmq.store.SelectMappedBufferResult;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class OneMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final SelectMappedBufferResult selectMappedBufferResult;

    /**
     * Bytes which were transferred already.
     */
    private long transferred;

    public OneMessageTransfer(ByteBuffer byteBufferHeader, SelectMappedBufferResult selectMappedBufferResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.selectMappedBufferResult = selectMappedBufferResult;
    }

    @Override
    public long position() {
        return this.byteBufferHeader.position() + this.selectMappedBufferResult.getByteBuffer().position();
    }

    @Override
    public long transfered() {
        return 0;
    }


    @Override
    public long count() {
        return this.byteBufferHeader.limit() + this.selectMappedBufferResult.getSize();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += target.write(this.byteBufferHeader);
            return transferred;
        } else if (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
            transferred += target.write(this.selectMappedBufferResult.getByteBuffer());
            return transferred;
        }

        return 0;
    }


    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
        this.selectMappedBufferResult.release();
    }
}
