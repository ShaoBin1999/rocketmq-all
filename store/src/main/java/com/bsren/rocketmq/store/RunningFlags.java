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

/**
 * 值得学习的地方，使用单独的一个类，用10011这种位来表示状态，将状态从另外一个类中抽离出来
 * 记录文件运行的状态
 * NOT_READABLE 不可读
 * NOT_WRITEABLE 不可写
 * WRITE_LOGICS_QUEUE_ERROR 写队列错误
 * WRITE_INDEX_FILE_ERROR   写索引错误
 * DISK_FULL 文件已满
 */
public class RunningFlags {

    private static final int NOT_READABLE_BIT = 1;

    private static final int NOT_WRITEABLE_BIT = 1 << 1;

    private static final int WRITE_LOGICS_QUEUE_ERROR_BIT = 1 << 2;

    private static final int WRITE_INDEX_FILE_ERROR_BIT = 1 << 3;

    private static final int DISK_FULL_BIT = 1 << 4;

    //todo
    private volatile int flagBits = 0;

    public RunningFlags() {
    }

    public int getFlagBits() {
        return flagBits;
    }

    public boolean getAndMakeReadable() {
        boolean result = this.isReadable();
        if (!result) {
            this.flagBits &= ~NOT_READABLE_BIT;
        }
        return result;
    }

    public boolean isReadable() {
        if ((this.flagBits & NOT_READABLE_BIT) == 0) {
            return true;
        }

        return false;
    }

    public boolean getAndMakeNotReadable() {
        boolean result = this.isReadable();
        if (result) {
            this.flagBits |= NOT_READABLE_BIT;
        }
        return result;
    }

    public boolean getAndMakeWriteable() {
        boolean result = this.isWriteable();
        if (!result) {
            this.flagBits &= ~NOT_WRITEABLE_BIT;
        }
        return result;
    }

    public boolean isWriteable() {
        if ((this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | DISK_FULL_BIT | WRITE_INDEX_FILE_ERROR_BIT)) == 0) {
            return true;
        }

        return false;
    }

    //for consume queue, just ignore the DISK_FULL_BIT
    public boolean isCQWriteable() {
        if ((this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | WRITE_INDEX_FILE_ERROR_BIT)) == 0) {
            return true;
        }

        return false;
    }

    public boolean getAndMakeNotWriteable() {
        boolean result = this.isWriteable();
        if (result) {
            this.flagBits |= NOT_WRITEABLE_BIT;
        }
        return result;
    }

    public void makeLogicsQueueError() {
        this.flagBits |= WRITE_LOGICS_QUEUE_ERROR_BIT;
    }

    public boolean isLogicsQueueError() {
        if ((this.flagBits & WRITE_LOGICS_QUEUE_ERROR_BIT) == WRITE_LOGICS_QUEUE_ERROR_BIT) {
            return true;
        }

        return false;
    }

    public void makeIndexFileError() {
        this.flagBits |= WRITE_INDEX_FILE_ERROR_BIT;
    }

    public boolean isIndexFileError() {
        if ((this.flagBits & WRITE_INDEX_FILE_ERROR_BIT) == WRITE_INDEX_FILE_ERROR_BIT) {
            return true;
        }

        return false;
    }

    public boolean getAndMakeDiskFull() {
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        this.flagBits |= DISK_FULL_BIT;
        return result;
    }

    public boolean getAndMakeDiskOK() {
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        this.flagBits &= ~DISK_FULL_BIT;
        return result;
    }
}
