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

package org.sail.mq.tieredstore.stream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.sail.mq.tieredstore.common.FileSegmentType;
import org.sail.mq.tieredstore.util.MessageFormatUtil;

public class CommitLogInputStream extends FileSegmentInputStream {

    /**
     * commitLogOffset is the real physical offset of the commitLog buffer which is being read
     */
    private final long startCommitLogOffset;

    private long commitLogOffset;

    private final ByteBuffer codaBuffer;

    private long markCommitLogOffset = -1;

    public CommitLogInputStream(FileSegmentType fileType, long startOffset,
        List<ByteBuffer> uploadBufferList, ByteBuffer codaBuffer, int contentLength) {
        super(fileType, uploadBufferList, contentLength);
        this.startCommitLogOffset = startOffset;
        this.commitLogOffset = startOffset;
        this.codaBuffer = codaBuffer;
    }

    @Override
    public synchronized void mark(int ignore) {
        super.mark(ignore);
        this.markCommitLogOffset = commitLogOffset;
    }

    @Override
    public synchronized void reset() throws IOException {
        super.reset();
        this.commitLogOffset = markCommitLogOffset;
    }

    @Override
    public synchronized void rewind() {
        super.rewind();
        this.commitLogOffset = this.startCommitLogOffset;
        if (this.codaBuffer != null) {
            this.codaBuffer.rewind();
        }
    }

    @Override
    public ByteBuffer getCodaBuffer() {
        return this.codaBuffer;
    }

    @Override
    public int read() {
        if (available() <= 0) {
            return -1;
        }
        readPosition++;
        if (curReadBufferIndex >= bufferList.size()) {
            return readCoda();
        }
        int res;
        if (readPosInCurBuffer >= curBuffer.remaining()) {
            curReadBufferIndex++;
            if (curReadBufferIndex >= bufferList.size()) {
                readPosInCurBuffer = 0;
                return readCoda();
            }
            curBuffer = bufferList.get(curReadBufferIndex);
            commitLogOffset += readPosInCurBuffer;
            readPosInCurBuffer = 0;
        }
        if (readPosInCurBuffer >= MessageFormatUtil.PHYSICAL_OFFSET_POSITION
            && readPosInCurBuffer < MessageFormatUtil.SYS_FLAG_OFFSET_POSITION) {
            res = (int) ((commitLogOffset >> (8 * (MessageFormatUtil.SYS_FLAG_OFFSET_POSITION - readPosInCurBuffer - 1))) & 0xff);
            readPosInCurBuffer++;
        } else {
            res = curBuffer.get(readPosInCurBuffer++) & 0xff;
        }
        return res;
    }

    private int readCoda() {
        if (codaBuffer == null || readPosInCurBuffer >= codaBuffer.remaining()) {
            return -1;
        }
        return codaBuffer.get(readPosInCurBuffer++) & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException("off < 0 || len < 0 || len > b.length - off");
        }
        if (readPosition >= contentLength) {
            return -1;
        }

        int available = available();
        if (len > available) {
            len = available;
        }
        if (len <= 0) {
            return 0;
        }
        int needRead = len;
        int pos = readPosition;
        int bufIndex = curReadBufferIndex;
        int posInCurBuffer = readPosInCurBuffer;
        long curCommitLogOffset = commitLogOffset;
        ByteBuffer curBuf = curBuffer;
        while (needRead > 0 && bufIndex <= bufferList.size()) {
            int readLen, remaining, realReadLen = 0;
            if (bufIndex == bufferList.size()) {
                // read from coda buffer
                remaining = codaBuffer.remaining() - posInCurBuffer;
                readLen = Math.min(remaining, needRead);
                codaBuffer.position(posInCurBuffer);
                codaBuffer.get(b, off, readLen);
                codaBuffer.position(0);
                // update flags
                off += readLen;
                needRead -= readLen;
                pos += readLen;
                posInCurBuffer += readLen;
                continue;
            }
            remaining = curBuf.remaining() - posInCurBuffer;
            readLen = Math.min(remaining, needRead);
            curBuf = bufferList.get(bufIndex);
            if (posInCurBuffer < MessageFormatUtil.PHYSICAL_OFFSET_POSITION) {
                realReadLen = Math.min(MessageFormatUtil.PHYSICAL_OFFSET_POSITION - posInCurBuffer, readLen);
                // read from commitLog buffer
                curBuf.position(posInCurBuffer);
                curBuf.get(b, off, realReadLen);
                curBuf.position(0);
            } else if (posInCurBuffer < MessageFormatUtil.SYS_FLAG_OFFSET_POSITION) {
                realReadLen = Math.min(MessageFormatUtil.SYS_FLAG_OFFSET_POSITION - posInCurBuffer, readLen);
                // read from converted PHYSICAL_OFFSET_POSITION
                byte[] physicalOffsetBytes = new byte[realReadLen];
                for (int i = 0; i < realReadLen; i++) {
                    physicalOffsetBytes[i] = (byte) ((curCommitLogOffset >> (8 * (MessageFormatUtil.SYS_FLAG_OFFSET_POSITION - posInCurBuffer - i - 1))) & 0xff);
                }
                System.arraycopy(physicalOffsetBytes, 0, b, off, realReadLen);
            } else {
                realReadLen = readLen;
                // read from commitLog buffer
                curBuf.position(posInCurBuffer);
                curBuf.get(b, off, readLen);
                curBuf.position(0);
            }
            // update flags
            off += realReadLen;
            needRead -= realReadLen;
            pos += realReadLen;
            posInCurBuffer += realReadLen;
            if (posInCurBuffer == curBuf.remaining()) {
                // read from next buf
                bufIndex++;
                curCommitLogOffset += posInCurBuffer;
                posInCurBuffer = 0;
            }
        }
        readPosition = pos;
        curReadBufferIndex = bufIndex;
        readPosInCurBuffer = posInCurBuffer;
        commitLogOffset = curCommitLogOffset;
        curBuffer = curBuf;
        return len;
    }
}
