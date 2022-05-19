/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.impl.chunkstream;

import com.emc.storageos.data.cs.common.CSException;
import com.emc.storageos.data.cs.common.ChunkConfig;
import com.emc.storageos.data.cs.dt.CmClient;
import com.emc.storageos.data.cs.stream.ChunkStreamReader;
import com.emc.storageos.data.cs.stream.StreamBuffer;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Performs read from Chunk Stream Logs.
 */
@Slf4j
@NotThreadSafe
public class LogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {

    //region Members

    private final String logId;
    private final LogMetadata metadata;
    private final CmClient cmClient;
    private final ChunkConfig chunkConfig;
    private final Executor executor;
    private final int readTimeout;
    private final AtomicBoolean closed;
    private ChunkStreamReader streamReader;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogReader class.
     *
     * @param logId       The id of the {@link ChunkStreamLog} to read from. This is used for validation purposes.
     * @param metadata    The LogMetadata of the Log to read.
     * @param cmClient    A reference to the cm client to use.
     * @param chunkConfig Configuration for chunk to use.
     * @param executor    An Executor to use for async operations.
     */
    LogReader(String logId, LogMetadata metadata, CmClient cmClient, ChunkConfig chunkConfig, ChunkStreamConfig config, Executor executor) throws DurableDataLogException {
        this.logId = logId;
        this.metadata = Preconditions.checkNotNull(metadata, "metadata");
        this.cmClient = Preconditions.checkNotNull(cmClient, "cmClient");
        this.chunkConfig = Preconditions.checkNotNull(chunkConfig, "chunkConfig");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.readTimeout = (int) Math.ceil(config.getChunkStreamReadTimeoutMillis() / 1000.0);
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.streamReader != null) {
                try {
                    ChunkStreams.close(streamReader);
                } catch (DurableDataLogException ex) {
                    log.error("Unable to close stream reader {}.", streamReader.streamId(), ex);
                }
                this.streamReader = null;
            }
        }
    }

    //endregion

    //region CloseableIterator Implementation
    @Override
    public DurableDataLog.ReadItem getNext() throws DurableDataLogException {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (this.streamReader == null) {
            ChunkStreamReader streamReader = new ChunkStreamReader(cmClient, chunkConfig, executor);
            ChunkStreams.open(this.logId, metadata.getTruncationAddress().getSequence(), streamReader);
            this.streamReader = streamReader;
        }

        try {
            StreamBuffer streamBuffer = this.streamReader.nextBuffer().get(this.readTimeout, TimeUnit.SECONDS);
            return wrapItem(streamBuffer);
        } catch (Exception ex) {
            if (ex.getCause() instanceof CSException) {
                return null;
            } else {
                close();
                throw new DurableDataLogException(String.format("Error while reading from chunk stream %s.", this.streamReader.streamId()), ex);
            }
        }
    }

    //endregion

    //region ReadItem

    private static class ReadItem implements DurableDataLog.ReadItem {
        @Getter
        private final InputStream payload;
        @Getter
        private final int length;
        @Getter
        private final StreamAddress address;

        ReadItem(InputStream payload, int length, long offset) {
            this.payload = payload;
            this.length = length;
            this.address = new StreamAddress(offset);
        }

        @Override
        public String toString() {
            return String.format("%s, Length = %d.", this.address, this.length);
        }
    }

    private static DurableDataLog.ReadItem wrapItem(StreamBuffer streamBuffer) {
        byte[] data = new byte[streamBuffer.size()];
        streamBuffer.data().get(data);
        return new ReadItem(new ByteArrayInputStream(data), data.length, streamBuffer.offset());
    }

    //endregion
}
