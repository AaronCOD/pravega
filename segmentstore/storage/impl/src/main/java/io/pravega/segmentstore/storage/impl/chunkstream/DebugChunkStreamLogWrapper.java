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

import com.emc.storageos.data.cs.common.ChunkConfig;
import com.emc.storageos.data.cs.dt.CmClient;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ReadOnlyLogMetadata;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.WriteSettings;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper for a ChunkStreamLog which only exposes methods that should be used for debugging/admin tools.
 * NOTE: this class is not meant to be used for regular, production code. It exposes operations that should only be executed
 * from the admin tools.
 */
public class DebugChunkStreamLogWrapper implements DebugDurableDataLogWrapper {
    //region Members

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private final ChunkStreamLog log;
    private final CmClient cmClient;
    private final ChunkConfig chunkConfig;
    private final ChunkStreamConfig config;
    private final Executor executor;
    private final AtomicBoolean initialized;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DebugLogWrapper class.
     *
     * @param logId       The id of the ChunkStreamLog to wrap.
     * @param zkClient    A pointer to the CuratorFramework client to use.
     * @param cmClient    A pointer to the cm client to use.
     * @param chunkConfig Configuration for chunk to use.
     * @param config      Configuration for chunk stream log to use.
     * @param executor    An Executor to use for async operations.
     */
    DebugChunkStreamLogWrapper(int logId, CuratorFramework zkClient, CmClient cmClient, ChunkConfig chunkConfig, ChunkStreamConfig config, ScheduledExecutorService executor) {
        this.log = new ChunkStreamLog(logId, zkClient, cmClient, chunkConfig, config, executor);
        this.cmClient = cmClient;
        this.chunkConfig = chunkConfig;
        this.config = config;
        this.executor = executor;
        this.initialized = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.log.close();
    }

    //endregion

    //region Operations

    @Override
    public DurableDataLog asReadOnly() throws DataLogInitializationException {
        return new DebugChunkStreamLogWrapper.ReadOnlyChunkStreamLog(this.log.getLogId(), this.log.loadMetadata());
    }

    /**
     * Loads a fresh copy ChunkStreamLog Metadata from ZooKeeper, without doing any sort of fencing or otherwise modifying
     * it.
     *
     * @return A new instance of the LogMetadata class, or null if no such metadata exists (most likely due to this being
     * the first time accessing this log).
     * @throws DataLogInitializationException If an Exception occurred.
     */
    @Override
    public ReadOnlyLogMetadata fetchMetadata() throws DataLogInitializationException {
        return this.log.loadMetadata();
    }

    /**
     * Allows to overwrite the metadata of a ChunkStreamLog. CAUTION: This is a destructive operation and should be
     * used wisely for administration purposes (e.g., repair a damaged ChunkStreamLog).
     *
     * @param metadata New metadata to set in the original ChunkStreamLog metadata path.
     * @throws DurableDataLogException in case there is a problem managing metadata from Zookeeper.
     */
    @Override
    public void forceMetadataOverWrite(ReadOnlyLogMetadata metadata) throws DurableDataLogException {
        try {
            byte[] serializedMetadata = LogMetadata.SERIALIZER.serialize((LogMetadata) metadata).getCopy();
            this.log.getZkClient().setData().forPath(this.log.getLogNodePath(), serializedMetadata);
        } catch (Exception e) {
            throw new DurableDataLogException("Problem overwriting Chunk Stream Log metadata.", e);
        }
    }

    /**
     * Delete the metadata of the ChunkStreamLog in Zookeeper. CAUTION: This is a destructive operation and should be
     * used wisely for administration purposes (e.g., repair a damaged ChunkStreamLog).
     *
     * @throws DurableDataLogException in case there is a problem managing metadata from Zookeeper.
     */
    @Override
    public void deleteDurableLogMetadata() throws DurableDataLogException {
        try {
            this.log.getZkClient().delete().forPath(this.log.getLogNodePath());
        } catch (Exception e) {
            throw new DurableDataLogException("Problem deleting Chunk Stream Log metadata.", e);
        }
    }

    /**
     * Updates the Metadata for this ChunkStreamLog in ZooKeeper by setting its Enabled flag to true.
     * @throws DurableDataLogException If an exception occurred.
     */
    public void enable() throws DurableDataLogException {
        this.log.enable();
    }

    /**
     * Initialize the ChunkStreamLog (initializes it), then updates the Metadata for it in ZooKeeper by setting its
     * Enabled flag to false.
     * @throws DurableDataLogException If an exception occurred.
     */
    public void disable() throws DurableDataLogException {
        initialize();
        this.log.disable();
    }

    private void initialize() throws DurableDataLogException {
        if (this.initialized.compareAndSet(false, true)) {
            try {
                this.log.initialize(DEFAULT_TIMEOUT);
            } catch (Exception ex) {
                this.initialized.set(false);
                throw ex;
            }
        }
    }

    //endregion

    //region ReadOnlyChunkStreamLog

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private class ReadOnlyChunkStreamLog implements DurableDataLog {
        private final String logId;
        private final LogMetadata logMetadata;

        @Override
        public void close() {
            // Nothing to do.
        }

        @Override
        public CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException {
            return new LogReader(this.logId, this.logMetadata, DebugChunkStreamLogWrapper.this.cmClient, DebugChunkStreamLogWrapper.this.chunkConfig, DebugChunkStreamLogWrapper.this.config, DebugChunkStreamLogWrapper.this.executor);
        }

        @Override
        public WriteSettings getWriteSettings() {
            return new WriteSettings(ChunkStreamConfig.MAX_APPEND_LENGTH,
                    Duration.ofMillis(ChunkStreamConfig.CHUNK_STREAM_WRITE_TIMEOUT.getDefaultValue()),
                    ChunkStreamConfig.MAX_OUTSTANDING_BYTES.getDefaultValue());
        }

        @Override
        public long getEpoch() {
            return this.logMetadata.getEpoch();
        }

        @Override
        public QueueStats getQueueStatistics() {
            return null;
        }

        @Override
        public void registerQueueStateChangeListener(ThrottleSourceListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initialize(Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
            throw new UnsupportedOperationException();
        }
    }

    //endregion
}
