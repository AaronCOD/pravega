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
import com.emc.storageos.data.cs.stream.ChunkStreamWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.segmentstore.storage.DataLogDisabledException;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.ThrottlerSourceListenerCollection;
import io.pravega.segmentstore.storage.WriteSettings;
import io.pravega.segmentstore.storage.WriteTooLongException;
import io.pravega.segmentstore.storage.impl.HierarchyUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ThreadSafe
public class ChunkStreamLog implements DurableDataLog {
    //region Members

    private static final long REPORT_INTERVAL = 1000;
    @Getter
    private final String logId;
    @Getter(AccessLevel.PACKAGE)
    private final String logNodePath;
    @Getter(AccessLevel.PACKAGE)
    private final CuratorFramework zkClient;
    private final CmClient cmClient;
    private final ChunkConfig chunkConfig;
    private final ChunkStreamConfig config;
    private final ScheduledExecutorService executorService;
    private final AtomicBoolean closed;
    private final Object lock = new Object();
    private final String traceObjectId;
    @GuardedBy("lock")
    private ChunkStreamWriter streamWriter;
    @GuardedBy("lock")
    private LogMetadata logMetadata;
    private final ChunkMetrics.ChunkStreamLog metrics;
    private final ScheduledFuture<?> metricReporter;
    private final ThrottlerSourceListenerCollection queueStateChangeListeners;
    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Chunk Log class.
     *
     * @param containerId     The id of the Container whose ChunkStreamLog to open.
     * @param zkClient        A reference to the CuratorFramework client to use.
     * @param cmClient        A reference to the cm client to use.
     * @param chunkConfig     Configuration for chunk to use.
     * @param config          Configuration for chunk stream log to use.
     * @param executorService An Executor to use for async operations.
     */
    ChunkStreamLog(int containerId, CuratorFramework zkClient, CmClient cmClient, ChunkConfig chunkConfig, ChunkStreamConfig config, ScheduledExecutorService executorService) {
        Preconditions.checkArgument(containerId >= 0, "containerId must be a non-negative integer.");
        this.logId = String.valueOf(containerId);
        this.zkClient = Preconditions.checkNotNull(zkClient, "zkClient");
        this.cmClient = Preconditions.checkNotNull(cmClient, "cmClient");
        this.chunkConfig = Preconditions.checkNotNull(chunkConfig, "chunkConfig");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
        this.closed = new AtomicBoolean();
        this.logNodePath = HierarchyUtils.getPath(containerId, this.config.getZkHierarchyDepth());
        this.traceObjectId = String.format("Log[%d]", containerId);
        this.metrics = new ChunkMetrics.ChunkStreamLog(containerId);
        this.metricReporter = this.executorService.scheduleWithFixedDelay(this::reportMetrics, REPORT_INTERVAL, REPORT_INTERVAL, TimeUnit.MILLISECONDS);
        this.queueStateChangeListeners = new ThrottlerSourceListenerCollection();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.metricReporter.cancel(true);
            this.metrics.close();

            // Close active chunk.
            ChunkStreamWriter streamWriter;
            synchronized (this.lock) {
                streamWriter = this.streamWriter;
                this.streamWriter = null;
                this.logMetadata = null;
            }

            try {
                ChunkStreams.close(streamWriter);
            } catch (DurableDataLogException ex) {
                log.error("{}: Unable to close stream writer {}.", this.traceObjectId, streamWriter.streamId(), ex);
            }

            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region DurableDataLog Implementation

    /**
     * Open-Fences this Chunk log using the following protocol:
     * 1. Read Log Metadata from ZooKeeper.
     * 2. Open the chunk stream.
     * 3. Update Log Metadata using compare-and-set (this update contains the new epoch).
     *
     * @param timeout Timeout for the operation.
     * @throws DurableDataLogException If exception occurred.
     */
    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        LogMetadata newMetadata;
        synchronized (this.lock) {
            Preconditions.checkState(this.streamWriter == null, "ChunkStreamLog is already initialized.");
            assert this.logMetadata == null : "streamWriter == null but logMetadata != null";

            // Get metadata about the current state of the log, if any.
            LogMetadata oldMetadata = loadMetadata();

            if (oldMetadata != null && !oldMetadata.isEnabled()) {
                throw new DataLogDisabledException("ChunkStreamLog is disabled. Cannot initialize.");
            }

            // Open the chunk stream
            ChunkStreamWriter streamWriter = new ChunkStreamWriter(cmClient, this.chunkConfig, executorService);
            ChunkStreams.open(this.logId, true, streamWriter);
            log.info("{}: Opened Stream {}.", this.traceObjectId, streamWriter.streamId());

            // Update Metadata and persist to ZooKeeper.
            newMetadata = updateMetadataEpoch(oldMetadata);
            this.streamWriter = streamWriter;
            this.logMetadata = newMetadata;
        }

        log.info("{}: Initialized (Epoch = {}, UpdateVersion = {}).", this.traceObjectId, newMetadata.getEpoch(), newMetadata.getUpdateVersion());
    }

    @Override
    public void enable() throws DurableDataLogException {
        // Get the current metadata, enable it, and then persist it back.
        synchronized (this.lock) {
            ensurePreconditions();
            LogMetadata metadata = loadMetadata();
            Preconditions.checkState(metadata != null && !metadata.isEnabled(), "ChunkStreamLog is already enabled.");
            metadata = metadata.asEnabled();
            persistMetadata(metadata, false);
            log.info("{}: Enabled (Epoch = {}, UpdateVersion = {}).", this.traceObjectId, metadata.getEpoch(), metadata.getUpdateVersion());
        }
    }

    @Override
    public void disable() throws DurableDataLogException {
        // Get the current metadata, disable it, and then persist it back.
        synchronized (this.lock) {
            ensurePreconditions();
            LogMetadata metadata = getLogMetadata();
            Preconditions.checkState(metadata.isEnabled(), "ChunkStreamLog is already disabled.");
            metadata = this.logMetadata.asDisabled();
            persistMetadata(metadata, false);
            this.logMetadata = metadata;
            log.info("{}: Disabled (Epoch = {}, UpdateVersion = {}).", this.traceObjectId, metadata.getEpoch(), metadata.getUpdateVersion());
        }

        // Close this instance of the ChunkStreamLog. This ensures the proper cancellation of any ongoing writes.
        close();
    }

    @Override
    public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
        ensurePreconditions();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "append", data.getLength());
        if (data.getLength() > ChunkStreamConfig.MAX_APPEND_LENGTH) {
            return Futures.failedFuture(new WriteTooLongException(data.getLength(), ChunkStreamConfig.MAX_APPEND_LENGTH));
        }

        Timer timer = new Timer();

        CompletableFuture<LogAddress> result = new CompletableFuture<>();
        try {
            CompletableFuture<Long> future = streamWriter.append(convertData(data));
            future.whenCompleteAsync((offset, ex) -> {
                if (ex != null) {
                    handleWriteException(ex);
                    result.completeExceptionally(ex);
                } else {
                    // Update metrics and take care of other logging tasks.
                    this.metrics.writeCompleted(data.getLength(), timer.getElapsed());
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "append", traceId, data.getLength(), offset);
                    result.complete(new StreamAddress(offset));
                }
            }, this.executorService);
        } catch (Exception ex) {
            handleWriteException(ex);
            result.completeExceptionally(ex);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
        ensurePreconditions();
        Preconditions.checkArgument(upToAddress instanceof StreamAddress, "upToAddress must be of type StreamAddress.");
        return CompletableFuture.runAsync(() -> tryTruncate((StreamAddress) upToAddress), this.executorService);
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException {
        ensurePreconditions();
        return new LogReader(this.logId, getLogMetadata(), this.cmClient, this.chunkConfig, this.config, this.executorService);
    }

    @Override
    public WriteSettings getWriteSettings() {
        return new WriteSettings(ChunkStreamConfig.MAX_APPEND_LENGTH,
                Duration.ofMillis(this.config.getChunkStreamWriteTimeoutMillis()),
                this.config.getMaxOutstandingBytes());
    }

    @Override
    public long getEpoch() {
        ensurePreconditions();
        return getLogMetadata().getEpoch();
    }

    @Override
    public QueueStats getQueueStatistics() {
        // return this.logStream.getQueueStatistics();
        return new QueueStats(0, 0, 1024 * 1024, 0);
    }

    @Override
    public void registerQueueStateChangeListener(ThrottleSourceListener listener) {
        this.queueStateChangeListeners.register(listener);
    }

    //endregion

    //region Writes

    /**
     * Handles a general Write exception.
     */
    private void handleWriteException(Throwable ex) {
        if (ex instanceof ObjectClosedException && !this.closed.get()) {
            log.warn("{}: Caught ObjectClosedException but not closed; closing now.", this.traceObjectId, ex);
            close();
        }
    }

    /**
     * Attempts to truncate the Log. The general steps are:
     * 1. Create an in-memory copy of the metadata reflecting the truncation.
     * 2. Attempt to persist the metadata to ZooKeeper.
     * 2.1. This is the only operation that can fail the process. If this fails, the operation stops here.
     * 3. Swap in-memory metadata pointers.
     * 4. Truncated the stream.
     * 4.1. If the stream cannot be truncated, no further attempt to clean it up is done.
     *
     * @param upToAddress The address up to which to truncate.
     */
    @SneakyThrows(DurableDataLogException.class)
    private void tryTruncate(StreamAddress upToAddress) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "tryTruncate", upToAddress);

        // Truncate the metadata and get a new copy of it.
        val oldMetadata = getLogMetadata();
        val newMetadata = oldMetadata.truncate(upToAddress);

        // Attempt to persist the new Log Metadata.
        persistMetadata(newMetadata, false);

        // Repoint our metadata to the new one.
        synchronized (this.lock) {
            this.logMetadata = newMetadata;
        }

        try {
            ChunkStreams.truncate(this.streamWriter, upToAddress);
        } catch (DurableDataLogException ex) {
            log.error("{}: Unable to truncated stream {} up to {}.", this.traceObjectId, this.streamWriter.streamId(), upToAddress, ex);
        }

        log.info("{}: Truncated up to {}.", this.traceObjectId, upToAddress);
        LoggerHelpers.traceLeave(log, this.traceObjectId, "tryTruncate", traceId, upToAddress);
    }

    //endregion

    //region Metadata Management

    /**
     * Loads the metadata for the current log, as stored in ZooKeeper.
     *
     * @return A new LogMetadata object with the desired information, or null if no such node exists.
     * @throws DataLogInitializationException If an Exception (other than NoNodeException) occurred.
     */
    @VisibleForTesting
    LogMetadata loadMetadata() throws DataLogInitializationException {
        try {
            Stat storingStatIn = new Stat();
            byte[] serializedMetadata = this.zkClient.getData().storingStatIn(storingStatIn).forPath(this.logNodePath);
            LogMetadata result = LogMetadata.SERIALIZER.deserialize(serializedMetadata);
            result.withUpdateVersion(storingStatIn.getVersion());
            return result;
        } catch (KeeperException.NoNodeException nne) {
            // Node does not exist: this is the first time we are accessing this log.
            log.warn("{}: No ZNode found for path '{}{}'. This is OK if this is the first time accessing this log.",
                    this.traceObjectId, this.zkClient.getNamespace(), this.logNodePath);
            return null;
        } catch (Exception ex) {
            throw new DataLogInitializationException(String.format("Unable to load ZNode contents for path '%s%s'.",
                    this.zkClient.getNamespace(), this.logNodePath), ex);
        }
    }

    /**
     * Updates the metadata epoch and persists it as a result of successful initialize.
     *
     * @param currentMetadata   The current metadata.
     * @return A new instance of the LogMetadata, which updates the epoch.
     * @throws DurableDataLogException If an Exception occurred.
     */
    private LogMetadata updateMetadataEpoch(LogMetadata currentMetadata) throws DurableDataLogException {
        boolean create = currentMetadata == null;
        if (create) {
            currentMetadata = new LogMetadata();
        } else {
            currentMetadata = currentMetadata.updateEpoch();
        }

        try {
            persistMetadata(currentMetadata, create);
        } catch (Exception ex) {
            log.warn("{}: Error while using ZooKeeper.", this.traceObjectId);
            throw ex;
        }

        log.info("{} Metadata updated ({}).", this.traceObjectId, currentMetadata);
        return currentMetadata;
    }

    /**
     * Persists the given metadata into ZooKeeper.
     *
     * @param metadata The LogMetadata to persist. At the end of this method, this metadata will have its Version updated
     *                 to the one in ZooKeeper.
     * @param create   Whether to create (true) or update (false) the data in ZooKeeper.
     * @throws DataLogWriterNotPrimaryException If the metadata update failed (if we were asked to create and the node
     *                                          already exists or if we had to update and there was a version mismatch).
     * @throws DurableDataLogException          If another kind of exception occurred.
     */
    private void persistMetadata(LogMetadata metadata, boolean create) throws DurableDataLogException {
        try {
            byte[] serializedMetadata = LogMetadata.SERIALIZER.serialize(metadata).getCopy();
            Stat result = create
                    ? createZkMetadata(serializedMetadata)
                    : updateZkMetadata(serializedMetadata, metadata.getUpdateVersion());
            metadata.withUpdateVersion(result.getVersion());
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException keeperEx) {
            if (reconcileMetadata(metadata)) {
                log.info("{}: Received '{}' from ZooKeeper while persisting metadata (path = '{}{}'), however metadata has been persisted correctly. Not rethrowing.",
                        this.traceObjectId, keeperEx, this.zkClient.getNamespace(), this.logNodePath);
            } else {
                // We were fenced out. Convert to an appropriate exception.
                throw new DataLogWriterNotPrimaryException(
                        String.format("Unable to acquire exclusive write lock for log (path = '%s%s').", this.zkClient.getNamespace(), this.logNodePath),
                        keeperEx);
            }
        } catch (Exception generalEx) {
            // General exception. Convert to an appropriate exception.
            throw new DataLogInitializationException(
                    String.format("Unable to update ZNode for path '%s%s'.", this.zkClient.getNamespace(), this.logNodePath),
                    generalEx);
        }

        log.info("{} Metadata persisted ({}).", this.traceObjectId, metadata);
    }

    @VisibleForTesting
    protected Stat createZkMetadata(byte[] serializedMetadata) throws Exception {
        val result = new Stat();
        this.zkClient.create().creatingParentsIfNeeded().storingStatIn(result).forPath(this.logNodePath, serializedMetadata);
        return result;
    }

    @VisibleForTesting
    protected Stat updateZkMetadata(byte[] serializedMetadata, int version) throws Exception {
        return this.zkClient.setData().withVersion(version).forPath(this.logNodePath, serializedMetadata);
    }

    /**
     * Verifies the given {@link LogMetadata} against the actual one stored in ZooKeeper.
     *
     * @param metadata The Metadata to check.
     * @return True if the metadata stored in ZooKeeper is an identical match to the given one, false otherwise. If true,
     * {@link LogMetadata#getUpdateVersion()} will also be updated with the one stored in ZooKeeper.
     */
    private boolean reconcileMetadata(LogMetadata metadata) {
        try {
            val actualMetadata = loadMetadata();
            if (metadata.equals(actualMetadata)) {
                metadata.withUpdateVersion(actualMetadata.getUpdateVersion());
                return true;
            }
        } catch (DataLogInitializationException ex) {
            log.warn("{}: Unable to verify persisted metadata (path = '{}{}').", this.traceObjectId, this.zkClient.getNamespace(), this.logNodePath, ex);
        }
        return false;
    }

    /**
     * Persists the given metadata into ZooKeeper, overwriting whatever was there previously.
     *
     * @param metadata Thew metadata to write.
     * @throws IllegalStateException    If this ChunkStreamLog is not disabled.
     * @throws IllegalArgumentException If `metadata.getUpdateVersion` does not match the current version in ZooKeeper.
     * @throws DurableDataLogException  If another kind of exception occurred. See {@link #persistMetadata}.
     */
    @VisibleForTesting
    void overWriteMetadata(LogMetadata metadata) throws DurableDataLogException {
        LogMetadata currentMetadata = loadMetadata();
        boolean create = currentMetadata == null;
        if (!create) {
            Preconditions.checkState(!currentMetadata.isEnabled(), "Cannot overwrite metadata if ChunkStreamLog is enabled.");
            Preconditions.checkArgument(currentMetadata.getUpdateVersion() == metadata.getUpdateVersion(),
                    "Wrong Update Version; expected %s, given %s.", currentMetadata.getUpdateVersion(), metadata.getUpdateVersion());
        }

        persistMetadata(metadata, create);
    }

    //endregion

    //region Helpers

    private ByteBuffer convertData(CompositeArrayView data) {
        return ByteBuffer.wrap(data.getCopy());
    }

    private void reportMetrics() {
        // this.metrics.queueStats(this.logStream.getQueueStatistics());
    }

    private LogMetadata getLogMetadata() {
        synchronized (this.lock) {
            return this.logMetadata;
        }
    }

    private ChunkStreamWriter getStreamWriter() {
        synchronized (this.lock) {
            return this.streamWriter;
        }
    }

    private void ensurePreconditions() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        synchronized (this.lock) {
            Preconditions.checkState(this.streamWriter != null, "ChunkStreamLog is not initialized.");
            assert this.logMetadata != null : "logMetadata == null";
        }
    }

    //endregion
}
