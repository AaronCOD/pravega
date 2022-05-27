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

import com.emc.storageos.data.cs.atlas.AtlasRpcCommunicator;
import com.emc.storageos.data.cs.common.CSConfiguration;
import com.emc.storageos.data.cs.common.ChunkConfig;
import com.emc.storageos.data.cs.common.K8sCluster;
import com.emc.storageos.data.cs.dt.CmClientImpl;
import com.emc.storageos.data.cs.dt.cache.ChunkCache;
import com.emc.storageos.data.cs.dt.cache.ChunkHashMapCache;
import com.emc.storageos.rpc.disk.hdd.HDDClient;
import com.emc.storageos.rpc.disk.hdd.HDDRpcClientServer;
import com.emc.storageos.rpc.disk.hdd.HDDRpcConfiguration;
import com.emc.storageos.rpc.dt.DTRpcConfiguration;
import com.emc.storageos.rpc.dt.DTRpcServer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for ChunkStreamLogs.
 */
@Slf4j
public class ChunkStreamLogFactory implements DurableDataLogFactory {
    //region Members

    private static final int ATLAS_PORT = 8090;
    private static final int ATLAS_KEEP_ALIVE_TIME_SECONDS = 5;
    private static final int ATLAS_KEEP_ALIVE_TIMEOUT_SECONDS = 5;
    private static final String ATLAS_SERVICE_HOSTNAME = System.getenv("ATLAS_SERVICE_HOSTNAME");
    private static final String MY_OBJECTSTORE_NAME = System.getenv("MY_OBJECTSTORE_NAME");
    private static final String MY_POD_IP = System.getenv("MY_POD_IP");
    // Period of inspection to meet the maximum number of log creation attempts for a given container.
    private static final Duration LOG_CREATION_INSPECTION_PERIOD = Duration.ofSeconds(60);
    // Maximum number of log creation attempts for a given container before considering resetting the BK client.
    private static final int MAX_CREATE_ATTEMPTS_PER_LOG = 2;

    private final String namespace;
    private final CuratorFramework zkClient;
    private final CSConfiguration csConfig;
    private final ChunkConfig chunkConfig;
    private final ChunkStreamConfig config;
    private final ScheduledExecutorService executor;
    private final AtomicReference<CmClientImpl> cmClient;
    private AtlasRpcCommunicator atlasRpcClient;
    private K8sCluster cluster;
    private HDDRpcClientServer diskRpcClientServer;
    private HDDClient diskClient;
    private DTRpcServer rpcDTServer;
    private ChunkCache chunkCache;
    @GuardedBy("this")
    private final Map<Integer, LogInitializationRecord> logInitializationTracker = new HashMap<>();
    @GuardedBy("this")
    private final AtomicReference<Timer> lastCmClientReset = new AtomicReference<>(new Timer());

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ChunkStreamLogFactory class.
     *
     * @param config   The configuration to use for all instances created.
     * @param zkClient ZooKeeper Client to use.
     * @param executor An executor to use for async operations.
     */
    public ChunkStreamLogFactory(ChunkStreamConfig config, CuratorFramework zkClient, ScheduledExecutorService executor) {
        this.csConfig = new CSConfiguration("16M", 8, 4);
        this.chunkConfig = new ChunkConfig("16M", 8, 4, csConfig);
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.namespace = zkClient.getNamespace();
        this.zkClient = Preconditions.checkNotNull(zkClient, "zkClient")
                .usingNamespace(this.namespace + this.config.getZkMetadataPath());
        this.cmClient = new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.diskRpcClientServer != null) {
            this.diskRpcClientServer.shutdown();
        }
        if (this.rpcDTServer != null) {
            this.rpcDTServer.shutdown();
        }
        if (this.cluster != null) {
            this.cluster.shutdown();
        }
        if (this.chunkCache != null) {
            this.chunkCache.shutdown();
        }
        this.csConfig.shutdown();
        this.chunkConfig.shutdown();
        CmClientImpl cm = this.cmClient.getAndSet(null);
        if (cm != null) {
            try {
                // cm.close();
            } catch (Exception ex) {
                log.error("Unable to close cm client.", ex);
            }
        }
    }

    //endregion

    //region DurableDataLogFactory Implementation

    @Override
    public void initialize() throws DurableDataLogException {
        Preconditions.checkState(this.cmClient.get() == null, "ChunkStreamLogFactory is already initialized.");
        try {
            DTRpcConfiguration dtRpcConfiguration = new DTRpcConfiguration();
            this.rpcDTServer = new DTRpcServer(dtRpcConfiguration);
            this.rpcDTServer.start();
            this.atlasRpcClient = new AtlasRpcCommunicator(ATLAS_PORT, ATLAS_KEEP_ALIVE_TIME_SECONDS, ATLAS_KEEP_ALIVE_TIMEOUT_SECONDS, ATLAS_SERVICE_HOSTNAME);
            this.atlasRpcClient.startConfigWatch();
            this.cluster = new K8sCluster(this.atlasRpcClient, MY_OBJECTSTORE_NAME, MY_POD_IP);
            this.cluster.setRpcServer(this.rpcDTServer);
            Thread.sleep(120000);
            this.cluster.init();
            HDDRpcConfiguration hddRpcConfig = new HDDRpcConfiguration();
            this.diskRpcClientServer = new HDDRpcClientServer(hddRpcConfig);
            this.diskClient = new HDDClient(this.diskRpcClientServer, this.csConfig, this.cluster);
            this.chunkCache = new ChunkHashMapCache();
            this.cmClient.set(startCmClient());
        } catch (IllegalArgumentException | NullPointerException ex) {
            // Most likely a configuration issue; re-throw as is.
            close();
            throw ex;
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex)) {
                // Make sure we close anything we may have opened.
                close();
            }

            // ZooKeeper not reachable, some other environment issue.
            throw new DataLogNotAvailableException("Unable to establish connection to ZooKeeper or chunk stream.", ex);
        }
    }

    @Override
    public DurableDataLog createDurableDataLog(int logId) {
        Preconditions.checkState(this.cmClient.get() != null, "ChunkStreamLogFactory is not initialized.");
        tryResetCmClient(logId);
        return new ChunkStreamLog(logId, this.zkClient, this.cmClient.get(), this.chunkConfig, this.config, this.executor);
    }

    @Override
    public DebugChunkStreamLogWrapper createDebugLogWrapper(int logId) {
        Preconditions.checkState(this.cmClient.get() != null, "ChunkStreamLogFactory is not initialized.");
        tryResetCmClient(logId);
        return new DebugChunkStreamLogWrapper(logId, this.zkClient, this.cmClient.get(), this.chunkConfig, this.config, this.executor);
    }

    @Override
    public int getRepairLogId() {
        return ChunkStreams.REPAIR_LOG_ID;
    }

    @Override
    public int getBackupLogId() {
        return ChunkStreams.BACKUP_LOG_ID;
    }

    /**
     * Gets a pointer to the cm client used by this ChunkStreamLogFactory. This should only be used for testing or
     * admin tool purposes only. It should not be used for regular operations.
     *
     * @return The cm client.
     */
    @VisibleForTesting
    public CmClientImpl getCmClient() {
        return this.cmClient.get();
    }

    //endregion

    //region Initialization

    private CmClientImpl startCmClient() {
        return new CmClientImpl(this.csConfig, this.diskClient, this.rpcDTServer, this.cluster, this.chunkCache);
    }

    /**
     * Recreate the cm client if a given log exhibits MAX_CREATE_ATTEMPTS_PER_LOG creation attempts (as a proxy
     * for Container recoveries) within the period of time defined in LOG_CREATION_INSPECTION_PERIOD.
     *
     * @param logId id of the log being restarted.
     */
    private void tryResetCmClient(int logId) {
        synchronized (this) {
            LogInitializationRecord record = logInitializationTracker.get(logId);
            if (record != null) {
                // Account for a restart of the chunk stream log.
                record.incrementLogCreations();
                // If the number of restarts for a single container is meets the threshold, let's reset the BK client.
                if (record.isCmClientResetNeeded()
                        && lastCmClientReset.get().getElapsed().compareTo(LOG_CREATION_INSPECTION_PERIOD) > 0) {
                    try {
                        log.info("Start creating cm client in reset.");
                        CmClientImpl newClient = startCmClient();
                        // If we have been able to create a new client successfully, reset the current one and update timer.
                        log.info("Successfully created new cm client, setting it as the new one to use.");
                        CmClientImpl oldClient = this.cmClient.getAndSet(newClient);
                        lastCmClientReset.set(new Timer());
                        // Lastly, attempt to close the old client.
                        log.info("Attempting to close old client.");
                        // oldClient.close();
                    } catch (Exception e) {
                        throw new RuntimeException("Failure resetting the cm client", e);
                    }
                }
            } else {
                logInitializationTracker.put(logId, new LogInitializationRecord());
            }
        }
    }

    //endregion

    /**
     * Keeps track of the number of log creation attempts within an inspection period.
     */
    static class LogInitializationRecord {
        private final AtomicReference<Timer> timer = new AtomicReference<>(new Timer());
        private final AtomicInteger counter = new AtomicInteger(0);

        /**
         * Returns whether the cm client should be reset based on the max allowed attempts of re-creating a
         * log within the inspection period.
         *
         * @return whether to re-create the cm client or not.
         */
        boolean isCmClientResetNeeded() {
            return timer.get().getElapsed().compareTo(LOG_CREATION_INSPECTION_PERIOD) < 0 && counter.get() >= MAX_CREATE_ATTEMPTS_PER_LOG;
        }

        /**
         * Increments the counter for log restarts within a particular inspection period. If the las sample is older
         * than the inspection period, the timer and the counter are reset.
         */
        void incrementLogCreations() {
            // If the time since the last log creation is too far, we need to refresh the timer to the new inspection
            // period and set the counter of log creations to 1.
            if (timer.get().getElapsed().compareTo(LOG_CREATION_INSPECTION_PERIOD) > 0) {
                timer.set(new Timer());
                counter.set(1);
            } else {
                // Otherwise, just increment the counter.
                counter.incrementAndGet();
            }
        }
    }
}
