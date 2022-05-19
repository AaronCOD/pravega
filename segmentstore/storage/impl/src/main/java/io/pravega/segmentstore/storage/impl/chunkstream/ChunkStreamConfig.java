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

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.InvalidPropertyValueException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * General configuration for Chunk Stream.
 */
public class ChunkStreamConfig {
    //region Config Names

    public static final Property<String> ZK_METADATA_PATH = Property.named("zk.metadata.path", "/segmentstore/containers/chunkstream", "zkMetadataPath");
    public static final Property<Integer> ZK_HIERARCHY_DEPTH = Property.named("zk.metadata.hierarchy.depth", 2, "zkHierarchyDepth");
    public static final Property<Integer> CHUNK_STREAM_WRITE_TIMEOUT = Property.named("write.timeout.milliseconds", 60000, "chunkStreamWriteTimeoutMillis");
    public static final Property<Integer> CHUNK_STREAM_READ_TIMEOUT = Property.named("read.timeout.milliseconds", 30000, "chunkStreamReadTimeoutMillis");
    public static final Property<Integer> MAX_OUTSTANDING_BYTES = Property.named("write.outstanding.bytes.max", 256 * 1024 * 1024, "maxOutstandingBytes");

    public static final String COMPONENT_CODE = "chunkstream";

    /**
     * Maximum append length, as specified by Chunk Stream.
     */
    static final int MAX_APPEND_LENGTH = 1024 * 1024 - 1024;

    //endregion

    //region Members

    /**
     * Sub-namespace to use for ZooKeeper LogMetadata.
     */
    @Getter
    private final String zkMetadataPath;

    /**
     * Depth of the node hierarchy in ZooKeeper. 0 means flat, N means N deep, where each level is indexed by its
     * respective log id digit.
     */
    @Getter
    private final int zkHierarchyDepth;

    /**
     * The Write Timeout (Chunk Stream), in milliseconds.
     */
    @Getter
    private final int chunkStreamWriteTimeoutMillis;

    /**
     * The Read Timeout (Chunk Stream), in milliseconds.
     */
    @Getter
    private final int chunkStreamReadTimeoutMillis;

    /**
     * The maximum number of bytes that can be outstanding per ChunkStreamLog at any given time. This value should be used
     * for throttling purposes.
     */
    @Getter
    private final int maxOutstandingBytes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ChunkStreamConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ChunkStreamConfig(TypedProperties properties) throws ConfigurationException {
        this.zkMetadataPath = properties.get(ZK_METADATA_PATH);
        this.zkHierarchyDepth = properties.getInt(ZK_HIERARCHY_DEPTH);
        if (this.zkHierarchyDepth < 0) {
            throw new InvalidPropertyValueException(String.format("Property %s (%d) must be a non-negative integer.",
                    ZK_HIERARCHY_DEPTH, this.zkHierarchyDepth));
        }
        this.chunkStreamWriteTimeoutMillis = properties.getInt(CHUNK_STREAM_WRITE_TIMEOUT);
        this.chunkStreamReadTimeoutMillis = properties.getInt(CHUNK_STREAM_READ_TIMEOUT);
        this.maxOutstandingBytes = properties.getInt(MAX_OUTSTANDING_BYTES);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ChunkStreamConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ChunkStreamConfig::new);
    }
}
