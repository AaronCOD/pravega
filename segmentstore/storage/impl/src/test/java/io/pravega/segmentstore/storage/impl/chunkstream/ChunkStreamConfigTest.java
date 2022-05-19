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

import io.pravega.common.util.InvalidPropertyValueException;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

public class ChunkStreamConfigTest {
    @Test
    public void testDefaultValues() {
        ChunkStreamConfig cfg = ChunkStreamConfig.builder()
                .build();
        Assert.assertEquals("/segmentstore/containers/chunkstream", cfg.getZkMetadataPath());
        Assert.assertEquals(2, cfg.getZkHierarchyDepth());
        Assert.assertEquals(60000, cfg.getChunkStreamWriteTimeoutMillis());
        Assert.assertEquals(30000, cfg.getChunkStreamReadTimeoutMillis());
        Assert.assertEquals(256 * 1024 * 1024, cfg.getMaxOutstandingBytes());
    }

    @Test
    public void testZkHierarchyDepth() {
        AssertExtensions.assertThrows("BookKeeperConfig did not throw InvalidPropertyValueException",
                () -> ChunkStreamConfig.builder()
                        .with(ChunkStreamConfig.ZK_HIERARCHY_DEPTH, -1)
                        .build(),
                ex -> ex instanceof InvalidPropertyValueException);
    }
}
