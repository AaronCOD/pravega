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

import io.pravega.segmentstore.storage.ReadOnlyLogMetadata;

/**
 * Defines a read-only view of the ChunkStream Log Metadata.
 */
public interface ReadOnlyChunkStreamLogMetadata extends ReadOnlyLogMetadata {
    /**
     * Gets a StreamAddress representing the first location in the log that is accessible for reads.
     *
     * @return The Truncation Address.
     */
    StreamAddress getTruncationAddress();

    /**
     * Determines whether this {@link ReadOnlyChunkStreamLogMetadata} is equivalent to the other one.
     *
     * @param other The other instance.
     * @return True if equivalent, false otherwise.
     */
    default boolean equals(ReadOnlyChunkStreamLogMetadata other) {
        if (other == null) {
            return false;
        }

        if (this.isEnabled() != other.isEnabled()
                || this.getEpoch() != other.getEpoch()
                || !this.getTruncationAddress().equals(other.getTruncationAddress())) {
            return false;
        }

        // All tests have passed.
        return true;
    }
}
