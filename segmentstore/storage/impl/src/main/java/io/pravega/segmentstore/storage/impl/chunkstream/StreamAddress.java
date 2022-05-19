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

import io.pravega.segmentstore.storage.LogAddress;

public class StreamAddress extends LogAddress implements Comparable<StreamAddress> {

    //region Constructor

    /**
     * Creates a new instance of the StreamAddress class.
     *
     * @param offset The offset in the stream.
     */
    StreamAddress(long offset) {
        super(offset);
    }

    //endregion

    //region Comparable Implementation

    @Override
    public int hashCode() {
        return Long.hashCode(getSequence());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StreamAddress) {
            return this.compareTo((StreamAddress) obj) == 0;
        }

        return false;
    }

    @Override
    public int compareTo(StreamAddress address) {
        return Long.compare(getSequence(), address.getSequence());
    }

    //endregion
}
