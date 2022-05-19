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

import com.emc.storageos.data.cs.stream.ChunkStreamReader;
import com.emc.storageos.data.cs.stream.ChunkStreamWriter;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.DurableDataLogException;

/**
 * General utilities pertaining to chunk streams.
 */
public class ChunkStreams {

    /**
     * For special log repair operations, we allow the LogReader to read from a special log id that contains
     * admin-provided changes to repair the original lod data.
     */
    static final int REPAIR_LOG_ID = Integer.MAX_VALUE;

    /**
     * For special log repair operations, we need to store the original content of the damaged log on a temporary backup
     * log.
     */
    static final int BACKUP_LOG_ID = Integer.MAX_VALUE - 1;

    /**
     * Open a chunk stream writer.
     * @param streamId     The id of the stream to open for write.
     * @param streamWriter A {@link ChunkStreamWriter} to open.
     * @throws DurableDataLogException If another exception occurred. The causing exception is wrapped inside it.
     */
    static void open(String streamId, boolean create, ChunkStreamWriter streamWriter) throws DurableDataLogException {
        try {
            streamWriter.open(streamId, create).get();
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to open chunk stream writer %s.", streamId), ex);
        }
    }

    /**
     * Open a chunk stream reader.
     * @param streamId     The id of the stream to open for read.
     * @param streamReader A {@link ChunkStreamReader} to open.
     * @throws DurableDataLogException If another exception occurred. The causing exception is wrapped inside it.
     */
    static void open(String streamId, long startOffset, ChunkStreamReader streamReader) throws DurableDataLogException {
        try {
            streamReader.open(streamId, startOffset).get();
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to open chunk stream reader %s.", streamId), ex);
        }
    }

    /**
     * Closes the given chunk stream writer.
     *
     * @param streamWriter A {@link ChunkStreamWriter} to close.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static void close(ChunkStreamWriter streamWriter) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(streamWriter::close);
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to close chunk stream writer %s.", streamWriter.streamId()), ex);
        }
    }

    /**
     * Closes the given chunk stream reader.
     *
     * @param streamReader A {@link ChunkStreamReader} to close.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static void close(ChunkStreamReader streamReader) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(streamReader::close);
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to close chunk stream reader %s.", streamReader.streamId()), ex);
        }
    }


    /**
     * Truncate the chunk stream to given address.
     *
     * @param streamWriter A {@link ChunkStreamWriter} to truncate.
     * @param address The address up to which to truncate.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static void truncate(ChunkStreamWriter streamWriter, StreamAddress address) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(() -> streamWriter.truncate(address.getSequence()));
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to truncate chunk stream %s up to %s.", streamWriter.streamId(), address), ex);
        }
    }
}
