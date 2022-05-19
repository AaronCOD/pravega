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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Getter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class LogMetadata implements ReadOnlyChunkStreamLogMetadata {
    //region Members

    static final VersionedSerializer.WithBuilder<LogMetadata, LogMetadataBuilder> SERIALIZER = new Serializer();

    /**
     * The initial epoch to use for the Log.
     */
    @VisibleForTesting
    static final long INITIAL_EPOCH = 1;

    /**
     * The initial version for the metadata (for an empty log). Every time the metadata is persisted, its version is incremented.
     */
    @VisibleForTesting
    static final int INITIAL_VERSION = -1;

    /**
     * The initial stream offset of the log.
     */
    @VisibleForTesting
    static final long INITIAL_STREAM_OFFSET = 0L;

    /**
     * A LogAddress to be used when the log is not truncated (initially).
     */
    @VisibleForTesting
    static final StreamAddress INITIAL_TRUNCATION_ADDRESS = new StreamAddress(INITIAL_STREAM_OFFSET);

    /**
     * The current epoch of the metadata. The epoch is incremented upon every successful recovery.
     */
    @Getter
    private final long epoch;

    /**
     * Whether the Log described by this LogMetadata is enabled or not.
     */
    @Getter
    private final boolean enabled;

    /**
     * The Address of the last write that was truncated out of the log. Every read will start from the next element.
     */
    @Getter
    private final StreamAddress truncationAddress;

    /**
     * The current update version of the metadata. The update version is incremented every time the metadata is persisted.
     */
    private final AtomicInteger updateVersion;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogMetadata class with epoch set to the default value.
     *
     */
    LogMetadata() {
        this(INITIAL_EPOCH, true, INITIAL_TRUNCATION_ADDRESS, INITIAL_VERSION);
    }

    /**
     * Creates a new instance of the LogMetadata class.
     *
     * @param epoch             The current Log epoch.
     * @param enabled           Whether this Log is enabled or not.
     * @param truncationAddress The truncation address for this log. This is the address of the last entry that has been
     * truncated out of the log.
     * @param updateVersion     The Update version to set on this instance.
     */
    @Builder
    private LogMetadata(long epoch, boolean enabled, StreamAddress truncationAddress, int updateVersion) {
        Preconditions.checkArgument(epoch > 0, "epoch must be a positive number");
        this.epoch = epoch;
        this.enabled = enabled;
        this.truncationAddress = Preconditions.checkNotNull(truncationAddress, "truncationAddress");
        this.updateVersion = new AtomicInteger(updateVersion);
    }

    //endregion

    //region Operations

    /**
     * Creates a new instance of the LogMetadata class which updates the epoch.
     *
     * @return A new instance of the LogMetadata class.
     */
    LogMetadata updateEpoch() {
        Preconditions.checkState(this.enabled, "Log is not enabled. Cannot perform any modifications on it.");
        return new LogMetadata(this.epoch + 1, this.enabled, this.truncationAddress, this.updateVersion.get());
    }

    /**
     * Creates a new instance of the LogMetadata class which is truncated after the given address.
     *
     * @param upToAddress The address to truncate to.
     * @return A new instance of the LogMetadata class.
     */
    LogMetadata truncate(StreamAddress upToAddress) {
        Preconditions.checkState(this.enabled, "Log is not enabled. Cannot perform any modifications on it.");
        return new LogMetadata(this.epoch, this.enabled, upToAddress, this.updateVersion.get());
    }

    /**
     * Gets a value indicating the current version of the Metadata (this changes upon every successful metadata persist).
     * Note: this is different from getEpoch() - which gets incremented with every successful recovery.
     *
     * @return The current version.
     */
    @Override
    public int getUpdateVersion() {
        return this.updateVersion.get();
    }

    /**
     * Updates the current version of the metadata.
     *
     * @param value The new metadata version.
     * @return This instance.
     */
    LogMetadata withUpdateVersion(int value) {
        Preconditions.checkArgument(value >= this.updateVersion.get(), "versions must increase");
        this.updateVersion.set(value);
        return this;
    }

    /**
     * Returns a LogMetadata class with the exact contents of this instance, but the enabled flag set to true. No changes
     * are performed on this instance.
     *
     * @return This instance, if isEnabled() == true, of a new instance of the LogMetadata class which will have
     * isEnabled() == true, otherwise.
     */
    LogMetadata asEnabled() {
        return this.enabled ? this : new LogMetadata(this.epoch, true, this.truncationAddress, this.updateVersion.get());
    }

    /**
     * Returns a LogMetadata class with the exact contents of this instance, but the enabled flag set to false. No changes
     * are performed on this instance.
     *
     * @return This instance, if isEnabled() == false, of a new instance of the LogMetadata class which will have
     * isEnabled() == false, otherwise.
     */
    LogMetadata asDisabled() {
        return this.enabled ? new LogMetadata(this.epoch, false, this.truncationAddress, this.updateVersion.get()) : this;
    }

    //endregion

    @Override
    public String toString() {
        return String.format("Version = %d, Epoch = %d, Truncate = %s", this.updateVersion.get(), this.epoch, this.truncationAddress);
    }

    //region Serialization

    static class LogMetadataBuilder implements ObjectBuilder<LogMetadata> {
    }

    private static class Serializer extends VersionedSerializer.WithBuilder<LogMetadata, LogMetadata.LogMetadataBuilder> {
        @Override
        protected LogMetadataBuilder newBuilder() {
            return LogMetadata.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(LogMetadata m, RevisionDataOutput output) throws IOException {
            output.writeBoolean(m.isEnabled());
            output.writeCompactLong(m.getEpoch());
            output.writeCompactLong(m.truncationAddress.getSequence());
        }

        private void read00(RevisionDataInput input, LogMetadataBuilder builder) throws IOException {
            builder.enabled(input.readBoolean());
            builder.epoch(input.readCompactLong());
            builder.truncationAddress(new StreamAddress(input.readCompactLong()));
            builder.updateVersion(INITIAL_VERSION);
        }
    }

    //endregion
}
