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

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class LogMetadataTest {
    private static final long INITIALIZE_COUNT = 10;

    @Test(timeout = 5000)
    public void testUpdateEpoch() {
        LogMetadata metadata = null;
        long expectedEpoch = 0;
        for (int i = 0; i < INITIALIZE_COUNT; i++) {
            expectedEpoch++;
            if (metadata == null) {
                metadata = new LogMetadata().withUpdateVersion(i);
            } else {
                metadata = metadata.updateEpoch().withUpdateVersion(i);
            }

            Assert.assertEquals("Unexpected epoch.", expectedEpoch, metadata.getEpoch());
            Assert.assertEquals("Unexpected update version.", i, metadata.getUpdateVersion());
        }
    }

    @Test(timeout = 5000)
    public void testTruncate() {
        LogMetadata metadata = new LogMetadata();
        StreamAddress truncateAddress = new StreamAddress(123L);
        val truncatedMetadata = metadata.truncate(truncateAddress);
        Assert.assertEquals("Unexpected TruncationAddress.", truncateAddress.getSequence(), truncatedMetadata.getTruncationAddress().getSequence());
    }

    @Test(timeout = 5000)
    public void testSerialization() throws Exception {
        LogMetadata m1 = null;
        for (int i = 0; i < INITIALIZE_COUNT; i++) {
            if (m1 == null) {
                m1 = new LogMetadata().withUpdateVersion(i);
            } else {
                m1 = m1.updateEpoch().withUpdateVersion(i);
            }
        }
        StreamAddress truncateAddress = new StreamAddress(321L);
        m1 = m1.truncate(truncateAddress);
        m1 = m1.asDisabled();

        val serialization = LogMetadata.SERIALIZER.serialize(m1);
        val m2 = LogMetadata.SERIALIZER.deserialize(serialization);

        Assert.assertEquals("Unexpected epoch.", m1.getEpoch(), m2.getEpoch());
        Assert.assertEquals("Unexpected TruncationAddress.", m1.getTruncationAddress().getSequence(), m2.getTruncationAddress().getSequence());
        Assert.assertEquals("Unexpected enabled flag.", m1.isEnabled(), m2.isEnabled());
    }

    @Test
    public void testEquals() {
        val m1 = LogMetadata.builder()
                .enabled(true)
                .epoch(1L)
                .updateVersion(234)
                .truncationAddress(new StreamAddress(100L))
                .build();

        // Check against null.
        Assert.assertFalse(m1.equals(null));

        // A copy of itself should be identical.
        Assert.assertTrue(m1.equals(copyOf(m1)));

        // Enabled/disabled.
        Assert.assertFalse(m1.equals(m1.asDisabled()));
        Assert.assertTrue(m1.equals(m1.asDisabled().asEnabled()));

        // Epoch.
        Assert.assertFalse(m1.equals(m1.updateEpoch()));

        // Update version is not part of the equality check.
        Assert.assertTrue(m1.equals(m1.withUpdateVersion(432)));

        // Truncation address.
        Assert.assertFalse(m1.equals(m1.truncate(new StreamAddress(200L))));
    }

    private LogMetadata copyOf(LogMetadata m) {
        return LogMetadata.builder()
                .enabled(m.isEnabled())
                .epoch(m.getEpoch())
                .updateVersion(m.getUpdateVersion())
                .truncationAddress(new StreamAddress(m.getTruncationAddress().getSequence()))
                .build();
    }
}
