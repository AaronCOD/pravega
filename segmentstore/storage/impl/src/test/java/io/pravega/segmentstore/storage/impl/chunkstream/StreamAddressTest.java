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

import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class StreamAddressTest {
    private static final int ENTRY_SIZE = 5;
    private static final int ENTRY_COUNT = 10;

    /**
     * Tests the Compare method.
     */
    @Test(timeout = 5000)
    public void testCompare() {
        val addresses = new ArrayList<StreamAddress>();
        long streamOffset = 0L;
        for (long entryCount = 0; entryCount < ENTRY_COUNT; entryCount++) {
            addresses.add(new StreamAddress(streamOffset));
            streamOffset += ENTRY_SIZE;
        }

        for (int i = 0; i < addresses.size() / 2; i++) {
            val a1 = addresses.get(i);
            val a2 = addresses.get(addresses.size() - i - 1);
            val result1 = a1.compareTo(a2);
            val result2 = a2.compareTo(a1);
            AssertExtensions.assertLessThan("Unexpected when comparing smaller to larger.", 0, result1);
            AssertExtensions.assertGreaterThan("Unexpected when comparing larger to smaller.", 0, result2);
            Assert.assertEquals("Unexpected when comparing to itself.", 0, a1.compareTo(a1));
        }
    }
}
