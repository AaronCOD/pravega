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

import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

import java.time.Duration;

import static io.pravega.shared.MetricsTags.containerTag;

public class ChunkMetrics {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("chunkstream");

    /**
     * ChunkStreamLog-specific (i.e. per Container) Metrics.
     */
    final static class ChunkStreamLog implements AutoCloseable {
        private final OpStatsLogger writeQueueSize;
        private final OpStatsLogger writeQueueFillRate;
        private final OpStatsLogger writeLatency;
        private final Counter writeBytes;
        private final String[] containerTag;

        ChunkStreamLog(int containerId) {
            this.containerTag = containerTag(containerId);
            this.writeQueueSize = STATS_LOGGER.createStats(MetricsNames.CHUNK_STREAM_WRITE_QUEUE_SIZE, this.containerTag);
            this.writeQueueFillRate = STATS_LOGGER.createStats(MetricsNames.CHUNK_STREAM_WRITE_QUEUE_FILL_RATE, this.containerTag);
            this.writeLatency = STATS_LOGGER.createStats(MetricsNames.CHUNK_STREAM_WRITE_LATENCY, this.containerTag);
            this.writeBytes = STATS_LOGGER.createCounter(MetricsNames.CHUNK_STREAM_WRITE_BYTES, this.containerTag);
        }

        @Override
        public void close() {
            this.writeLatency.close();
            this.writeBytes.close();
        }

        void queueStats(QueueStats qs) {
            this.writeQueueSize.reportSuccessValue(qs.getSize());
            this.writeQueueFillRate.reportSuccessValue((int) (qs.getAverageItemFillRatio() * 100));
        }

        void writeCompleted(int length, Duration elapsed) {
            this.writeLatency.reportSuccessEvent(elapsed);
            this.writeBytes.add(length);
        }
    }
}
