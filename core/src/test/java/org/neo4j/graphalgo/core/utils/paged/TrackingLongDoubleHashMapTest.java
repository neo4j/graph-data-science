package org.neo4j.graphalgo.core.utils.paged;

import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;

import static org.junit.Assert.*;

public class TrackingLongDoubleHashMapTest {

    @Test
    public void shouldComputeMemoryEstimationForSinglePage() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100).build();
        MemoryRange memoryRange = TrackingLongDoubleHashMap
                .memoryEstimation(4096)
                .apply(dimensions, 1)
                .memoryUsage();

        long minBufferSize = 9L;
        long maxBufferSize = 257;

        long min =
                56 /* TrackingIntDoubleHashMap.class */ +
                BitUtil.align(16 + minBufferSize * 8, 8) /* sizeOfLongArray(minBufferSize) */ +
                BitUtil.align(16 + minBufferSize * 8, 8) /* sizeOfDoubleArray(minBufferSize) */;
        long max =
                56 /* TrackingIntDoubleHashMap.class */ +
                BitUtil.align(16 + maxBufferSize * 8, 8) /* sizeOfLongArray(maxBufferSize) */ +
                BitUtil.align(16 + maxBufferSize * 8, 8) /* sizeOfDoubleArray(maxBufferSize) */;

        assertEquals(MemoryRange.of(min, max), memoryRange);
    }

    @Test
    public void shouldComputeMemoryEstimationForMultiplePages() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100_000).build();
        MemoryRange memoryRange = TrackingLongDoubleHashMap
                .memoryEstimation(4096)
                .apply(dimensions, 1)
                .memoryUsage();

        long minBufferSize = 9L;
        long maxBufferSize = 8193L;

        long min =
                56 /* TrackingIntDoubleHashMap.class */ +
                BitUtil.align(16 + minBufferSize * 8, 8) /* sizeOfLongArray(minBufferSize) */ +
                BitUtil.align(16 + minBufferSize * 8, 8) /* sizeOfDoubleArray(minBufferSize) */;
        long max =
                56 /* TrackingIntDoubleHashMap.class */ +
                BitUtil.align(16 + maxBufferSize * 8, 8) /* sizeOfLongArray(maxBufferSize) */ +
                BitUtil.align(16 + maxBufferSize * 8, 8) /* sizeOfDoubleArray(maxBufferSize) */;

        assertEquals(MemoryRange.of(min, max), memoryRange);
    }
}
