package org.neo4j.graphalgo.core.utils.paged;

import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;

import static org.junit.Assert.*;

public class TrackingIntDoubleHashMapTest {

    @Test
    public void shouldComputeMemoryEstimationForSinglePage() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100).build();
        MemoryRange memoryRange = TrackingIntDoubleHashMap
                .memoryEstimation()
                .apply(dimensions, 1)
                .memoryUsage();

        long minBufferSize = 9L;
        long maxBufferSize = 257;

        long min =
                64 /* TrackingIntDoubleHashMap.class */ +
                BitUtil.align(16 + minBufferSize * 4, 8) /* sizeOfIntArray(minBufferSize) */ +
                BitUtil.align(16 + minBufferSize * 8, 8) /* sizeOfDoubleArray(minBufferSize) */;
        long max =
                64 /* TrackingIntDoubleHashMap.class */ +
                BitUtil.align(16 + maxBufferSize * 4, 8) /* sizeOfIntArray(maxBufferSize) */ +
                BitUtil.align(16 + maxBufferSize * 8, 8) /* sizeOfDoubleArray(maxBufferSize) */;

        assertEquals(MemoryRange.of(min, max), memoryRange);
    }

    @Test
    public void shouldComputeMemoryEstimationForMultiplePages() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100_000).build();
        MemoryRange memoryRange = TrackingIntDoubleHashMap
                .memoryEstimation()
                .apply(dimensions, 1)
                .memoryUsage();

        long minBufferSize = 9L;
        long maxBufferSize = 32769L;

        long min =
                64 /* TrackingIntDoubleHashMap.class */ +
                BitUtil.align(16 + minBufferSize * 4, 8) /* sizeOfIntArray(minBufferSize) */ +
                BitUtil.align(16 + minBufferSize * 8, 8) /* sizeOfDoubleArray(minBufferSize) */;
        long max =
                64 /* TrackingIntDoubleHashMap.class */ +
                BitUtil.align(16 + maxBufferSize * 4, 8) /* sizeOfIntArray(maxBufferSize) */ +
                BitUtil.align(16 + maxBufferSize * 8, 8) /* sizeOfDoubleArray(maxBufferSize) */;

        assertEquals(MemoryRange.of(min, max), memoryRange);
    }
}
