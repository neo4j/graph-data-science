package org.neo4j.graphalgo.core.huge;

import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import static org.junit.Assert.assertEquals;

public class HugeAdjacencyOffsetsTest {

    @Test
    public void shouldComputeMemoryEstimationForSinglePage() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100).build();
        MemoryTree memRec = HugeAdjacencyOffsets
                .memoryEstimation(4096, 1)
                .apply(dimensions, 1);
        MemoryRange expected = MemoryRange.of(16L /* Page.class */ + BitUtil.align(16 + 4096 * 8, 8) /* data */);

        assertEquals(expected, memRec.memoryUsage());
    }

    @Test
    public void shouldComputeMemoryEstimationForMultiplePages() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100_000).build();
        int numberOfPages = (int) BitUtil.ceilDiv(100_000, 4096);
        MemoryTree memRec = HugeAdjacencyOffsets
                .memoryEstimation(4096, numberOfPages)
                .apply(dimensions, 1);

        MemoryRange expected = MemoryRange.of(
                32L /* PagedOffsets.class */ +
                BitUtil.align(16 + numberOfPages * 4, 8) + /* sizeOfObjectArray(numberOfPages) */
                (BitUtil.align(16 + 4096 * 8, 8) * numberOfPages)) /* sizeOfLongArray(pageSize) * numberOfPages */;

        assertEquals(expected, memRec.memoryUsage());
    }
}
