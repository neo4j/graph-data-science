package org.neo4j.graphalgo.core.huge.loader;

import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.core.utils.BitUtil.align;

public class HugeWeightMapTest {

    /**
     * Uses {@link org.neo4j.graphalgo.core.huge.loader.HugeWeightMap.Page}
     **/
    @Test
    public void shouldComputeMemoryRequirementsForSinglePage() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100).build();
        MemoryTree memRec = HugeWeightMap.memoryRequirements(4096, 1).apply(dimensions, 1);

        MemoryRange expected = MemoryRange.of(
                32L /* Page.class */ + 232L /* data */,
                32L /* Page.class */ + 4200 /* data */);

        assertEquals(expected, memRec.memoryUsage());
    }

    /**
     * Uses {@link org.neo4j.graphalgo.core.huge.loader.HugeWeightMap.PagedHugeWeightMap}
     **/
    @Test
    public void shouldComputeMemoryRequirementsForMultiplePages() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100_000).build();
        int numberOfPages = (int) BitUtil.ceilDiv(100_000, 4096);
        MemoryTree memRec = HugeWeightMap.memoryRequirements(4096, numberOfPages).apply(dimensions, 1);

        long min =
                40 /* PagedHugeWeightMap.class */ +
                align(16 + numberOfPages * 4, 8) /* sizeOfObjectArray(numberOfPages) */ +
                numberOfPages * (32 + 232) /* Page.memoryRequirements(pageSize).times(numberOfPages)) */;

        long max =
                40 /* PagedHugeWeightMap.class */ +
                align(16 + numberOfPages * 4, 8) /* sizeOfObjectArray(numberOfPages) */ +
                numberOfPages * (32 + 131176) /* Page.memoryRequirements(pageSize).times(numberOfPages)) */;

        assertEquals(MemoryRange.of(min, max), memRec.memoryUsage());
    }
}