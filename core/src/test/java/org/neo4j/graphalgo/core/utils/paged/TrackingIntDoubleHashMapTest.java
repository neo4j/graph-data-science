package org.neo4j.graphalgo.core.utils.paged;

import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;

import static org.junit.Assert.*;

public class TrackingIntDoubleHashMapTest {

    @Test
    public void shouldComputeMemoryRequirementsForSinglePage() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100).build();
        MemoryRange memoryRange = TrackingIntDoubleHashMap
                .memoryRequirements()
                .apply(dimensions, 1)
                .memoryUsage();
        assertEquals(MemoryRange.of(208L, 3184L), memoryRange);
    }

    @Test
    public void shouldComputeMemoryRequirementsForMultiplePages() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100_000).build();
        MemoryRange memoryRange = TrackingIntDoubleHashMap
                .memoryRequirements()
                .apply(dimensions, 1)
                .memoryUsage();
        assertEquals(MemoryRange.of(208L, 393_328L), memoryRange);
    }
}