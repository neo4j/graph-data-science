package org.neo4j.graphalgo.core.utils.mem;

import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemoryEstimationsTest {
    private final static GraphDimensions DIMENSIONS_100_NODES = new GraphDimensions.Builder().setNodeCount(100).build();

    @Test
    public void testEmptyBuilder() {
        MemoryEstimation empty = MemoryEstimations.empty();
        assertEquals("", empty.description());
        assertTrue(empty.components().isEmpty());
        assertEquals(
                MemoryRange.empty(),
                empty.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
    }

    @Test
    public void testBuilderForClass() {
        MemoryEstimation memoryEstimation1 = MemoryEstimations.builder(Foo.class).build();
        MemoryEstimation memoryEstimation2 = MemoryEstimations
                .builder()
                .field(Foo.class.getSimpleName(), Foo.class)
                .build();
        MemoryEstimation memoryEstimation3 = MemoryEstimations
                .builder()
                .startField(Foo.class.getSimpleName(), Foo.class)
                .endField()
                .build();
        MemoryEstimation memoryEstimation4 = MemoryEstimations.of(Foo.class);
        assertEquals(MemoryRange.of(40L), memoryEstimation1.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
        assertEquals(MemoryRange.of(40L), memoryEstimation2.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
        assertEquals(MemoryRange.of(40L), memoryEstimation3.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
        assertEquals(MemoryRange.of(40L), memoryEstimation4.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
    }

    @Test
    public void testFixed() {
        MemoryEstimation memoryEstimation = MemoryEstimations.builder()
                .fixed("bar", 23L)
                .fixed("baz", MemoryRange.of(19))
                .build();

        assertEquals(MemoryRange.of(42L), memoryEstimation.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
    }

    @Test
    public void testAdd() {
        MemoryEstimation memoryEstimation = MemoryEstimations.builder()
                .add("foo", MemoryEstimations.of(Foo.class))
                .add(MemoryEstimations.of(Foo.class))
                .build();

        assertEquals(MemoryRange.of(84L), memoryEstimation.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
    }

    @Test
    public void testPerNode() {
        MemoryEstimation memoryEstimation = MemoryEstimations.builder()
                .perNode("foo", nodeCount -> nodeCount * 42)
                .perNode("bar", MemoryEstimations.of(Foo.class))
                .rangePerNode("baz", nodeCount -> MemoryRange.of(23).times(nodeCount))
                .build();

        assertEquals(
                MemoryRange.of(100 * (42 + 40 + 23)),
                memoryEstimation.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
    }

    @Test
    public void testPerThread() {
        MemoryEstimation memoryEstimation = MemoryEstimations.builder()
                .perThread("foo", concurrency -> concurrency * 42)
                .perThread("bar", MemoryEstimations.of(Foo.class))
                .build();

        assertEquals(MemoryRange.of(4 * (42 + 40)), memoryEstimation.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
    }

    @Test
    public void testPerGraphDimension() {
        MemoryEstimation memoryEstimation = MemoryEstimations.builder()
                .perGraphDimension("foo", graphDimensions -> graphDimensions.nodeCount() * 42)
                .rangePerGraphDimension("bar", graphDimensions -> MemoryRange.of(23).times(graphDimensions.nodeCount()))
                .build();

        assertEquals(
                MemoryRange.of(100 * 42 + 100 * 23),
                memoryEstimation.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
    }

    @Test
    public void testSetup() {
        final MemoryEstimation memoryEstimation = MemoryEstimations.setup(
                "foo",
                (dimensions, concurrency) -> MemoryEstimations.builder()
                        .fixed("foo", dimensions.nodeCount() * concurrency)
                        .build());

        assertEquals(
                MemoryRange.of(100 * 4),
                memoryEstimation.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
    }

    @Test
    public void testSetupFromGraphDimensions() {
        final MemoryEstimation memoryEstimation = MemoryEstimations.setup(
                "foo",
                (dimensions) -> MemoryEstimations.builder()
                        .fixed("foo", dimensions.nodeCount() * dimensions.nodeCount())
                        .build());

        assertEquals(
                MemoryRange.of(100 * 4),
                memoryEstimation.apply(DIMENSIONS_100_NODES, 4).memoryUsage());
    }

    // Note that the memory consumption will be aligned (see BitUtil.align)
    static class Foo { // 12 Byte object header
        int bar; // 4 Byte
        long baz; // 8 Byte
        long[] lol; // 4 Byte
        String lulz; // 4 Byte
        boolean lolz; // 1 Byte
    }
}